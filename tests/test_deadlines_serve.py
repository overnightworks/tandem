import logging
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from logline import Outcome, StepTimer
from redis.asyncio import Redis

from tandem import Deadlines, Due, StoreError
from tandem.errors import BACKOFF_FIRST_SECONDS
from tandem.keys import deadline_keys
from tests.support.backoffs import BackoffRecordingDeadlines, Backoffs
from tests.support.deadlines import (
    SERVING_EVERY,
    Handler,
    assert_waits_for,
    ended,
    redis_ms,
    serving,
    state_of,
    store_error_behind_a_host_error,
    until_redis_ms,
)
from tests.support.logs import LogLines
from tests.support.proxy import TcpProxy
from tests.support.waiting import elapsed, until

CLAIM_FOR = timedelta(milliseconds=300)
KIND = "work"
LOGGER = "tandem.deadlines"
_HOST_LOGGER = "host.handler"


@pytest.fixture
def recording_deadlines(
    key_prefix: str,
) -> Callable[[Redis, Backoffs], BackoffRecordingDeadlines]:
    """Deadlines over a client whose back-offs after a failed round go to `backoffs`."""

    def build(client: Redis, backoffs: Backoffs) -> BackoffRecordingDeadlines:
        return BackoffRecordingDeadlines(
            client, key_prefix=key_prefix, kind=KIND, claim_for=CLAIM_FOR, backoffs=backoffs
        )

    return build


@pytest.fixture
def deadlines_through_proxy(
    proxied_client: Redis,
    recording_deadlines: Callable[[Redis, Backoffs], BackoffRecordingDeadlines],
) -> Callable[[Backoffs], BackoffRecordingDeadlines]:
    """Deadlines through the proxy whose back-offs go to `backoffs`."""
    return lambda backoffs: recording_deadlines(proxied_client, backoffs)


async def test_a_due_key_reaches_the_handler_and_done_follows(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-21, DEADLINE-23."""
    client = make_client(redis_url)
    deadlines = new_deadlines(client, claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    handler = Handler[Due]()

    async def finished() -> bool:
        return await state_of(client, deadlines, "room-1") is None

    async with serving(deadlines, handler):
        (due,) = await handler.received()
        await until(finished)

    assert due.key == "room-1"
    assert handler.items == [due]


async def test_a_handler_that_runs_past_claim_for_keeps_its_key_from_others_and_done_follows(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-22, DEADLINE-23."""
    client = make_client(redis_url)
    first = new_deadlines(client, claim_for=CLAIM_FOR)
    second = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await first.set("room-1", timedelta(0))
    held, other = Handler[Due](hold=True), Handler[Due]()

    async def finished() -> bool:
        return await state_of(client, first, "room-1") is None

    async with serving(first, held):
        await held.received()
        async with serving(second, other):
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)
            assert other.items == []
            held.release()
            await until(finished)

    assert len(held.items) == 1
    assert other.items == []


async def test_a_handler_that_recurs_and_raises_is_not_retried_before_its_next_due(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: DEADLINE-23."""
    client = make_client(redis_url)
    deadlines = new_deadlines(client, claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    calls: list[Due] = []

    async def recurs_then_fails(due: Due) -> None:
        calls.append(due)
        await deadlines.recur(due, timedelta(seconds=30))
        raise RuntimeError("the handler fails")

    async with serving(deadlines, recurs_then_fails):
        await log_lines.await_of("deadline_handling.ended", LOGGER)
        await until_redis_ms(client, await redis_ms(client) + 2 * 300)

    assert len(calls) == 1
    state = await state_of(client, deadlines, "room-1")
    assert state is not None
    assert state[0] == "waiting"


async def test_a_handler_that_raises_without_ending_its_claim_gets_its_key_back_after_claim_for(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-23."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    attempts: list[Due] = []

    async def fails_once(due: Due) -> None:
        attempts.append(due)
        if len(attempts) == 1:
            raise RuntimeError("the handler fails")

    async def twice() -> bool:
        return len(attempts) >= 2

    async with serving(deadlines, fails_once):
        await until(twice)

    first, second = attempts[:2]
    assert first.key == second.key
    assert first.claim != second.claim


async def test_a_handler_that_recurs_and_returns_leaves_its_key_due_at_the_new_time(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: DEADLINE-23."""
    client = make_client(redis_url)
    deadlines = new_deadlines(client, claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    next_due = timedelta(seconds=30)

    async def recurs(due: Due) -> None:
        await deadlines.recur(due, next_due)

    before_ms = await redis_ms(client)
    async with serving(deadlines, recurs):
        # The round ends after the batch's `done`, which found the claim already ended.
        await log_lines.await_of("deadline_round.ended", LOGGER)
    after_ms = await redis_ms(client)

    assert_waits_for(
        await state_of(client, deadlines, "room-1"),
        next_due,
        before_ms=before_ms,
        after_ms=after_ms,
    )


@pytest.mark.parametrize(
    ("raised", "level", "outcome"),
    [(None, "info", Outcome.OK), (ValueError, "error", Outcome.FAILED)],
)
async def test_a_handling_writes_one_line_with_its_outcome_and_no_key_or_token(
    raised: type[Exception] | None,
    level: str,
    outcome: Outcome,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: LOG-28, LOG-22."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    received: list[Due] = []

    async def handle(due: Due) -> None:
        received.append(due)
        if raised is not None:
            raise raised("the handler fails")

    async with serving(deadlines, handle):
        (line, *_) = await log_lines.await_of("deadline_handling.ended", LOGGER)

    assert line["level"] == level
    assert line["outcome"] == outcome
    assert line["deadline_kind"] == KIND
    assert line["duration_ms"] >= 0
    assert line.get("error_type") == (raised.__name__ if raised else None)
    assert "exception" not in line
    log_lines.assert_none_holds(received[0].key)
    log_lines.assert_none_holds(received[0].claim)


@pytest.mark.parametrize(
    ("failure", "writes_traceback"),
    [
        (lambda: ValueError("the handler fails"), True),
        (store_error_behind_a_host_error, False),
    ],
    ids=["a host error", "a store error in its chain"],
)
async def test_a_failed_handler_writes_its_traceback_unless_a_store_error_is_in_it(
    failure: Callable[[], Exception],
    writes_traceback: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: LOG-30, LOG-28."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    raised = failure()

    async def handle(due: Due) -> None:
        raise raised

    async with serving(deadlines, handle):
        await log_lines.await_of("deadline_handling.ended", LOGGER)

    # Both lines are written without a suspension between them, so one snapshot holds both.
    handling = log_lines.of("deadline_handling.ended", LOGGER)
    tracebacks = log_lines.of("deadline_handler.failed", LOGGER)
    assert {line["error_type"] for line in handling} == {type(raised).__name__}
    assert len(tracebacks) == (len(handling) if writes_traceback else 0)
    for line in tracebacks:
        assert line["level"] == "error"
        assert line["error_type"] == type(raised).__name__
        assert line["deadline_kind"] == KIND
        assert "Traceback" in line["exception"]


async def test_each_handling_has_its_own_trace_and_the_steps_of_its_handler_hang_under_it(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: DEADLINE-24, LOG-27."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    await deadlines.set("room-2", timedelta(0))

    async def handle(due: Due) -> None:
        async with StepTimer(
            "host_call.ended", logging.getLogger(_HOST_LOGGER), deadline_id=due.key
        ):
            pass

    async with serving(deadlines, handle):
        handling = await log_lines.await_of("deadline_handling.ended", LOGGER, count=2)
        rounds = await log_lines.await_of("deadline_round.ended", LOGGER)

    steps = log_lines.of("host_call.ended", _HOST_LOGGER)
    handling_traces = {line["trace_id"] for line in handling}
    assert None not in handling_traces
    assert len(handling_traces) == len(handling)
    assert handling_traces.isdisjoint(line["trace_id"] for line in rounds)
    assert None not in {line["trace_id"] for line in rounds}
    for line in handling:
        (step,) = [step for step in steps if step["trace_id"] == line["trace_id"]]
        assert step.get("parent_span_id") == line.get("span_id")


@pytest.mark.parametrize(
    ("raised", "failed_count"),
    [(None, 0), (ValueError, 1)],
    ids=["a handled key", "a failed key counts in failed_count"],
)
async def test_a_round_claiming_nothing_writes_no_line_and_one_that_did_writes_its_counts(
    raised: type[Exception] | None,
    failed_count: int,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: LOG-29."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)

    async def handle(due: Due) -> None:
        if raised is not None:
            raise raised("the handler fails")

    async with serving(deadlines, handle):
        await elapsed(5 * SERVING_EVERY.total_seconds())
        assert not log_lines.of("deadline_round.ended", LOGGER)
        await deadlines.set("room-1", timedelta(0))
        await log_lines.await_of("deadline_round.ended", LOGGER)

    rounds = log_lines.of("deadline_round.ended", LOGGER)
    first = rounds[0]
    assert (first["level"], first["outcome"]) == ("info", Outcome.OK)
    assert (first["deadline_count"], first["failed_count"]) == (1, failed_count)
    assert first["deadline_kind"] == KIND
    assert first["duration_ms"] >= 0
    assert all(line["deadline_count"] > 0 for line in rounds)
    assert not [line for line in rounds if "failed_run_count" in line]


async def test_a_whole_outage_writes_one_failed_line_and_one_recovery_counting_its_failed_rounds(
    proxy: TcpProxy,
    deadlines_through_proxy: Callable[[Backoffs], BackoffRecordingDeadlines],
    backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: LOG-29, DEADLINE-25."""
    proxy.cut()

    async with serving(deadlines_through_proxy(backoffs), Handler[Due]()):
        await backoffs.reached(3)
        proxy.restore()
        failure, recovery = await log_lines.await_of("deadline_round.ended", LOGGER, count=2)

    assert (failure["level"], failure["outcome"], failure["reason"]) == (
        "warning",
        Outcome.FAILED,
        "outage",
    )
    assert (failure["deadline_count"], failure["failed_count"]) == (0, 0)
    assert "failed_run_count" not in failure
    assert (recovery["level"], recovery["outcome"]) == ("info", Outcome.OK)
    assert recovery["failed_run_count"] == len(backoffs.delays)
    assert backoffs.delays[0] == BACKOFF_FIRST_SECONDS
    assert backoffs.delays[1] > backoffs.delays[0]
    assert len(log_lines.of("deadline_round.ended", LOGGER)) == 2


async def test_a_short_outage_is_ridden_out_with_real_waits_and_the_key_is_handled_after_it(
    proxy: TcpProxy,
    proxied_client: Redis,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-25."""
    through_proxy = new_deadlines(proxied_client, claim_for=CLAIM_FOR)
    handler = Handler[Due]()

    async with serving(through_proxy, handler):
        proxy.cut()
        await proxy.attempts_reached(1)
        proxy.restore()
        await new_deadlines(make_client(redis_url)).set("room-1", timedelta(0))
        await handler.received()


async def test_a_refusal_is_ridden_out_with_one_warning_and_one_recovery_once_the_acl_is_restored(
    url_denying: Callable[[str], Awaitable[str]],
    observer: Redis,
    key_prefix: str,
    make_client: Callable[..., Redis],
    recording_deadlines: Callable[[Redis, Backoffs], BackoffRecordingDeadlines],
    backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: DEADLINE-25, LOG-29."""
    deadlines = recording_deadlines(make_client(await url_denying("evalsha")), backoffs)

    async with serving(deadlines, Handler[Due]()) as task:
        await backoffs.reached(3)
        assert not task.done()
        await observer.acl_setuser(key_prefix, enabled=True, commands=["+evalsha"])
        failure, recovery = await log_lines.await_of("deadline_round.ended", LOGGER, count=2)
        assert not task.done()

    assert (failure["level"], failure["outcome"], failure["reason"]) == (
        "warning",
        Outcome.FAILED,
        "refused",
    )
    assert (recovery["level"], recovery["outcome"]) == ("info", Outcome.OK)
    assert recovery["failed_run_count"] == len(backoffs.delays)
    assert len(log_lines.of("deadline_round.ended", LOGGER)) == 2


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (lambda: StoreError("reconcile lost Redis", outage=True), "outage"),
        (store_error_behind_a_host_error, "refused"),
    ],
    ids=["an outage raised directly", "a refusal chained in a host error"],
)
async def test_a_store_error_from_reconcile_counts_into_the_streak_and_is_ridden_out(
    failure: Callable[[], Exception],
    reason: str,
    redis_url: str,
    make_client: Callable[..., Redis],
    recording_deadlines: Callable[[Redis, Backoffs], BackoffRecordingDeadlines],
    backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: DEADLINE-27, DEADLINE-25, LOG-29."""
    deadlines = recording_deadlines(make_client(redis_url), backoffs)
    attempts = 0

    async def reconcile() -> None:
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            raise failure()

    async with serving(deadlines, Handler[Due](), reconcile=reconcile) as task:
        first, recovery = await log_lines.await_of("deadline_round.ended", LOGGER, count=2)
        assert not task.done()

    assert (first["level"], first["outcome"], first["reason"]) == (
        "warning",
        Outcome.FAILED,
        reason,
    )
    assert recovery["failed_run_count"] == 2


async def test_a_reconcile_failure_that_is_no_store_error_ends_the_loop_without_a_line(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: DEADLINE-27, LOG-20."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)

    async def reconcile() -> None:
        raise RuntimeError("the host's database is down")

    async with serving(deadlines, Handler[Due](), reconcile=reconcile) as task:
        failure = await ended(task)

    assert isinstance(failure, RuntimeError)
    assert not log_lines.of_logger(LOGGER)


async def test_reconcile_runs_before_the_claim_of_every_round(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-27."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    handler = Handler[Due]()

    async def arm_the_key() -> None:
        await deadlines.arm("room-1", timedelta(0))

    async with serving(deadlines, handler, reconcile=arm_the_key):
        received = await handler.received(2)

    assert [due.key for due in received] == ["room-1", "room-1"]


@pytest.mark.parametrize("every", [timedelta(0), timedelta(seconds=-1), 0.05])
async def test_every_that_is_not_a_positive_timedelta_is_refused(
    every: timedelta | float,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-26."""
    deadlines = new_deadlines(make_client(redis_url))

    with pytest.raises(ValueError, match="every"):
        await deadlines.serve_due(Handler[Due](), every=every)  # pyright: ignore[reportArgumentType]


async def test_cancelling_the_loop_cancels_the_handler_and_the_claim_runs_out_for_another_worker(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: DEADLINE-28."""
    client = make_client(redis_url)
    first = new_deadlines(client, claim_for=CLAIM_FOR)
    second = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await first.set("room-1", timedelta(0))
    held, other = Handler[Due](hold=True), Handler[Due]()

    async with serving(first, held) as task:
        (due,) = await held.received()
        task.cancel()
        (handling,) = await log_lines.await_of("deadline_handling.ended", LOGGER)
    state = await state_of(client, first, "room-1")
    async with serving(second, other):
        (again,) = await other.received()

    assert handling["outcome"] == Outcome.CANCELLED
    assert state is not None
    assert state[0] == "claimed"
    assert again.key == due.key
    assert again.claim != due.claim


async def test_a_transition_the_handler_made_stands_when_the_loop_is_cancelled(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: DEADLINE-28."""
    client = make_client(redis_url)
    first = new_deadlines(client, claim_for=CLAIM_FOR)
    second = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await first.set("room-1", timedelta(0))
    held, other = Handler[Due](hold=True), Handler[Due]()

    async def recurs_then_holds(due: Due) -> None:
        await first.recur(due, timedelta(seconds=30))
        await held(due)

    async with serving(first, recurs_then_holds) as task:
        await held.received()
        task.cancel()
        await log_lines.await_of("deadline_handling.ended", LOGGER)
    async with serving(second, other):
        await until_redis_ms(client, await redis_ms(client) + 2 * 300)

    state = await state_of(client, first, "room-1")
    assert other.items == []
    assert state is not None
    assert state[0] == "waiting"


async def test_a_cancellation_while_a_handler_runs_writes_handling_and_round_as_cancelled(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    log_lines: LogLines,
):
    """Spec: LOG-28, LOG-29."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    held = Handler[Due](hold=True)

    async with serving(deadlines, held) as task:
        await held.received()
        task.cancel()
        (handling,) = await log_lines.await_of("deadline_handling.ended", LOGGER)
        (round_line,) = await log_lines.await_of("deadline_round.ended", LOGGER)

    assert handling["outcome"] == Outcome.CANCELLED
    assert (round_line["level"], round_line["outcome"]) == ("info", Outcome.CANCELLED)
    assert (round_line["deadline_count"], round_line["failed_count"]) == (1, 0)


async def test_a_cancelled_back_off_writes_no_line_beyond_the_failed_round_before_it(
    proxy: TcpProxy,
    deadlines_through_proxy: Callable[[Backoffs], BackoffRecordingDeadlines],
    held_backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: LOG-29."""
    proxy.cut()

    async with serving(deadlines_through_proxy(held_backoffs), Handler[Due]()):
        await held_backoffs.reached(1)

    assert [line["outcome"] for line in log_lines.of("deadline_round.ended", LOGGER)] == [
        Outcome.FAILED
    ]


async def test_an_extension_lost_to_an_outage_is_logged_but_never_declared_expired(
    proxy: TcpProxy,
    deadlines_through_proxy: Callable[[Backoffs], BackoffRecordingDeadlines],
    new_deadlines: Callable[..., Deadlines],
    make_client: Callable[..., Redis],
    redis_url: str,
    backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: LOG-31, LOG-32, DEADLINE-22."""
    await new_deadlines(make_client(redis_url)).set("room-1", timedelta(0))
    held = Handler[Due](hold=True)

    async with serving(deadlines_through_proxy(backoffs), held):
        await held.received()
        proxy.cut()
        (line,) = await log_lines.await_of("deadline_claim_extension.failed", LOGGER)
        await elapsed(2 * CLAIM_FOR.total_seconds())
        held.release()

    assert line["level"] == "warning"
    assert line["reason"] == "outage"
    assert line["error_type"] == StoreError.__name__
    assert line["deadline_kind"] == KIND
    assert not log_lines.of("deadline_claim.expired", LOGGER)


async def test_an_extension_that_fails_with_no_store_error_ends_the_loop_and_the_handler(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    monkeypatch: pytest.MonkeyPatch,
):
    """Spec: DEADLINE-22."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))

    async def extension_that_breaks(due: Due) -> bool:
        raise RuntimeError("the extension has a bug")

    monkeypatch.setattr(deadlines, "extend", extension_that_breaks)
    held = Handler[Due](hold=True)

    async with serving(deadlines, held) as task:
        await held.received()
        failure = await ended(task)

    assert isinstance(failure, RuntimeError)


async def test_a_claim_that_runs_out_is_logged_without_its_key_or_token(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_deadlines: Callable[..., Deadlines],
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LOG-32, DEADLINE-22."""
    deadlines = new_deadlines(make_client(redis_url), claim_for=CLAIM_FOR)
    await deadlines.set("room-1", timedelta(0))
    held = Handler[Due](hold=True)

    async with serving(deadlines, held):
        (due,) = await held.received()
        await observer.delete(deadline_keys(key_prefix, KIND).claim)
        (line,) = await log_lines.await_of("deadline_claim.expired", LOGGER)
        held.release()

    assert line["level"] == "warning"
    assert line["deadline_kind"] == KIND
    log_lines.assert_none_holds(due.key)
    log_lines.assert_none_holds(due.claim)
