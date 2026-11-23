import asyncio
import logging
import signal
import sys
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from logline import Outcome, StepTimer
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import Lapse, Leases, Liveness, StoreError
from tandem.errors import BACKOFF_FIRST_SECONDS
from tandem.keys import lease_keys
from tests.support.attempts import Attempts, ObservedLeases
from tests.support.backoffs import BackoffRecordingLeases, Backoffs
from tests.support.deadlines import (
    SERVING_EVERY,
    redis_ms,
    store_error_behind_a_host_error,
    until_redis_ms,
)
from tests.support.leases import Handled, liveness_of, serving, until_gone
from tests.support.logs import LogLines
from tests.support.proxy import TcpProxy
from tests.support.store_failures import StoreFailure
from tests.support.waiting import elapsed, until, within

LASTS = timedelta(milliseconds=300)
CLAIM_FOR = timedelta(milliseconds=300)
LONG_CLAIM_FOR = timedelta(milliseconds=1500)  # a margin that a loaded machine cannot eat


@pytest.fixture
def recording_leases(
    key_prefix: str,
) -> Callable[[Redis, Backoffs], BackoffRecordingLeases]:
    """Leases over a client whose back-offs after a failed round go to `backoffs`."""

    def build(client: Redis, backoffs: Backoffs) -> BackoffRecordingLeases:
        return BackoffRecordingLeases(
            client,
            key_prefix=key_prefix,
            family="owner",
            lasts=LASTS,
            claim_for=CLAIM_FOR,
            backoffs=backoffs,
        )

    return build


@pytest.fixture
def observed_leases(key_prefix: str, attempts: Attempts) -> Callable[..., ObservedLeases]:
    """Leases over a client whose lapse claims' attempts go to `attempts`."""

    def build(client: Redis, *, claim_for: timedelta = CLAIM_FOR) -> ObservedLeases:
        return ObservedLeases(
            client,
            key_prefix=key_prefix,
            family="owner",
            lasts=LASTS,
            claim_for=claim_for,
            attempts=attempts,
        )

    return build


@pytest.fixture
def leases_through_proxy(
    proxied_client: Redis, recording_leases: Callable[[Redis, Backoffs], BackoffRecordingLeases]
) -> Callable[[Backoffs], BackoffRecordingLeases]:
    """Leases through the proxy whose back-offs go to `backoffs`."""
    return lambda backoffs: recording_leases(proxied_client, backoffs)


async def _process_line(process: asyncio.subprocess.Process) -> str:
    assert process.stdout is not None
    return (await within(process.stdout.readline())).decode().strip()


async def _start_process(*arguments: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.support.lease_process",
        *arguments,
        stdout=asyncio.subprocess.PIPE,
    )


async def test_the_lapse_of_a_killed_pod_reaches_one_handler_and_another_if_that_claimer_dies(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    """Spec: LEASE-16."""
    client = make_client(redis_url)
    leases = new_leases(client, lasts=timedelta(seconds=2), claim_for=CLAIM_FOR)
    holder = await _start_process("renew", redis_url, key_prefix, "pod-a", "2000")
    claimer: asyncio.subprocess.Process | None = None
    try:
        assert await _process_line(holder) == "RENEWED"
        holder.send_signal(signal.SIGKILL)
        assert await liveness_of(leases, "pod-a") is Liveness.ALIVE

        claimer = await _start_process("serve", redis_url, key_prefix, "300")
        assert await _process_line(claimer) == "HANDLING pod-a"
        handled = Handled()
        async with serving(leases, handled):
            await until_redis_ms(client, await redis_ms(client) + 1000)
            assert handled.items == []
            claimer.send_signal(signal.SIGKILL)
            (lapse,) = await handled.received()
        assert lapse.holder == "pod-a"
    finally:
        for process in (holder, claimer):
            if process is not None:
                if process.returncode is None:
                    process.kill()
                await process.wait()


async def test_a_handler_that_runs_past_claim_for_keeps_its_lapse_from_others_and_done_follows(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    """Spec: LEASE-17, LEASE-18."""
    client = make_client(redis_url)
    first = new_leases(client, lasts=LASTS, claim_for=CLAIM_FOR)
    second = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await first.renew("pod-a")
    running, other = Handled(hold=True), Handled()

    async with serving(first, running):
        await running.received()
        async with serving(second, other):
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)
            assert other.items == []
            running.release()
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)

    assert len(running.items) == 1
    assert other.items == []


async def test_a_connection_drop_is_ridden_out_with_back_off_and_a_raising_handler_leaves_the_loop(
    proxy: TcpProxy,
    leases_through_proxy: Callable[[Backoffs], BackoffRecordingLeases],
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    backoffs: Backoffs,
):
    """Spec: LEASE-18, LEASE-20."""
    serving_leases = leases_through_proxy(backoffs)
    renewer = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    attempts: list[str] = []

    async def failing_once(lapse: object) -> None:
        attempts.append("attempt")
        if len(attempts) == 1:
            raise RuntimeError("the handler fails")

    task = asyncio.create_task(serving_leases.serve_lapses(failing_once, every=SERVING_EVERY))
    try:
        proxy.cut()
        await backoffs.reached(2)
        assert not task.done()
        proxy.restore()
        await renewer.renew("pod-a")
        await renewer.renew("pod-b")

        async def twice() -> bool:
            return len(attempts) >= 2

        await until(twice)
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert backoffs.delays[0] == BACKOFF_FIRST_SECONDS
    assert backoffs.delays[1] > backoffs.delays[0]


async def test_a_refusal_is_ridden_out_and_the_lapse_is_handled_once_the_acl_is_restored(
    url_denying: Callable[[str], Awaitable[str]],
    redis_url: str,
    observer: Redis,
    key_prefix: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    recording_leases: Callable[[Redis, Backoffs], BackoffRecordingLeases],
    backoffs: Backoffs,
):
    """Spec: LEASE-20."""
    leases = recording_leases(make_client(await url_denying("evalsha")), backoffs)
    await new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR).renew("pod-a")
    handled = Handled()

    async with serving(leases, handled) as task:
        await backoffs.reached(3)
        assert not task.done()
        await observer.acl_setuser(key_prefix, enabled=True, commands=["+evalsha"])
        (lapse,) = await handled.received()

    assert lapse.holder == "pod-a"


@pytest.mark.parametrize("lapses", [1, 25])
async def test_every_lapse_of_a_mass_lapse_reaches_a_handler(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    lapses: int,
):
    """Spec: LEASE-16."""
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    holders = [f"pod-{number}" for number in range(lapses)]
    for holder in holders:
        await leases.renew(holder)
    handled = Handled()

    async with serving(leases, handled):
        await handled.received(lapses)

    assert sorted(lapse.holder for lapse in handled.items) == sorted(holders)


async def test_the_claims_of_a_held_batch_do_not_expire_and_each_lapse_is_handled_once(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    """Spec: LEASE-17."""
    client = make_client(redis_url)
    first = new_leases(client, lasts=LASTS, claim_for=CLAIM_FOR)
    second = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    holders = ["pod-a", "pod-b", "pod-c"]
    for holder in holders:
        await first.renew(holder)
    for holder in holders:
        await until_gone(first, holder)
    running, other = Handled(hold=True), Handled()

    async with serving(first, running):
        await running.received()
        async with serving(second, other):
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)
            assert other.items == []
            running.release()
            await running.received(3)
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)

    assert sorted(lapse.holder for lapse in running.items) == holders
    assert other.items == []


async def test_a_short_outage_is_ridden_out_with_real_waits_and_the_lapse_is_handled_after_it(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    """Spec: LEASE-20."""
    through_proxy = new_leases(
        make_client(proxy.url, retry=Retry(NoBackoff(), 0)), lasts=LASTS, claim_for=CLAIM_FOR
    )
    renewer = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    handled = Handled()

    async with serving(through_proxy, handled):
        proxy.cut()
        await proxy.attempts_reached(1)
        proxy.restore()
        await renewer.renew("pod-a")
        await handled.received()


async def test_a_claim_that_runs_out_leaves_the_handler_running(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    """Spec: LEASE-17."""
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")
    running = Handled(hold=True)

    async with serving(leases, running) as task:
        await running.received()
        await observer.delete(lease_keys(key_prefix, "owner").lapse.claim)
        await until_redis_ms(client, await redis_ms(client) + 300)
        running.release()
        await until_redis_ms(client, await redis_ms(client) + 300)
        assert not task.done()


async def test_a_claim_that_cannot_be_extended_for_an_outage_leaves_the_handler_running(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    """Spec: LEASE-17."""
    client = make_client(redis_url)
    through_proxy = new_leases(
        make_client(proxy.url, retry=Retry(NoBackoff(), 0)), lasts=LASTS, claim_for=CLAIM_FOR
    )
    await new_leases(client, lasts=LASTS).renew("pod-a")
    running = Handled(hold=True)

    async with serving(through_proxy, running) as task:
        await running.received()
        proxy.cut()
        await until_redis_ms(client, await redis_ms(client) + 300)
        proxy.restore()
        running.release()
        await until_redis_ms(client, await redis_ms(client) + 300)
        assert not task.done()


_LOGGER = "tandem.leases"
_HOST_LOGGER = "host.handler"


@pytest.mark.parametrize(
    ("raised", "level", "outcome"),
    [(None, "info", Outcome.OK), (ValueError, "error", Outcome.FAILED)],
)
async def test_a_lapse_handling_writes_one_line_with_its_outcome(
    raised: type[Exception] | None,
    level: str,
    outcome: Outcome,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    """Spec: LOG-10, LOG-18."""
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")

    async def handle(lapse: object) -> None:
        if raised is not None:
            raise raised("the handler fails")

    async with serving(leases, handle):
        (line, *_) = await log_lines.await_of("lapse_handling.ended", _LOGGER)

    assert line["level"] == level
    assert line["outcome"] == outcome
    assert line["lease_family"] == "owner"
    assert line["holder_id"] == "pod-a"
    assert line["duration_ms"] >= 0
    assert line.get("error_type") == (raised.__name__ if raised else None)
    assert "exception" not in line


def _store_error_raised_directly() -> Exception:
    return StoreError("a refusal naming the store")


def _store_error_inside_a_group() -> Exception:
    return ExceptionGroup("the host's tasks failed", [StoreError("a refusal naming the store")])


def _host_error_raised_from_another() -> Exception:
    try:
        try:
            raise KeyError("the host's own failure")
        except KeyError as first:
            raise ValueError("the host gave up") from first
    except ValueError as wrapped:
        return wrapped


@pytest.mark.parametrize(
    ("failure", "error_type", "writes_traceback"),
    [
        (_store_error_raised_directly, StoreError, False),
        (store_error_behind_a_host_error, ValueError, False),
        (_store_error_inside_a_group, ExceptionGroup, False),
        (_host_error_raised_from_another, ValueError, True),
    ],
)
async def test_a_failed_handler_writes_its_traceback_on_its_own_line_unless_a_store_error_is_in_it(
    failure: Callable[[], Exception],
    error_type: type[Exception],
    writes_traceback: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    """Spec: LOG-10, LOG-25."""
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")

    async def handle(lapse: object) -> None:
        raise failure()

    async with serving(leases, handle):
        await log_lines.await_of("lapse_handling.ended", _LOGGER)

    # Both lines are written without a suspension between them, so one snapshot holds both.
    handling = log_lines.of("lapse_handling.ended", _LOGGER)
    tracebacks = log_lines.of("lapse_handler.failed", _LOGGER)
    assert {line["error_type"] for line in handling} == {error_type.__name__}
    assert not [line for line in handling if "exception" in line]
    assert len(tracebacks) == (len(handling) if writes_traceback else 0)
    for line in tracebacks:
        assert line["level"] == "error"
        assert line["error_type"] == error_type.__name__
        assert (line["lease_family"], line["holder_id"]) == ("owner", "pod-a")
        assert "Traceback" in line["exception"]
    log_lines.assert_none_holds("a refusal naming the store")


async def test_each_lapse_handling_has_its_own_trace_and_the_steps_of_its_handler_hang_under_it(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    """Spec: LEASE-19, LOG-27."""
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")
    await leases.renew("pod-b")

    async def handle(lapse: Lapse) -> None:
        async with StepTimer(
            "host_call.ended", logging.getLogger(_HOST_LOGGER), holder_id=lapse.holder
        ):
            pass

    async with serving(leases, handle):
        handling = await log_lines.await_of("lapse_handling.ended", _LOGGER, count=2)
        rounds = await log_lines.await_of("lapse_round.ended", _LOGGER)

    steps = log_lines.of("host_call.ended", _HOST_LOGGER)
    handling_traces = {line["trace_id"] for line in handling}
    assert None not in handling_traces
    assert len(handling_traces) == len(handling)
    assert handling_traces.isdisjoint(line["trace_id"] for line in rounds)
    assert None not in {line["trace_id"] for line in rounds}
    for line in handling:
        (step,) = [step for step in steps if step["holder_id"] == line["holder_id"]]
        assert step["trace_id"] == line["trace_id"]
        assert step.get("parent_span_id") == line.get("span_id")


@pytest.mark.parametrize(
    ("raised", "failed_count"),
    [(None, 0), (ValueError, 1)],
    ids=["a handled lapse", "a failed lapse counts in failed_count"],
)
async def test_a_round_claiming_no_lapse_writes_no_line_and_one_that_did_writes_its_counts(
    raised: type[Exception] | None,
    failed_count: int,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    """Spec: LOG-11, LOG-26."""
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)

    async def handle(lapse: Lapse) -> None:
        if raised is not None:
            raise raised("the handler fails")

    async with serving(leases, handle):
        await elapsed(5 * SERVING_EVERY.total_seconds())
        assert not log_lines.of("lapse_round.ended", _LOGGER)
        await leases.renew("pod-a")
        await log_lines.await_of("lapse_round.ended", _LOGGER)

    rounds = log_lines.of("lapse_round.ended", _LOGGER)
    first = rounds[0]
    assert (first["level"], first["outcome"]) == ("info", Outcome.OK)
    assert (first["lapse_count"], first["failed_count"]) == (1, failed_count)
    assert first["lease_family"] == "owner"
    assert first["duration_ms"] >= 0
    assert all(line["lapse_count"] > 0 for line in rounds)
    assert not [line for line in rounds if "failed_run_count" in line]


@pytest.mark.parametrize(
    ("handled_after_the_claim_may_have_run_out", "level"),
    [(True, "warning"), (False, "info")],
    ids=["handle ends after the claim may have run out", "handle ends while the claim surely runs"],
)
async def test_a_failed_extension_marks_the_claim_not_kept_only_once_it_may_have_run_out(
    handled_after_the_claim_may_have_run_out: bool,
    level: str,
    store_failure: StoreFailure,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    observed_leases: Callable[..., ObservedLeases],
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-10, LEASE-17."""
    await new_leases(make_client(redis_url), lasts=LASTS).renew("pod-a")
    running = Handled(hold=True)

    async with serving(observed_leases(store_failure.client, claim_for=LONG_CLAIM_FOR), running):
        await running.received()
        await store_failure.begin()
        await attempts.until_unsuccessful()
        if handled_after_the_claim_may_have_run_out:
            await elapsed(LONG_CLAIM_FOR.total_seconds())
            assert attempts.unsuccessful_count == 1
        running.release()
        (line,) = await log_lines.await_of("lapse_handling.ended", _LOGGER)

    assert (line["level"], line["outcome"]) == (level, Outcome.OK)
    assert (line["lease_family"], line["holder_id"]) == ("owner", "pod-a")
    assert line.get("claim_kept") == (False if handled_after_the_claim_may_have_run_out else None)


@pytest.mark.parametrize("raised", [None, RuntimeError], ids=["returning", "raising"])
async def test_a_handling_whose_claim_was_deleted_says_claim_not_kept_whether_it_returns_or_raises(
    raised: type[Exception] | None,
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    observed_leases: Callable[..., ObservedLeases],
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-10, LOG-22."""
    await observed_leases(make_client(redis_url)).renew("pod-a")
    running = Handled(hold=True)

    async def handle(lapse: Lapse) -> None:
        await running(lapse)
        if raised is not None:
            raise raised("the handler fails")

    async with serving(observed_leases(make_client(redis_url), claim_for=LONG_CLAIM_FOR), handle):
        (lapse,) = await running.received()
        await observer.delete(lease_keys(key_prefix, "owner").lapse.claim)
        await attempts.until_unsuccessful()
        await elapsed(LONG_CLAIM_FOR.total_seconds() / 2)  # 1.5 extension intervals
        assert attempts.unsuccessful_count == 1
        running.release()
        (line,) = await log_lines.await_of("lapse_handling.ended", _LOGGER)

    failed = raised is not None
    expected = ("error", Outcome.FAILED) if failed else ("warning", Outcome.OK)
    shown = (line["level"], line["outcome"])
    assert shown == expected
    assert line.get("error_type") == (raised.__name__ if raised else None)
    assert line["claim_kept"] is False
    assert (line["lease_family"], line["holder_id"]) == ("owner", "pod-a")
    log_lines.assert_none_holds(lapse.generation)


async def test_a_handling_whose_claim_was_extended_to_its_end_carries_no_claim_kept(
    redis_url: str,
    make_client: Callable[..., Redis],
    observed_leases: Callable[..., ObservedLeases],
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-10."""
    await observed_leases(make_client(redis_url)).renew("pod-a")
    running = Handled(hold=True)

    async with serving(observed_leases(make_client(redis_url)), running):
        await running.received()
        await attempts.reached(2)
        running.release()
        (line,) = await log_lines.await_of("lapse_handling.ended", _LOGGER)

    assert attempts.results[:2] == [True, True]
    assert (line["level"], line["outcome"]) == ("info", Outcome.OK)
    assert "claim_kept" not in line


async def test_a_whole_outage_writes_one_failed_line_and_one_recovery_counting_its_failed_rounds(
    proxy: TcpProxy,
    leases_through_proxy: Callable[[Backoffs], BackoffRecordingLeases],
    backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: LOG-11, LOG-26."""
    proxy.cut()

    async with serving(leases_through_proxy(backoffs), Handled()):
        await backoffs.reached(3)
        proxy.restore()
        failure, recovery = await log_lines.await_of("lapse_round.ended", _LOGGER, count=2)

    assert (failure["level"], failure["outcome"], failure["reason"]) == (
        "warning",
        Outcome.FAILED,
        "outage",
    )
    assert (failure["lapse_count"], failure["failed_count"]) == (0, 0)
    assert "failed_run_count" not in failure
    assert (recovery["level"], recovery["outcome"]) == ("info", Outcome.OK)
    assert recovery["failed_run_count"] == len(backoffs.delays)
    assert len(log_lines.of("lapse_round.ended", _LOGGER)) == 2


@pytest.mark.parametrize(
    ("earlier_failure", "failed_lapse_counts"),
    [(False, [1]), (True, [0])],
    ids=["first failure of the run", "later failure of the run"],
)
async def test_a_round_held_through_an_outage_writes_its_failed_line_only_as_the_first_of_the_run(
    earlier_failure: bool,
    failed_lapse_counts: list[int],
    proxy: TcpProxy,
    leases_through_proxy: Callable[[Backoffs], BackoffRecordingLeases],
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: LOG-11, LOG-26."""
    renewer = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    running = Handled(hold=True)
    if earlier_failure:
        proxy.cut()

    async with serving(leases_through_proxy(backoffs), running):
        if earlier_failure:
            await backoffs.reached(1)
        await renewer.renew("pod-a")
        if earlier_failure:
            await until_gone(renewer, "pod-a")
            proxy.restore()
        await running.received()
        proxy.cut()
        recorded = len(backoffs.delays)
        running.release()
        await backoffs.reached(recorded + 1)

    failed = [
        line
        for line in log_lines.of("lapse_round.ended", _LOGGER)
        if line["outcome"] == Outcome.FAILED
    ]
    assert [line["lapse_count"] for line in failed] == failed_lapse_counts
    assert {(line["level"], line["reason"]) for line in failed} == {("warning", "outage")}


@pytest.mark.parametrize(
    ("denied_from_start", "denied_while_handling", "lapse_count"),
    [("evalsha", None, 0), ("hset", None, 0), ("flushall", "zrem", 1)],
    ids=["at the sweep", "at the first claim", "after the first claim"],
)
async def test_a_refusal_writes_one_failed_line_with_its_reason_and_serve_lapses_goes_on(
    denied_from_start: str,
    denied_while_handling: str | None,
    lapse_count: int,
    url_denying: Callable[[str], Awaitable[str]],
    redis_url: str,
    observer: Redis,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    recording_leases: Callable[[Redis, Backoffs], BackoffRecordingLeases],
    backoffs: Backoffs,
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LOG-11, LOG-26, LEASE-20."""
    leases = recording_leases(make_client(await url_denying(denied_from_start)), backoffs)
    await new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR).renew("pod-a")
    running = Handled(hold=True)

    async with serving(leases, running) as task:
        if denied_while_handling is not None:
            await running.received()
            await observer.acl_setuser(
                key_prefix, enabled=True, commands=[f"-{denied_while_handling}"]
            )
            running.release()
        (failure, *_) = await log_lines.await_of("lapse_round.ended", _LOGGER)
        await backoffs.reached(1)
        assert not task.done()

    assert (failure["level"], failure["outcome"], failure["reason"]) == (
        "warning",
        Outcome.FAILED,
        "refused",
    )
    assert (failure["lapse_count"], failure["failed_count"]) == (lapse_count, 0)


async def test_a_round_cancelled_before_it_claimed_a_lapse_writes_no_line(
    proxy: TcpProxy,
    leases_through_proxy: Callable[[Backoffs], BackoffRecordingLeases],
    backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: LOG-11, LOG-26."""
    proxy.hold()

    async with serving(leases_through_proxy(backoffs), Handled()):
        await elapsed(3 * SERVING_EVERY.total_seconds())

    assert not log_lines.of_logger(_LOGGER)


async def test_a_cancelled_back_off_writes_no_line_beyond_the_failed_round_before_it(
    proxy: TcpProxy,
    leases_through_proxy: Callable[[Backoffs], BackoffRecordingLeases],
    held_backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: LOG-11, LOG-26."""
    proxy.cut()

    async with serving(leases_through_proxy(held_backoffs), Handled()):
        await held_backoffs.reached(1)

    assert [line["outcome"] for line in log_lines.of("lapse_round.ended", _LOGGER)] == [
        Outcome.FAILED
    ]


async def test_serve_lapses_takes_no_claimer_argument(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    leases = new_leases(make_client(redis_url))

    with pytest.raises(TypeError):
        leases.serve_lapses(Handled(), every=SERVING_EVERY, claimer="pod-1")  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize("claim_lost", [False, True], ids=["claim kept", "claim lost"])
async def test_a_cancellation_while_a_handler_runs_writes_handling_and_round_as_cancelled(
    claim_lost: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    observed_leases: Callable[..., ObservedLeases],
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-10, LOG-11, LOG-18, LOG-22."""
    await observed_leases(make_client(redis_url)).renew("pod-a")
    running = Handled(hold=True)

    async with serving(
        observed_leases(make_client(redis_url), claim_for=LONG_CLAIM_FOR), running
    ) as task:
        await running.received()
        if claim_lost:
            await observer.delete(lease_keys(key_prefix, "owner").lapse.claim)
            await attempts.until_unsuccessful()
        task.cancel()
        (line,) = await log_lines.await_of("lapse_handling.ended", _LOGGER)
        (round_line,) = await log_lines.await_of("lapse_round.ended", _LOGGER)

    assert line["outcome"] == Outcome.CANCELLED
    assert line["holder_id"] == "pod-a"
    assert "claim_kept" not in line
    assert (round_line["level"], round_line["outcome"]) == ("info", Outcome.CANCELLED)
    assert (round_line["lapse_count"], round_line["failed_count"]) == (1, 0)


async def test_a_lapse_that_reaches_two_handlers_writes_a_line_for_each(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LOG-23."""
    first = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    second = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await first.renew("pod-a")
    running, replay = Handled(hold=True), Handled(hold=True)

    async with serving(first, running):
        await running.received()
        await observer.delete(lease_keys(key_prefix, "owner").lapse.claim)
        async with serving(second, replay):
            await replay.received()
            running.release()
            replay.release()
            lines = await log_lines.await_of("lapse_handling.ended", _LOGGER, count=2)

    assert [line["holder_id"] for line in lines[:2]] == ["pod-a", "pod-a"]
