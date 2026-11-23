import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Any

import pytest
from logline import Outcome, current_traceparent, unit_trace
from redis.asyncio import Redis

from tandem import Liveness, LockLost, Locks, StoreError
from tests.support.attempts import Attempts, ExtendObservedLocks
from tests.support.deadlines import redis_ms, until_redis_ms
from tests.support.locks import FAMILY, wipe
from tests.support.logs import LogLines
from tests.support.store_failures import StoreFailure
from tests.support.waiting import BOUND_SECONDS, elapsed, within

_LOGGER = "tandem.locks"
LASTS = timedelta(milliseconds=600)
EVERY = timedelta(milliseconds=100)
NAME = "job-7f3a"


class _GatedLocks(Locks):
    """Locks whose extensions after the entry's and whose release wait at gates, open until a test
    closes one with `clear`."""

    def __init__(self, *args: Any, **keywords: Any) -> None:
        super().__init__(*args, **keywords)
        self.extending = asyncio.Event()
        self.extend_gate = asyncio.Event()
        self.releasing = asyncio.Event()
        self.release_gate = asyncio.Event()
        self.extend_gate.set()
        self.release_gate.set()
        self.release_traceparent: str | None = None
        self._extensions = 0

    async def _extend_untimed(self, name: str, generation: str) -> bool:
        self._extensions += 1
        if self._extensions > 1:
            self.extending.set()
            await self.extend_gate.wait()
        return await super()._extend_untimed(name, generation)

    async def _release_untimed(self, name: str, generation: str) -> bool:
        self.releasing.set()
        self.release_traceparent = current_traceparent()
        await self.release_gate.wait()
        return await super()._release_untimed(name, generation)


async def _never() -> None:
    await within(asyncio.Event().wait())


async def _absorbs_one_cancel_and_returns() -> None:
    with contextlib.suppress(asyncio.CancelledError):
        await _never()


async def _absorbs_one_cancel_and_waits_again() -> None:
    await _absorbs_one_cancel_and_returns()
    await _never()


async def _answers_a_cancel_with_its_own_exception() -> None:
    try:
        await _never()
    except asyncio.CancelledError:
        raise ValueError("its own") from None


async def _waits_inside_a_timeout() -> None:
    async with asyncio.timeout(BOUND_SECONDS):
        await asyncio.Event().wait()


async def _waits_inside_a_task_group() -> None:
    async with asyncio.TaskGroup() as group:
        group.create_task(asyncio.Event().wait())


@pytest.fixture
def pod(
    redis_url: str, make_client: Callable[..., Redis], new_locks: Callable[..., Locks]
) -> Locks:
    return new_locks(make_client(redis_url), lasts=LASTS)


@pytest.fixture
def new_observed_locks(
    key_prefix: str, settled_epoch: None, attempts: Attempts
) -> Callable[[Redis], Locks]:
    def build(client: Redis) -> Locks:
        return ExtendObservedLocks(
            client, key_prefix=key_prefix, family=FAMILY, lasts=LASTS, attempts=attempts
        )

    return build


@pytest.fixture
def new_gated_locks(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str, settled_epoch: None
) -> Callable[..., _GatedLocks]:
    def build(url: str | None = None) -> _GatedLocks:
        return _GatedLocks(
            make_client(redis_url if url is None else url),
            key_prefix=key_prefix,
            family=FAMILY,
            lasts=LASTS,
        )

    return build


async def _acquired(locks: Locks) -> str:
    generation = await locks.acquire(NAME)
    assert generation is not None
    return generation


async def _held_through_a_run_of_failures(
    locks: Locks,
    store_failure: StoreFailure,
    attempts: Attempts,
    log_lines: LogLines,
    *,
    written: int,
) -> None:
    """One `held` block whose extensions fail twice, then succeed, until `written` lines exist."""
    generation = await _acquired(locks)
    async with locks.held(NAME, generation, every=EVERY):
        failed_before = sum(isinstance(result, StoreError) for result in attempts.results)
        await store_failure.begin()
        await attempts.failed(failed_before + 2)
        await store_failure.end()
        await log_lines.await_of("lock_keeping.ended", _LOGGER, count=written)


async def test_held_extends_before_its_block_runs_then_every_every_and_releases_at_exit(
    pod: Locks, observer: Redis, log_lines: LogLines
):
    """Spec: LOCK-12, LOG-36."""
    generation = await _acquired(pod)

    async with pod.held(NAME, generation, every=EVERY):
        assert len(log_lines.of("lock_extension.ended", _LOGGER)) == 1
        two_lasts_ms = 2 * LASTS // timedelta(milliseconds=1)
        await until_redis_ms(observer, await redis_ms(observer) + two_lasts_ms)
        assert (await pod.state(NAME)).generation == generation

    assert (await pod.state(NAME)).liveness is Liveness.GONE
    assert len(log_lines.of("lock_extension.ended", _LOGGER)) == 1
    assert len(log_lines.of("lock_release.ended", _LOGGER)) == 1
    assert not log_lines.of("lock_keeping.ended", _LOGGER)


@pytest.mark.parametrize(
    "every",
    [timedelta(0), timedelta(milliseconds=-1), LASTS, 0.1],
    ids=["zero", "negative", "equal-to-lasts", "not-a-timedelta"],
)
async def test_every_that_is_not_a_positive_timedelta_below_lasts_raises_at_the_call(
    every: Any, pod: Locks
):
    """Spec: LOCK-13."""
    with pytest.raises(ValueError, match="every"):
        pod.held(NAME, "any generation", every=every)


@pytest.mark.parametrize("cancel_on_loss", [True, False])
async def test_an_entry_with_a_generation_that_is_not_live_raises_lock_lost_and_releases_nothing(
    cancel_on_loss: bool, pod: Locks
):
    """Spec: LOCK-14."""
    holder = await _acquired(pod)

    entering = pod.held(NAME, "not the holder's", every=EVERY, cancel_on_loss=cancel_on_loss)
    body_ran = False

    async def enter() -> None:
        nonlocal body_ran
        async with entering:
            body_ran = True

    with pytest.raises(LockLost):
        await enter()

    assert not body_ran
    assert (await pod.state(NAME)).generation == holder


async def test_a_refusal_at_the_entry_raises_store_error_and_the_body_does_not_run(
    url_with_wrong_password: str, make_client: Callable[..., Redis], new_locks: Callable[..., Locks]
):
    """Spec: LOCK-14."""
    entering = new_locks(make_client(url_with_wrong_password), lasts=LASTS).held(
        NAME, "any generation", every=EVERY
    )
    body_ran = False

    async def enter() -> None:
        nonlocal body_ran
        async with entering:
            body_ran = True

    with pytest.raises(StoreError):
        await enter()

    assert not body_ran


async def test_a_run_of_failed_extensions_is_ridden_out_and_writes_its_first_failure_and_recovery(
    store_failure: StoreFailure,
    new_observed_locks: Callable[[Redis], Locks],
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOCK-15, LOG-33."""
    locks = new_observed_locks(store_failure.client)
    generation = await _acquired(locks)

    async with locks.held(NAME, generation, every=EVERY):
        await store_failure.begin()
        await attempts.failed(2)
        await store_failure.end()
        (failure, recovery) = await log_lines.await_of("lock_keeping.ended", _LOGGER, count=2)
        await attempts.reached(len(attempts.results) + 2)
        assert (await locks.state(NAME)).generation == generation

    failures = sum(isinstance(result, StoreError) for result in attempts.results)
    assert log_lines.of("lock_keeping.ended", _LOGGER) == [failure, recovery]
    assert (failure["level"], failure["outcome"], failure["reason"]) == (
        "warning",
        Outcome.FAILED,
        store_failure.reason,
    )
    assert "exception" not in failure
    assert (recovery["level"], recovery["outcome"]) == ("info", Outcome.OK)
    assert 2 <= recovery["failed_run_count"] == failures
    for line in (failure, recovery):
        assert line["lock_family"] == FAMILY
        assert line["duration_ms"] >= 0
    assert not log_lines.of("lock.lost", _LOGGER)
    log_lines.assert_none_holds(generation)
    log_lines.assert_none_holds(NAME)


async def test_the_lines_of_a_run_of_failed_extensions_are_written_in_the_trace_that_entered_held(
    store_failure: StoreFailure,
    new_observed_locks: Callable[[Redis], Locks],
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-33."""
    locks = new_observed_locks(store_failure.client)

    with unit_trace(None):
        traceparent = current_traceparent()
        assert traceparent is not None
        await _held_through_a_run_of_failures(locks, store_failure, attempts, log_lines, written=2)

    lines = log_lines.of("lock_keeping.ended", _LOGGER)
    assert [line["trace_id"] for line in lines] == [traceparent.split("-")[1]] * 2


async def test_a_run_of_failed_extensions_counts_per_held_block(
    store_failure: StoreFailure,
    new_observed_locks: Callable[[Redis], Locks],
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-33."""
    locks = new_observed_locks(store_failure.client)

    await _held_through_a_run_of_failures(locks, store_failure, attempts, log_lines, written=2)
    failures_of_first_block = sum(isinstance(result, StoreError) for result in attempts.results)
    await _held_through_a_run_of_failures(locks, store_failure, attempts, log_lines, written=4)
    failures = sum(isinstance(result, StoreError) for result in attempts.results)

    lines = log_lines.of("lock_keeping.ended", _LOGGER)
    assert [line["level"] for line in lines] == ["warning", "info", "warning", "info"]
    assert lines[-1]["failed_run_count"] == failures - failures_of_first_block


async def test_a_loss_in_cancel_mode_ends_the_block_with_lock_lost_and_releases_nothing(
    pod: Locks, observer: Redis, key_prefix: str, log_lines: LogLines
):
    """Spec: LOCK-16, LOG-34."""
    generation = await _acquired(pod)
    task = asyncio.current_task()
    assert task is not None

    entering = pod.held(NAME, generation, every=EVERY)

    async def block_that_loses_its_lock() -> None:
        async with entering:
            await wipe(observer, key_prefix)
            await _never()

    with pytest.raises(LockLost) as lost:
        await block_that_loses_its_lock()

    assert lost.value.__suppress_context__
    assert task.cancelling() == 0
    (line,) = log_lines.of("lock.lost", _LOGGER)
    assert (line["level"], line["lock_family"]) == ("warning", FAMILY)
    assert not log_lines.of("lock_release.ended", _LOGGER)
    log_lines.assert_none_holds(generation)
    log_lines.assert_none_holds(NAME)


@pytest.mark.parametrize(
    ("ending", "raised"),
    [
        (_never, LockLost),
        (_absorbs_one_cancel_and_waits_again, LockLost),
        (_waits_inside_a_timeout, LockLost),
        (_waits_inside_a_task_group, LockLost),
        (_absorbs_one_cancel_and_returns, None),
        (_answers_a_cancel_with_its_own_exception, ValueError),
    ],
    ids=[
        "propagates-the-cancel",
        "absorbs-the-first-cancel-and-is-cancelled-again",
        "inner-timeout-neither-turns-it-into-timeout-error-nor-swallows-it",
        "inner-task-group-neither-swallows-it-nor-replaces-it",
        "absorbs-the-cancel-and-returns",
        "answers-the-cancel-with-its-own-exception",
    ],
)
async def test_a_loss_in_cancel_mode_ends_the_block_as_the_block_ends(
    ending: Callable[[], Awaitable[None]],
    raised: type[Exception] | None,
    pod: Locks,
    observer: Redis,
    key_prefix: str,
):
    """Spec: LOCK-16."""
    generation = await _acquired(pod)
    task = asyncio.current_task()
    assert task is not None

    async def block() -> None:
        async with pod.held(NAME, generation, every=EVERY):
            await wipe(observer, key_prefix)
            await ending()

    if raised is None:
        await block()
    else:
        with pytest.raises(raised):
            await block()

    assert task.cancelling() == 0


@pytest.mark.parametrize("cancel_on_loss", [True, False])
async def test_the_hosts_own_cancellation_propagates_and_the_exit_releases_the_lock(
    cancel_on_loss: bool, pod: Locks
):
    """Spec: LOCK-16, LOCK-17."""
    generation = await _acquired(pod)
    inside = asyncio.Event()

    async def block() -> None:
        async with pod.held(NAME, generation, every=EVERY, cancel_on_loss=cancel_on_loss):
            inside.set()
            await _never()

    task = asyncio.create_task(block())
    await within(inside.wait())
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert (await pod.state(NAME)).liveness is Liveness.GONE


async def test_the_hosts_cancellation_after_the_keepers_stays_a_cancellation_not_a_lock_lost(
    pod: Locks, observer: Redis, key_prefix: str
):
    """Spec: LOCK-16."""
    generation = await _acquired(pod)
    absorbed = asyncio.Event()

    async def block() -> None:
        async with pod.held(NAME, generation, every=EVERY):
            await wipe(observer, key_prefix)
            with contextlib.suppress(asyncio.CancelledError):
                await _never()
            absorbed.set()
            await _never()

    task = asyncio.create_task(block())
    await within(absorbed.wait())
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()


async def test_a_loss_in_tell_only_mode_is_only_told_and_the_block_exits_in_another_task(
    new_observed_locks: Callable[[Redis], Locks],
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOCK-16, LOG-34."""
    locks = new_observed_locks(make_client(redis_url))
    generation = await _acquired(locks)
    task = asyncio.current_task()
    assert task is not None
    stack = AsyncExitStack()
    await stack.enter_async_context(locks.held(NAME, generation, every=EVERY, cancel_on_loss=False))

    await wipe(observer, key_prefix)
    (line,) = await log_lines.await_of("lock.lost", _LOGGER)
    await elapsed(3 * EVERY.total_seconds())
    await asyncio.create_task(stack.aclose())

    assert (line["level"], line["lock_family"]) == ("warning", FAMILY)
    assert task.cancelling() == 0
    assert attempts.unsuccessful_count == 1  # the extensions stopped at the first False
    assert not log_lines.of("lock_release.ended", _LOGGER)
    assert len(log_lines.of("lock.lost", _LOGGER)) == 1


@pytest.mark.parametrize("cancel_on_loss", [True, False])
@pytest.mark.parametrize("raises", [False, True], ids=["returns", "raises-its-own-exception"])
async def test_a_false_from_an_extension_in_flight_at_the_exit_changes_neither_ending_nor_release(
    cancel_on_loss: bool,
    raises: bool,
    new_gated_locks: Callable[..., _GatedLocks],
    key_prefix: str,
    observer: Redis,
    log_lines: LogLines,
):
    """Spec: LOCK-16, LOCK-17."""
    locks = new_gated_locks()
    locks.extend_gate.clear()
    generation = await _acquired(locks)
    block_ended = asyncio.Event()

    async def block() -> None:
        async with locks.held(NAME, generation, every=EVERY, cancel_on_loss=cancel_on_loss):
            await locks.extending.wait()
            await wipe(observer, key_prefix)
            block_ended.set()
            if raises:
                raise ValueError("its own")

    task = asyncio.create_task(block())
    await within(block_ended.wait())
    locks.extend_gate.set()
    (result,) = await asyncio.gather(task, return_exceptions=True)

    assert isinstance(result, ValueError) if raises else result is None
    assert task.cancelling() == 0
    assert len(log_lines.of("lock.lost", _LOGGER)) == 1
    assert not log_lines.of("lock_release.ended", _LOGGER)


async def test_the_exit_waits_for_an_extension_in_flight_before_it_releases(
    new_gated_locks: Callable[..., _GatedLocks], log_lines: LogLines
):
    """Spec: LOCK-17."""
    locks = new_gated_locks()
    locks.extend_gate.clear()
    generation = await _acquired(locks)
    block_ended = asyncio.Event()

    async def block() -> None:
        async with locks.held(NAME, generation, every=EVERY):
            await locks.extending.wait()
            block_ended.set()

    task = asyncio.create_task(block())
    await within(block_ended.wait())
    locks.extend_gate.set()
    await task

    assert (await locks.state(NAME)).liveness is Liveness.GONE
    assert not log_lines.of("lock.lost", _LOGGER)


async def test_a_cancellation_in_the_first_exit_phase_is_held_until_both_phases_end(
    new_gated_locks: Callable[..., _GatedLocks],
):
    """Spec: LOCK-17."""
    locks = new_gated_locks()
    locks.extend_gate.clear()
    generation = await _acquired(locks)
    cancelled_itself = asyncio.Event()

    async def block() -> None:
        async with locks.held(NAME, generation, every=EVERY):
            await locks.extending.wait()
            task = asyncio.current_task()
            assert task is not None
            task.cancel()  # lands at the exit's first await, which waits for the extension
            cancelled_itself.set()

    task = asyncio.create_task(block())
    await within(cancelled_itself.wait())
    locks.extend_gate.set()
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert (await locks.state(NAME)).liveness is Liveness.GONE


async def test_a_cancellation_in_the_first_exit_phase_that_hides_a_failed_release_logs_it_once(
    url_denying: Callable[[str], Awaitable[str]],
    new_gated_locks: Callable[..., _GatedLocks],
    observer: Redis,
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LOCK-17, LOG-35."""
    locks = new_gated_locks(await url_denying("flushall"))
    locks.extend_gate.clear()
    generation = await _acquired(locks)
    cancelled_itself = asyncio.Event()

    async def block() -> None:
        async with locks.held(NAME, generation, every=EVERY):
            await locks.extending.wait()
            task = asyncio.current_task()
            assert task is not None
            task.cancel()  # lands at the exit's first await, which waits for the extension
            cancelled_itself.set()

    task = asyncio.create_task(block())
    await within(cancelled_itself.wait())
    await observer.acl_setuser(key_prefix, enabled=True, commands=["-evalsha"])
    locks.extend_gate.set()
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    (failure,) = log_lines.of("lock_release.failed", _LOGGER)
    assert failure["error_type"] == StoreError.__name__


async def test_the_release_runs_inside_its_own_span_in_the_exiting_task(
    new_gated_locks: Callable[..., _GatedLocks],
):
    """Spec: LOCK-17, LOG-5."""
    locks = new_gated_locks()
    generation = await _acquired(locks)

    with unit_trace(None):
        async with locks.held(NAME, generation, every=EVERY):
            traceparent_of_the_block = current_traceparent()

    assert traceparent_of_the_block is not None
    assert locks.release_traceparent not in (None, traceparent_of_the_block)


async def test_a_cancellation_in_the_second_exit_phase_is_held_until_the_release_ends(
    new_gated_locks: Callable[..., _GatedLocks],
):
    """Spec: LOCK-17."""
    locks = new_gated_locks()
    locks.release_gate.clear()
    generation = await _acquired(locks)

    async def block() -> None:
        async with locks.held(NAME, generation, every=EVERY):
            pass

    task = asyncio.create_task(block())
    await within(locks.releasing.wait())
    task.cancel()
    locks.release_gate.set()
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert (await locks.state(NAME)).liveness is Liveness.GONE


@pytest.mark.parametrize("release_fails", [False, True])
async def test_a_cancelled_exit_logs_a_failed_release_once_and_the_cancellation_propagates(
    release_fails: bool,
    url_denying: Callable[[str], Awaitable[str]],
    new_gated_locks: Callable[..., _GatedLocks],
    observer: Redis,
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LOCK-17, LOG-35."""
    locks = new_gated_locks(await url_denying("flushall"))
    locks.release_gate.clear()
    generation = await _acquired(locks)
    inside = asyncio.Event()

    async def block() -> None:
        async with locks.held(NAME, generation, every=EVERY):
            inside.set()
            await _never()

    task = asyncio.create_task(block())
    await within(inside.wait())
    task.cancel()
    await within(locks.releasing.wait())
    task.cancel()  # the exit is cancelled again, while it waits for its release
    if release_fails:
        await observer.acl_setuser(key_prefix, enabled=True, commands=["-evalsha"])
    locks.release_gate.set()
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    failures = log_lines.of("lock_release.failed", _LOGGER)
    assert len(failures) == (1 if release_fails else 0)
    for failure in failures:
        assert failure["level"] == "error"
        assert failure["error_type"] == StoreError.__name__
        assert failure["lock_family"] == FAMILY
    errors = [line for line in log_lines.of_logger(_LOGGER) if line["level"] == "error"]
    assert errors == failures
    if not release_fails:
        assert (await locks.state(NAME)).liveness is Liveness.GONE


@pytest.mark.parametrize("release_fails", [False, True])
async def test_an_uncancelled_exit_writes_lock_release_ended_like_a_direct_release(
    release_fails: bool,
    url_denying: Callable[[str], Awaitable[str]],
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LOCK-17, LOG-5, LOG-36."""
    locks = new_locks(make_client(await url_denying("flushall")), lasts=LASTS)
    generation = await _acquired(locks)

    async def hold_then_exit() -> None:
        async with locks.held(NAME, generation, every=EVERY):
            if release_fails:
                await observer.acl_setuser(key_prefix, enabled=True, commands=["-evalsha"])

    if release_fails:
        with pytest.raises(StoreError):
            await hold_then_exit()
    else:
        await hold_then_exit()

    (line,) = log_lines.of("lock_release.ended", _LOGGER)
    assert line["level"] == ("error" if release_fails else "info")
    assert line["outcome"] == (Outcome.FAILED if release_fails else Outcome.OK)
    assert line.get("error_type") == (StoreError.__name__ if release_fails else None)
    assert line["lock_family"] == FAMILY
    assert not log_lines.of("lock_release.failed", _LOGGER)


async def test_cancel_mode_exited_in_another_task_raises_runtime_error_after_the_lock_is_released(
    pod: Locks,
):
    """Spec: LOCK-16, LOCK-17."""
    generation = await _acquired(pod)
    stack = AsyncExitStack()
    await stack.enter_async_context(pod.held(NAME, generation, every=EVERY))

    exit_task = asyncio.create_task(stack.aclose())

    with pytest.raises(RuntimeError):
        await exit_task

    assert (await pod.state(NAME)).liveness is Liveness.GONE


async def test_cancel_mode_exited_in_another_task_withdraws_the_cancels_the_keeper_delivered(
    pod: Locks, observer: Redis, key_prefix: str
):
    """Spec: LOCK-16, LOCK-17."""
    generation = await _acquired(pod)
    task = asyncio.current_task()
    assert task is not None
    stack = AsyncExitStack()
    await stack.enter_async_context(pod.held(NAME, generation, every=EVERY))
    await wipe(observer, key_prefix)
    await _absorbs_one_cancel_and_returns()  # the keeper's cancel lands and is absorbed
    exit_task = asyncio.create_task(stack.aclose())

    with pytest.raises(RuntimeError):
        await exit_task

    assert task.cancelling() == 0
