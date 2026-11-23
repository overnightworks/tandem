import asyncio
from collections.abc import Callable
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Any

import pytest
from logline import Outcome, current_traceparent, unit_trace
from redis.asyncio import Redis

from tandem import Leases, Liveness, Renewal, StoreError
from tandem.errors import BACKOFF_FIRST_SECONDS
from tests.support.attempts import Attempts, RenewalObservedLeases
from tests.support.deadlines import redis_ms, until_redis_ms
from tests.support.leases import liveness_of, until_gone
from tests.support.logs import LogLines
from tests.support.proxy import TcpProxy
from tests.support.store_failures import StoreFailure
from tests.support.waiting import until, within

_LOGGER = "tandem.leases"
LASTS = timedelta(milliseconds=600)
EVERY = timedelta(milliseconds=100)


class _GatedLeases(Leases):
    """Leases whose renewals after the first wait at a gate the test opens."""

    def __init__(self, *args: Any, **keywords: Any) -> None:
        super().__init__(*args, **keywords)
        self.entered = asyncio.Event()
        self.gate = asyncio.Event()
        self._calls = 0

    async def _renew_untimed(self, holder: str) -> Renewal:
        self._calls += 1
        if self._calls > 1:
            self.entered.set()
            await self.gate.wait()
        return await super()._renew_untimed(holder)


class _GatedEnd(Leases):
    """Leases whose `end` waits at a gate the test opens."""

    def __init__(self, *args: Any, **keywords: Any) -> None:
        super().__init__(*args, **keywords)
        self.ending = asyncio.Event()
        self.gate = asyncio.Event()
        self.ended = False

    async def _end_untimed(self, holder: str, generation: str) -> bool:
        self.ending.set()
        await self.gate.wait()
        ended = await super()._end_untimed(holder, generation)
        self.ended = True
        return ended


async def _exiting_at_its_gate(
    client: Redis, key_prefix: str
) -> tuple[_GatedEnd, asyncio.Task[None]]:
    """A task cancelled inside `kept`, whose `end` now waits at the gate."""
    leases = _GatedEnd(
        client,
        key_prefix=key_prefix,
        family="owner",
        lasts=LASTS,
        claim_for=timedelta(seconds=30),
    )
    inside = asyncio.Event()

    async def body() -> None:
        async with leases.kept("pod-a", every=EVERY):
            inside.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(body())
    await within(inside.wait())
    task.cancel()
    await within(leases.ending.wait())
    return leases, task


def _observed_leases(client: Redis, key_prefix: str, attempts: Attempts) -> RenewalObservedLeases:
    return RenewalObservedLeases(
        client,
        key_prefix=key_prefix,
        family="owner",
        lasts=LASTS,
        claim_for=timedelta(seconds=30),
        attempts=attempts,
    )


async def _kept_through_a_run_of_failures(
    leases: Leases,
    store_failure: StoreFailure,
    attempts: Attempts,
    log_lines: LogLines,
    *,
    written: int,
) -> None:
    """One `kept` block whose renewals fail twice and then succeed, until `written` lines exist."""
    async with leases.kept("pod-a", every=EVERY):
        failed_before = sum(isinstance(result, StoreError) for result in attempts.results)
        await store_failure.begin()
        await attempts.failed(failed_before + 2)
        await store_failure.end()
        await log_lines.await_of("lease_keeping.ended", _LOGGER, count=written)


async def test_kept_renews_before_its_body_runs_and_ends_the_lease_at_exit(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    """Spec: LEASE-11."""
    leases = new_leases(make_client(redis_url), lasts=LASTS)

    async with leases.kept("pod-a", every=EVERY) as hold:
        assert await liveness_of(leases, "pod-a") is Liveness.ALIVE
        assert await leases.renew("pod-a") == Renewal(started=False, generation=hold.generation)

    await until_gone(leases, "pod-a")


async def test_kept_renews_while_its_body_runs_and_writes_no_line_for_a_renewal_that_succeeds(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    """Spec: LEASE-11, LOG-19."""
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS)

    async with leases.kept("pod-a", every=EVERY):
        two_lasts_ms = 2 * LASTS // timedelta(milliseconds=1)
        await until_redis_ms(client, await redis_ms(client) + two_lasts_ms)
        assert await liveness_of(leases, "pod-a") is Liveness.ALIVE

    assert len(log_lines.of("lease_renewal.ended", "tandem.leases")) == 1
    assert not log_lines.of("lease_keeping.ended", _LOGGER)


async def test_a_refusal_at_the_first_renewal_raises_out_of_the_entry(
    url_with_wrong_password: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    """Spec: LEASE-14."""
    leases = new_leases(make_client(url_with_wrong_password), lasts=LASTS)

    held = leases.kept("pod-a", every=EVERY)

    with pytest.raises(StoreError):
        async with held:
            pytest.fail("the body must not run")


async def test_a_run_of_failed_renewals_writes_its_first_failure_and_its_recovery_only(
    store_failure: StoreFailure,
    key_prefix: str,
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LEASE-14, LOG-8, LOG-19."""
    leases = _observed_leases(store_failure.client, key_prefix, attempts)

    async with leases.kept("pod-a", every=EVERY) as hold:
        entered = hold.generation
        await store_failure.begin()
        await attempts.failed(2)
        await store_failure.end()
        (failure, recovery) = await log_lines.await_of("lease_keeping.ended", _LOGGER, count=2)
        await attempts.reached(len(attempts.results) + 2)
        generation = hold.generation

    failures = sum(isinstance(result, StoreError) for result in attempts.results)
    assert log_lines.of("lease_keeping.ended", _LOGGER) == [failure, recovery]
    assert (failure["level"], failure["outcome"], failure["reason"]) == (
        "warning",
        Outcome.FAILED,
        store_failure.reason,
    )
    assert "exception" not in failure
    assert (recovery["level"], recovery["outcome"]) == ("info", Outcome.OK)
    assert 2 <= recovery["failed_run_count"] == failures
    for line in (failure, recovery):
        assert (line["lease_family"], line["holder_id"]) == ("owner", "pod-a")
        assert line["duration_ms"] >= 0
    log_lines.assert_none_holds(entered)
    log_lines.assert_none_holds(generation)


async def test_the_lines_of_a_run_of_failed_renewals_are_written_in_the_trace_that_entered_kept(
    store_failure: StoreFailure,
    key_prefix: str,
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-8."""
    leases = _observed_leases(store_failure.client, key_prefix, attempts)

    with unit_trace(None):
        traceparent = current_traceparent()
        assert traceparent is not None
        await _kept_through_a_run_of_failures(leases, store_failure, attempts, log_lines, written=2)

    lines = log_lines.of("lease_keeping.ended", _LOGGER)
    assert [line["trace_id"] for line in lines] == [traceparent.split("-")[1]] * 2


async def test_a_run_of_failed_renewals_counts_per_kept_block(
    store_failure: StoreFailure,
    key_prefix: str,
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-8."""
    leases = _observed_leases(store_failure.client, key_prefix, attempts)

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(leases.kept("pod-a", every=EVERY))
        await store_failure.begin()
        await log_lines.await_of("lease_keeping.ended", _LOGGER)
        with pytest.raises(StoreError):  # the end at the exit cannot reach the failing store
            await stack.aclose()
    await store_failure.end()
    failures_of_first_block = sum(isinstance(result, StoreError) for result in attempts.results)
    await _kept_through_a_run_of_failures(leases, store_failure, attempts, log_lines, written=3)
    failures = sum(isinstance(result, StoreError) for result in attempts.results)

    lines = log_lines.of("lease_keeping.ended", _LOGGER)
    assert [line["level"] for line in lines] == ["warning", "warning", "info"]
    assert lines[-1]["failed_run_count"] == failures - failures_of_first_block


async def test_a_block_that_exits_during_a_run_of_failed_renewals_writes_nothing_more(
    proxied_client: Redis,
    proxy: TcpProxy,
    key_prefix: str,
    attempts: Attempts,
    log_lines: LogLines,
):
    """Spec: LOG-8."""
    leases = _observed_leases(proxied_client, key_prefix, attempts)

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(leases.kept("pod-a", every=EVERY))
        proxy.cut()
        await attempts.failed(2)
        with pytest.raises(StoreError):  # the end at the exit cannot reach the cut store
            await stack.aclose()

    assert [line["outcome"] for line in log_lines.of("lease_keeping.ended", _LOGGER)] == [
        Outcome.FAILED
    ]


async def test_a_failed_renewal_is_retried_after_every_even_when_every_exceeds_the_first_back_off(
    proxy: TcpProxy, proxied_client: Redis, new_leases: Callable[..., Leases]
):
    """Spec: LEASE-14."""
    every = timedelta(seconds=BACKOFF_FIRST_SECONDS * 1.6)
    leases = new_leases(proxied_client, lasts=2 * every)
    clock = asyncio.get_running_loop()

    async with leases.kept("pod-a", every=every):
        proxy.cut()
        await proxy.attempts_reached(1)
        first = clock.time()
        await proxy.attempts_reached(2)
        second = clock.time()
        proxy.restore()

    assert second - first >= every.total_seconds() * 0.9


async def test_a_holder_paused_past_lasts_sees_the_generation_of_its_new_lease(
    proxy: TcpProxy,
    proxied_client: Redis,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    """Spec: LEASE-12."""
    paused_pod = new_leases(proxied_client, lasts=LASTS)
    observer_pod = new_leases(make_client(redis_url), lasts=LASTS)

    async with paused_pod.kept("pod-a", every=EVERY) as hold:
        first = hold.generation
        proxy.cut()

        async def lapsed() -> bool:
            return await liveness_of(observer_pod, "pod-a") is Liveness.GONE

        await until(lapsed)
        proxy.restore()

        async def restarted() -> bool:
            return hold.generation != first

        await until(restarted)
        assert await observer_pod.renew("pod-a") == Renewal(
            started=False, generation=hold.generation
        )


async def test_cancelling_the_body_during_a_renewal_leaves_the_holder_gone_for_good(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    """Spec: LEASE-15."""
    client = make_client(redis_url)
    leases = _GatedLeases(
        client,
        key_prefix=key_prefix,
        family="owner",
        lasts=LASTS,
        claim_for=timedelta(seconds=30),
    )

    async def body() -> None:
        async with leases.kept("pod-a", every=EVERY):
            await asyncio.Event().wait()

    task = asyncio.create_task(body())
    await within(leases.entered.wait())
    task.cancel()
    leases.gate.set()
    await asyncio.gather(task, return_exceptions=True)

    await until_gone(leases, "pod-a")
    await until_redis_ms(client, await redis_ms(client) + 3 * 100)
    assert await liveness_of(leases, "pod-a") is Liveness.GONE


@pytest.mark.parametrize("end_fails", [False, True])
async def test_an_uncancelled_exit_writes_lease_end_ended_like_a_direct_end(
    end_fails: bool,
    refused_store: StoreFailure,
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    """Spec: LEASE-15, LOG-19."""
    leases = new_leases(refused_store.client, lasts=LASTS)

    async def hold_then_exit() -> None:
        async with leases.kept("pod-a", every=EVERY):
            if end_fails:
                await refused_store.begin()

    if end_fails:
        with pytest.raises(StoreError):
            await hold_then_exit()
    else:
        await hold_then_exit()

    (line,) = log_lines.of("lease_end.ended", "tandem.leases")
    assert line["level"] == ("error" if end_fails else "info")
    assert line["outcome"] == ("failed" if end_fails else "ok")
    assert line.get("error_type") == (StoreError.__name__ if end_fails else None)
    assert (line["lease_family"], line["holder_id"]) == ("owner", "pod-a")
    assert not log_lines.of("lease_end.failed", "tandem.leases")


@pytest.mark.parametrize("end_fails", [False, True])
async def test_a_cancelled_exit_waits_for_its_cleanup_before_the_cancellation_propagates(
    end_fails: bool,
    refused_store: StoreFailure,
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LEASE-15."""
    leases, task = await _exiting_at_its_gate(refused_store.client, key_prefix)
    task.cancel()
    if end_fails:
        await refused_store.begin()
    leases.gate.set()
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert leases.ended is not end_fails
    failures = log_lines.of("lease_end.failed", "tandem.leases")
    assert len(failures) == (1 if end_fails else 0)
    for failure in failures:
        assert failure["level"] == "error"
        assert failure["error_type"] == StoreError.__name__
        assert (failure["lease_family"], failure["holder_id"]) == ("owner", "pod-a")
    errors = [line for line in log_lines.of_logger("tandem.leases") if line["level"] == "error"]
    assert errors == failures


async def test_cancelling_the_exit_and_its_cleanup_together_ends_the_task_cancelled_unlogged(
    redis_url: str,
    make_client: Callable[..., Redis],
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LEASE-15, LOG-9."""
    earlier = asyncio.all_tasks()
    _, task = await _exiting_at_its_gate(make_client(redis_url), key_prefix)
    for started_since in asyncio.all_tasks() - earlier:
        started_since.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert not [
        line
        for line in log_lines.of_logger("tandem.leases")
        if line["level"] in ("warning", "error")
    ]
