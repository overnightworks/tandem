import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import Leases, Liveness, Renewal, StoreError
from tandem.errors import BACKOFF_FIRST_SECONDS
from tests.support.deadlines import redis_ms, until_redis_ms
from tests.support.leases import liveness_of, until_gone
from tests.support.logs import LogLines
from tests.support.proxy import TcpProxy
from tests.support.waiting import until, within

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


async def test_kept_renews_before_its_body_runs_and_ends_the_lease_at_exit(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
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
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS)

    async with leases.kept("pod-a", every=EVERY):
        two_lasts_ms = 2 * LASTS // timedelta(milliseconds=1)
        await until_redis_ms(client, await redis_ms(client) + two_lasts_ms)
        assert await liveness_of(leases, "pod-a") is Liveness.ALIVE

    assert len(log_lines.of("lease_renewal.ended", "tandem.leases")) == 1


async def test_a_refusal_at_the_first_renewal_raises_out_of_the_entry(
    url_with_wrong_password: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    leases = new_leases(make_client(url_with_wrong_password), lasts=LASTS)

    held = leases.kept("pod-a", every=EVERY)

    with pytest.raises(StoreError):
        async with held:
            pytest.fail("the body must not run")


async def test_a_refusal_at_a_later_renewal_is_logged_and_retried_and_the_body_runs_on(
    url_denying: Callable[[str], Awaitable[str]],
    redis_url: str,
    observer: Redis,
    key_prefix: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    holder_pod = new_leases(make_client(await url_denying("flushall")), lasts=LASTS)
    observer_pod = new_leases(make_client(redis_url), lasts=LASTS)

    async def alive() -> bool:
        return await liveness_of(observer_pod, "pod-a") is Liveness.ALIVE

    async with holder_pod.kept("pod-a", every=EVERY):
        await observer.acl_setuser(key_prefix, enabled=True, commands=["-evalsha"])
        await until_gone(observer_pod, "pod-a")
        await observer.acl_setuser(key_prefix, enabled=True, commands=["+evalsha"])
        await until(alive)

    failures = log_lines.of("lease_renewal.failed", "tandem.leases")
    assert failures
    assert {(line["level"], line["reason"]) for line in failures} == {("warning", "refused")}
    assert {line["error_type"] for line in failures} == {StoreError.__name__}
    assert {(line["lease_family"], line["holder_id"]) for line in failures} == {("owner", "pod-a")}


async def test_an_outage_at_a_later_renewal_is_logged_as_a_warning_without_a_traceback(
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    leases = new_leases(
        make_client(proxy.url, retry=Retry(NoBackoff(), 0)),
        lasts=LASTS,
    )

    async with leases.kept("pod-a", every=EVERY) as hold:
        entered = hold.generation
        proxy.cut()

        async def failure_logged() -> bool:
            return bool(log_lines.of("lease_renewal.failed", "tandem.leases"))

        await until(failure_logged)
        proxy.restore()
        generation = hold.generation

    (first, *_) = log_lines.of("lease_renewal.failed", "tandem.leases")
    assert (first["level"], first["reason"]) == ("warning", "outage")
    assert first["error_type"] == StoreError.__name__
    assert "exception" not in first
    log_lines.assert_none_holds(entered)
    log_lines.assert_none_holds(generation)


async def test_a_failed_renewal_is_retried_after_every_even_when_every_exceeds_the_first_back_off(
    proxy: TcpProxy, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    every = timedelta(seconds=BACKOFF_FIRST_SECONDS * 1.6)
    leases = new_leases(make_client(proxy.url, retry=Retry(NoBackoff(), 0)), lasts=2 * every)
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
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    paused_pod = new_leases(make_client(proxy.url, retry=Retry(NoBackoff(), 0)), lasts=LASTS)
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
    url_denying: Callable[[str], Awaitable[str]],
    observer: Redis,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    key_prefix: str,
    log_lines: LogLines,
):
    leases = new_leases(make_client(await url_denying("flushall")), lasts=LASTS)

    async def hold_then_exit() -> None:
        async with leases.kept("pod-a", every=EVERY):
            if end_fails:
                await observer.acl_setuser(key_prefix, enabled=True, commands=["-evalsha"])

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
    url_denying: Callable[[str], Awaitable[str]],
    observer: Redis,
    make_client: Callable[..., Redis],
    key_prefix: str,
    log_lines: LogLines,
):
    leases, task = await _exiting_at_its_gate(
        make_client(await url_denying("flushall")), key_prefix
    )
    task.cancel()
    if end_fails:
        await observer.acl_setuser(key_prefix, enabled=True, commands=["-evalsha"])
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
