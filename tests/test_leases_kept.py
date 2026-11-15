import asyncio
import logging
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
from tests.support.leases import until_gone
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

    async def renew(self, holder: str) -> Renewal:
        self._calls += 1
        if self._calls > 1:
            self.entered.set()
            await self.gate.wait()
        return await super().renew(holder)


class _FailingRenewals(Leases):
    """Leases whose renewals after the first fail as an outage, each noted at its loop time."""

    def __init__(self, *args: Any, **keywords: Any) -> None:
        super().__init__(*args, **keywords)
        self.failed_at: list[float] = []
        self._calls = 0

    async def renew(self, holder: str) -> Renewal:
        self._calls += 1
        if self._calls == 1:
            return await super().renew(holder)
        self.failed_at.append(asyncio.get_running_loop().time())
        raise StoreError("the renewal failed", outage=True)


class _GatedEnd(Leases):
    """Leases whose `end` waits at a gate the test opens, then ends or, when told, fails."""

    def __init__(self, *args: Any, **keywords: Any) -> None:
        super().__init__(*args, **keywords)
        self.ending = asyncio.Event()
        self.gate = asyncio.Event()
        self.ended = False
        self.fails = False

    async def end(self, holder: str, generation: str) -> bool:
        self.ending.set()
        await self.gate.wait()
        if self.fails:
            raise StoreError("the end failed")
        ended = await super().end(holder, generation)
        self.ended = True
        return ended


async def test_kept_renews_before_its_body_runs_and_ends_the_lease_at_exit(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    leases = new_leases(make_client(redis_url), lasts=LASTS)

    async with leases.kept("pod-a", every=EVERY) as hold:
        assert await leases.liveness(["pod-a"]) == {"pod-a": Liveness.ALIVE}
        assert await leases.renew("pod-a") == Renewal(started=False, generation=hold.generation)

    await until_gone(leases, "pod-a")


async def test_kept_renews_while_its_body_runs(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS)

    async with leases.kept("pod-a", every=EVERY):
        two_lasts_ms = 2 * LASTS // timedelta(milliseconds=1)
        await until_redis_ms(client, await redis_ms(client) + two_lasts_ms)
        assert await leases.liveness(["pod-a"]) == {"pod-a": Liveness.ALIVE}


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
    caplog: pytest.LogCaptureFixture,
):
    holder_pod = new_leases(make_client(await url_denying("flushall")), lasts=LASTS)
    observer_pod = new_leases(make_client(redis_url), lasts=LASTS)

    async def alive() -> bool:
        return (await observer_pod.liveness(["pod-a"]))["pod-a"] is Liveness.ALIVE

    with caplog.at_level(logging.WARNING, logger="tandem.leases"):
        async with holder_pod.kept("pod-a", every=EVERY):
            await observer.acl_setuser(key_prefix, commands=["-evalsha"])
            await until_gone(observer_pod, "pod-a")
            await observer.acl_setuser(key_prefix, commands=["+evalsha"])
            await until(alive)

    levels = {record.levelno for record in caplog.records if record.name == "tandem.leases"}
    assert levels == {logging.ERROR}


async def test_a_failed_renewal_is_retried_after_every_even_when_every_exceeds_the_first_back_off(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    every = timedelta(seconds=BACKOFF_FIRST_SECONDS * 1.6)
    leases = _FailingRenewals(
        make_client(redis_url),
        key_prefix=key_prefix,
        family="owner",
        lasts=2 * every,
        claim_for=timedelta(seconds=30),
    )

    async def failed_twice() -> bool:
        return len(leases.failed_at) >= 2

    async with leases.kept("pod-a", every=every):
        await until(failed_twice)

    first, second = leases.failed_at[:2]
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
            return (await observer_pod.liveness(["pod-a"]))["pod-a"] is Liveness.GONE

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
    assert await leases.liveness(["pod-a"]) == {"pod-a": Liveness.GONE}


@pytest.mark.parametrize("end_fails", [False, True])
async def test_cancelling_the_exit_still_ends_the_lease_before_the_cancellation_propagates(
    end_fails: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    key_prefix: str,
    caplog: pytest.LogCaptureFixture,
):
    leases = _GatedEnd(
        make_client(redis_url),
        key_prefix=key_prefix,
        family="owner",
        lasts=LASTS,
        claim_for=timedelta(seconds=30),
    )
    leases.fails = end_fails

    inside = asyncio.Event()

    async def body() -> None:
        async with leases.kept("pod-a", every=EVERY):
            inside.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(body())
    await within(inside.wait())
    task.cancel()
    await within(leases.ending.wait())
    task.cancel()
    with caplog.at_level(logging.ERROR, logger="tandem.leases"):
        leases.gate.set()
        await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert leases.ended is not end_fails
    logged = [record for record in caplog.records if record.name == "tandem.leases"]
    assert bool(logged) is end_fails


async def test_cancelling_the_exit_and_its_cleanup_together_ends_the_task_cancelled_unlogged(
    redis_url: str,
    make_client: Callable[..., Redis],
    key_prefix: str,
    caplog: pytest.LogCaptureFixture,
):
    leases = _GatedEnd(
        make_client(redis_url),
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

    earlier = asyncio.all_tasks()
    task = asyncio.create_task(body())
    await within(inside.wait())
    task.cancel()
    await within(leases.ending.wait())
    with caplog.at_level(logging.DEBUG, logger="tandem.leases"):
        for started_since in asyncio.all_tasks() - earlier:
            started_since.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert not [record for record in caplog.records if record.name == "tandem.leases"]
