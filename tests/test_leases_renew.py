import asyncio
from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Leases, Liveness, Renewal
from tandem.keys import lease_keys
from tests.support.deadlines import redis_ms, until_redis_ms
from tests.support.leases import Handled, liveness_of, serving, until_gone
from tests.support.waiting import until

LASTS = timedelta(milliseconds=300)
ENDING_LASTS = timedelta(seconds=2)


async def test_concurrent_renewals_of_one_absent_holder_start_exactly_one_lease(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    pods = [new_leases(make_client(redis_url)) for _ in range(8)]

    renewals = await asyncio.gather(*(pod.renew("pod-a") for pod in pods))

    assert sum(renewal.started for renewal in renewals) == 1
    assert len({renewal.generation for renewal in renewals}) == 1
    assert len(renewals[0].generation) >= 32


async def test_a_renewal_of_a_live_lease_keeps_its_generation_and_reads_alive(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    leases = new_leases(make_client(redis_url))

    first = await leases.renew("pod-a")
    second = await leases.renew("pod-a")

    assert first.started is True
    assert second == Renewal(started=False, generation=first.generation)
    assert await liveness_of(leases, "pod-a") is Liveness.ALIVE


async def test_the_state_of_no_holder_is_empty_without_a_round_trip(
    make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    nowhere = make_client("redis://127.0.0.1:1")

    assert await new_leases(nowhere).state([]) == {}


async def test_a_lapse_that_serve_lapses_claimed_reads_gone_and_a_renewal_starts_a_new_generation(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS)
    old = await leases.renew("pod-a")
    handled = Handled(hold=True)

    async with serving(leases, handled):
        (lapse,) = await handled.received()
        assert await liveness_of(leases, "pod-a") is Liveness.GONE
        renewal = await leases.renew("pod-a")
        assert await liveness_of(leases, "pod-a") is Liveness.ALIVE
        handled.release()

    assert lapse.generation == old.generation
    assert lapse.holder == "pod-a"
    assert renewal.started is True
    assert renewal.generation != old.generation


async def test_ending_the_current_generation_removes_the_lease_and_a_stale_one_changes_nothing(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    leases = new_leases(make_client(redis_url), lasts=ENDING_LASTS)
    first = await leases.renew("pod-a")
    handled = Handled()

    assert await leases.end("pod-a", first.generation) is True
    await until_gone(leases, "pod-a")
    second = await leases.renew("pod-a")
    assert second.started is True
    assert await leases.end("pod-a", first.generation) is False

    assert await liveness_of(leases, "pod-a") is Liveness.ALIVE
    assert await leases.renew("pod-a") == Renewal(started=False, generation=second.generation)
    async with serving(leases, handled):
        (lapse,) = await handled.received()
    assert (lapse.holder, lapse.generation) == ("pod-a", first.generation)


async def test_an_expired_lease_reaches_a_handler_once_and_a_renewal_after_it_starts_again(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS)
    old = await leases.renew("pod-a")
    await until_redis_ms(client, await redis_ms(client) + 300)

    async def lapsed() -> bool:
        return await liveness_of(leases, "pod-a") is Liveness.GONE

    await until(lapsed)
    renewal = await leases.renew("pod-a")
    handled = Handled()
    async with serving(leases, handled):
        (lapse,) = await handled.received()

    assert renewal.started is True
    assert renewal.generation != old.generation
    assert lapse.generation == old.generation


async def test_end_drops_the_holder_from_the_generation_record(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    leases = new_leases(make_client(redis_url), lasts=timedelta(seconds=30))
    generations = lease_keys(key_prefix, "owner").generation
    ended = await leases.renew("pod-ended")
    await leases.renew("pod-kept")

    assert await leases.end("pod-ended", ended.generation) is True

    assert not await observer.hexists(generations, "pod-ended")
    assert await observer.hexists(generations, "pod-kept")


async def test_the_sweep_drops_a_lapsed_holder_from_the_generation_record(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    leases = new_leases(make_client(redis_url), lasts=LASTS)
    generations = lease_keys(key_prefix, "owner").generation
    await leases.renew("pod-lapsed")
    await until_gone(leases, "pod-lapsed")

    async def swept() -> bool:
        return await observer.hlen(generations) == 0

    async with serving(leases, Handled()):
        await until(swept)
