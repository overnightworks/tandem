import asyncio
from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Liveness, Locks, LockState
from tandem.keys import lock_keys
from tests.support.deadlines import redis_ms, until_redis_ms
from tests.support.locks import FAMILY, expiry_of, wipe
from tests.support.waiting import until

LASTS = timedelta(milliseconds=300)


async def test_concurrent_acquires_of_an_absent_name_give_exactly_one_a_generation(
    redis_url: str, make_client: Callable[..., Redis], new_locks: Callable[..., Locks]
):
    pods = [new_locks(make_client(redis_url)) for _ in range(8)]

    results = await asyncio.gather(*(pod.acquire("gpu-0") for pod in pods))

    generations = [result for result in results if result is not None]
    assert len(generations) == 1
    assert len(generations[0]) >= 32


async def test_the_live_holder_acquiring_again_gets_none_and_its_expiry_stays(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
):
    locks = new_locks(make_client(redis_url))
    generation = await locks.acquire("gpu-0")
    expiry = await expiry_of(observer, key_prefix, "gpu-0")

    assert await locks.acquire("gpu-0") is None

    assert await expiry_of(observer, key_prefix, "gpu-0") == expiry
    assert (await locks.state("gpu-0")).generation == generation


async def test_extending_moves_the_expiry_only_for_the_current_generation(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
):
    client = make_client(redis_url)
    locks = new_locks(client)
    generation = await locks.acquire("gpu-0")
    assert generation is not None
    expiry = await expiry_of(observer, key_prefix, "gpu-0")
    await until_redis_ms(client, await redis_ms(client) + 20)

    assert await locks.extend("gpu-0", "not-the-generation") is False
    assert await expiry_of(observer, key_prefix, "gpu-0") == expiry
    assert await locks.extend("gpu-0", generation) is True

    assert await expiry_of(observer, key_prefix, "gpu-0") > expiry
    assert (await locks.state("gpu-0")).generation == generation


async def test_extending_after_the_expiry_returns_false_and_a_later_acquire_gets_a_new_generation(
    redis_url: str, make_client: Callable[..., Redis], new_locks: Callable[..., Locks]
):
    client = make_client(redis_url)
    locks = new_locks(client, lasts=LASTS)
    generation = await locks.acquire("gpu-0")
    assert generation is not None
    await until_redis_ms(client, await redis_ms(client) + 2 * 300)

    assert await locks.extend("gpu-0", generation) is False
    assert await locks.state("gpu-0") == LockState(Liveness.GONE, None, None)
    later = await locks.acquire("gpu-0")

    assert later is not None
    assert later != generation


async def test_releasing_the_current_generation_frees_the_name_and_a_stale_one_keeps_the_lock(
    redis_url: str, make_client: Callable[..., Redis], new_locks: Callable[..., Locks]
):
    locks = new_locks(make_client(redis_url))
    first = await locks.acquire("gpu-0")
    assert first is not None

    assert await locks.release("gpu-0", first) is True
    second = await locks.acquire("gpu-0")
    assert second is not None
    assert second != first
    assert await locks.release("gpu-0", first) is False

    assert (await locks.state("gpu-0")).generation == second


async def test_state_reads_alive_with_the_generation_and_the_time_left_and_gone_when_free(
    redis_url: str, make_client: Callable[..., Redis], new_locks: Callable[..., Locks]
):
    locks = new_locks(make_client(redis_url), lasts=timedelta(seconds=30))
    assert await locks.state("gpu-0") == LockState(Liveness.GONE, None, None)

    generation = await locks.acquire("gpu-0")
    held = await locks.state("gpu-0")

    assert held.liveness is Liveness.ALIVE
    assert held.generation == generation
    assert held.remaining is not None
    assert timedelta(0) < held.remaining <= locks.lasts


async def test_state_reads_unknown_right_after_a_wipe_and_gone_once_lasts_have_passed(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
):
    client = make_client(redis_url)
    locks = new_locks(client, lasts=LASTS)
    await wipe(observer, key_prefix)

    read_at = await redis_ms(client)
    first = await locks.state("gpu-0")

    async def settled() -> bool:
        return (await locks.state("gpu-0")).liveness is Liveness.GONE

    assert first == LockState(Liveness.UNKNOWN, None, None)
    await until(settled)
    assert await redis_ms(client) >= read_at + 300


async def test_later_acquires_shrink_the_structures_of_names_that_expired_unacquired(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
):
    client = make_client(redis_url)
    locks = new_locks(client, lasts=LASTS)
    keys = lock_keys(key_prefix, FAMILY)
    names = [f"download-{number}" for number in range(100)]
    await asyncio.gather(*(locks.acquire(name) for name in names))
    assert await observer.zcard(keys.expiry) == 100
    await until_redis_ms(client, await redis_ms(client) + 2 * 300)

    for number in range(3):
        await locks.acquire(f"later-{number}")

    remaining = await observer.zcard(keys.expiry)
    assert remaining < 100
    assert await observer.hlen(keys.generation) == remaining
