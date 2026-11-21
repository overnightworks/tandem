from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Liveness, Locks, LockState
from tests.support.deadlines import redis_ms
from tests.support.restart import restart
from tests.support.waiting import until

LASTS_MS = 600
LASTS = timedelta(milliseconds=LASTS_MS)


async def test_after_a_redis_restart_acquire_waits_for_lasts_and_the_old_generation_is_lost(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
):
    """Spec: LOCK-4."""
    client = make_client(restartable_redis_url)
    locks = new_locks(client, lasts=LASTS)

    async def settled() -> bool:
        return (await locks.state("probe")).liveness is Liveness.GONE

    await until(settled)
    generation = await locks.acquire("gpu-0")
    assert generation is not None

    await restart(client)
    read_at = await redis_ms(client)
    assert await locks.acquire("gpu-0") is None
    assert await locks.extend("gpu-0", generation) is False
    assert await locks.state("gpu-0") == LockState(Liveness.UNKNOWN, None, None)

    async def given() -> bool:
        return await locks.acquire("gpu-0") is not None

    await until(given)
    assert await redis_ms(client) >= read_at + LASTS_MS
    assert (await locks.state("gpu-0")).generation != generation
