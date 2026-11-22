"""Tools of the lock tests: a wiped app, a lock's expiry and the wait until a lock is acquirable."""

from redis.asyncio import Redis

from tandem import Locks
from tandem.keys import lock_keys
from tests.support.waiting import until

FAMILY = "gpu"


async def wipe(observer: Redis, key_prefix: str, family: str = FAMILY) -> None:
    """Delete the family's structures and the app's epoch: the state a Redis restart leaves."""
    keys = lock_keys(key_prefix, family)
    await observer.delete(keys.epoch, keys.expiry, keys.generation)


async def expiry_of(observer: Redis, key_prefix: str, name: str, family: str = FAMILY) -> float:
    """The expiry in ms of the lock on `name`, as Redis stores it."""
    score = await observer.zscore(lock_keys(key_prefix, family).expiry, name)
    assert score is not None
    return score


async def until_acquirable(locks: Locks, name: str) -> None:
    """Wait until `locks` acquires the lock on `name`, which it then holds."""

    async def acquired() -> bool:
        return await locks.acquire(name) is not None

    await until(acquired)
