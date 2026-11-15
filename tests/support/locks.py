"""Tools of the lock tests: the family the helpers share, a wiped app and a lock's expiry."""

from redis.asyncio import Redis

from tandem.keys import lock_keys

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
