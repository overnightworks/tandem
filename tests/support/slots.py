"""Tools of the slot tests: the shared family, a read of the keys left and an expired hold."""

from datetime import timedelta

from redis.asyncio import Redis

from tandem import Slots
from tandem.keys import slot_key
from tests.support.deadlines import until_redis_ms

FAMILY = "streams"


async def keys_of(observer: Redis, key_prefix: str) -> set[str]:
    """Every key of the app of `key_prefix` on the server."""
    return {key async for key in observer.scan_iter(match=f"{key_prefix}:*")}


async def take_hold_left_expired_beside_a_live_one(
    slots: Slots, client: Redis, observer: Redis, key_prefix: str, *, lasts: timedelta
) -> tuple[str, int]:
    """Take a hold, take a second half a `lasts` before the first expires, wait past that expiry.

    The second take keeps the family key alive past the first hold's expiry, and nothing prunes the
    first hold before the caller acts. Returns the first hold's token and its stored expiry in ms,
    read before the second take.
    """
    expired = await slots.take({"person-a": 1})
    assert expired is not None
    expiry = await observer.zscore(slot_key(key_prefix, FAMILY), expired)
    assert expiry is not None
    expiry_ms = int(expiry)
    await until_redis_ms(client, expiry_ms - int(lasts.total_seconds() * 1000) // 2)
    assert await slots.take({"person-b": 1}) is not None
    await until_redis_ms(client, expiry_ms + 1)
    return expired, expiry_ms
