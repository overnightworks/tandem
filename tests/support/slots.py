"""Tools of the slot tests: the shared family, a read of the keys left and an expired hold."""

from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Slots
from tandem.keys import slot_key
from tests.support.deadlines import until_redis_ms

FAMILY = "streams"
EXPIRING_HOLD_LASTS = timedelta(seconds=1)


async def keys_of(observer: Redis, key_prefix: str) -> set[str]:
    """Every key of the app of `key_prefix` on the server."""
    return {key async for key in observer.scan_iter(match=f"{key_prefix}:*")}


async def take_hold_left_expired_beside_a_live_one(
    new_slots: Callable[..., Slots], client: Redis, observer: Redis, key_prefix: str
) -> str:
    """Take a short hold, then a longer one, and wait past the first expiry.

    The second take prunes every hold already expired, so the first hold, which lasts
    `EXPIRING_HOLD_LASTS`, survives only if the second take starts within that time. The second
    hold then keeps the family key alive, and nothing prunes the first hold before the caller
    acts. Returns the first hold's token.
    """
    family = slot_key(key_prefix, FAMILY)
    expired = await new_slots(client, lasts=EXPIRING_HOLD_LASTS).take({"person-a": 1})
    assert expired is not None
    assert await new_slots(client).take({"person-b": 1}) is not None
    expiry = await observer.zscore(family, expired)
    assert expiry is not None
    await until_redis_ms(client, int(expiry) + 1)
    return expired
