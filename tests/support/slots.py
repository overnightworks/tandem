"""Tools of the slot tests: the shared family, a read of the keys left and an expired hold."""

from redis.asyncio import Redis

from tandem import Slots
from tandem.keys import slot_key
from tests.support.deadlines import until_redis_ms

FAMILY = "streams"


async def keys_of(observer: Redis, key_prefix: str) -> set[str]:
    """Every key of the app of `key_prefix` on the server."""
    return {key async for key in observer.scan_iter(match=f"{key_prefix}:*")}


async def take_hold_left_expired_beside_a_live_one(
    short_lived: Slots, long_lived: Slots, client: Redis, observer: Redis, key_prefix: str
) -> str:
    """Take a hold on `short_lived`, a second on `long_lived` at once, wait past the first expiry.

    Both are `Slots` of one family. The second hold outlives the first whatever stalls between the
    two takes, so it keeps the family key alive and nothing prunes the first hold before the caller
    acts. Returns the first hold's token.
    """
    family = slot_key(key_prefix, FAMILY)
    expired = await short_lived.take({"person-a": 1})
    assert expired is not None
    assert await long_lived.take({"person-b": 1}) is not None
    expiry = await observer.zscore(family, expired)
    assert expiry is not None
    await until_redis_ms(client, int(expiry) + 1)
    return expired
