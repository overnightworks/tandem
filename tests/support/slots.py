"""Tools of the slot tests: the family the helpers share and a read of the keys a test left."""

from redis.asyncio import Redis

FAMILY = "streams"


async def keys_of(observer: Redis, key_prefix: str) -> set[str]:
    """Every key of the app of `key_prefix` on the server."""
    return {key async for key in observer.scan_iter(match=f"{key_prefix}:*")}
