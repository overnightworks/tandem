"""Helpers for a host's tests; production code must not call them."""

from redis.asyncio import Redis

from tandem.keys import epoch_key

__all__ = ["settle_epoch"]


async def settle_epoch(redis: Redis, *, key_prefix: str) -> None:
    """Set the epoch of the app that owns `key_prefix` so far in the past that every lease and
    lock family reads settled at once.

    The epoch is the app's, shared by all families. Production code that calls this ends the
    restart grace: after a Redis restart a lease or lock that Redis lost then reads GONE or free
    at once, where it should read UNKNOWN until `lasts` have passed.
    """
    await redis.set(epoch_key(key_prefix), 0)
