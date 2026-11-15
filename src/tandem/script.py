"""Script: run a caller's Lua next to `LUA_KIT`, with Redis failures reported as StoreError."""

from redis import exceptions as redis_errors

from tandem.client import Client
from tandem.errors import store_error
from tandem.lua import LUA_KIT


class Script:
    def __init__(self, redis: Client, source: str) -> None:
        """Prepare `source`, which runs after `LUA_KIT`, on the host's client."""
        self._script = redis.register_script(LUA_KIT + source)

    async def run(self, keys: list[str], args: list[str | int | bytes]) -> object:
        """Run the script by its SHA (loading it on NOSCRIPT) and return Redis's raw reply.

        The host client's own `Retry` governs a lost connection and can run the script again after a
        lost reply; tandem adds no retry. Every Redis failure raises `StoreError`.
        """
        try:
            return await self._script(keys=keys, args=args)
        except redis_errors.RedisError as error:
            raise store_error("running a script", error) from error
