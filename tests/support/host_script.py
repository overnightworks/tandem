"""A host's script that runs beside tandem's: one family's three keys, then the host's own key."""

from redis.asyncio import Redis

from tandem import Leases, Locks, Script
from tandem.deadlines import milliseconds


class HostScript:
    """Runs `source` with the family's `epoch`, `expiry` and `generation` as KEYS[1..3], the host's
    own key as KEYS[4], the family's `lasts` in ms as ARGV[1] and the call's arguments after it."""

    def __init__(self, client: Redis, source: str, family: Leases | Locks, own_key: str) -> None:
        self._script = Script(client, source)
        self._family = family
        self._own_key = own_key

    async def run(self, *arguments: str) -> object:
        keys = self._family.keys
        return await self._script.run(
            [keys.epoch, keys.expiry, keys.generation, self._own_key],
            [milliseconds(self._family.lasts), *arguments],
        )
