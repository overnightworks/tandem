"""A minimal host: its truth is a Redis hash that it re-reads at every Resubscribed."""

from redis.asyncio import Redis

from tandem import Resubscribed, Wake, WakeChannel


class TestHost:
    __test__ = False

    def __init__(self, redis: Redis, channel: WakeChannel, key_prefix: str) -> None:
        self._redis = redis
        self._channel = channel
        self._truth_key = f"{key_prefix}:truth"
        self.served: dict[str, str] = {}

    async def commit(self, address: str, value: str) -> None:
        await self._redis.hset(self._truth_key, address, value)

    async def reread(self) -> None:
        stored = await self._redis.hgetall(self._truth_key)
        self.served = {_text(key): _text(value) for key, value in stored.items()}

    async def handle(self, event: Wake | Resubscribed) -> None:
        if isinstance(event, Resubscribed):
            await self.reread()
        else:
            self.served[event.address] = _text(
                await self._redis.hget(self._truth_key, event.address)
            )


def _text(value: bytes | str | None) -> str:
    assert value is not None
    return value.decode() if isinstance(value, bytes) else value
