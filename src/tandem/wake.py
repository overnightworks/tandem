"""WakeChannel: tell the pods of one app that something changed, and when to re-read everything."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from redis import exceptions as redis_errors
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from tandem.errors import StoreError
from tandem.keys import wake_channel

BACKOFF_FIRST_SECONDS = 0.5
BACKOFF_CAP_SECONDS = 5.0


@dataclass(frozen=True)
class Wake:
    address: str  # what changed, never its content


@dataclass(frozen=True)
class Resubscribed:
    """Redis acknowledged the subscription (again): re-read everything this pod serves."""


class WakeChannel:
    def __init__(self, redis: Redis, *, key_prefix: str) -> None:
        self._redis = redis
        self._channel = wake_channel(key_prefix)

    async def publish(self, address: str) -> None:
        """Send one PUBLISH. A failure means the delivery is uncertain; nothing is retried."""
        try:
            await self._redis.publish(self._channel, address)
        except redis_errors.RedisError as error:
            raise StoreError(f"publish to {self._channel} failed: {error}") from error

    async def received(self) -> AsyncIterator[Wake | Resubscribed]:
        """Yield every confirmation of the subscription and every wake after it, until cancelled."""
        backoff = BACKOFF_FIRST_SECONDS
        while True:
            pubsub = self._redis.pubsub(ignore_subscribe_messages=False)
            try:
                await pubsub.subscribe(self._channel)
                while True:
                    message = await pubsub.get_message(timeout=None)
                    if message is None:  # the reply to redis-py's health check is no event
                        continue
                    event = _event(message)
                    if isinstance(event, Resubscribed):
                        backoff = BACKOFF_FIRST_SECONDS
                    yield event
            except redis_errors.ConnectionError, redis_errors.TimeoutError:
                pass
            except redis_errors.RedisError as error:
                raise StoreError(f"subscribe to {self._channel} failed: {error}") from error
            finally:
                await self._close(pubsub)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_CAP_SECONDS)

    async def _close(self, pubsub: PubSub) -> None:
        connection = pubsub.connection
        if connection is not None and connection.is_connected:
            with suppress(redis_errors.RedisError):
                await pubsub.unsubscribe(self._channel)
        await pubsub.aclose()


def _event(message: dict[str, Any]) -> Wake | Resubscribed:
    if message["type"] == "subscribe":
        return Resubscribed()
    data = message["data"]
    return Wake(data.decode() if isinstance(data, bytes) else data)
