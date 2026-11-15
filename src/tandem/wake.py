"""WakeChannel: tell the pods of one app that something changed, and when to re-read everything."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass

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
        # The client's own execute_command would run the PUBLISH under the client's Retry, which can
        # send it again after a lost reply; the pooled connection is driven directly instead.
        pool = self._redis.connection_pool
        try:
            connection = await pool.get_connection()
            try:
                await connection.send_command("PUBLISH", self._channel, address)
                await connection.read_response()
            finally:
                await pool.release(connection)
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
                    event = self._event(message)
                    if isinstance(event, Resubscribed):
                        backoff = BACKOFF_FIRST_SECONDS
                    yield event
            except (redis_errors.AuthenticationError, redis_errors.AuthorizationError) as error:
                raise StoreError(f"subscribe to {self._channel} refused: {error}") from error
            except redis_errors.ConnectionError, redis_errors.TimeoutError:
                pass
            except redis_errors.RedisError as error:
                raise StoreError(f"subscribe to {self._channel} failed: {error}") from error
            finally:
                await self._close(pubsub)
            await self._wait(backoff)
            backoff = min(backoff * 2, BACKOFF_CAP_SECONDS)

    async def _wait(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def _close(self, pubsub: PubSub) -> None:
        connection = pubsub.connection
        if connection is not None and connection.is_connected:
            with suppress(redis_errors.RedisError):
                await pubsub.unsubscribe(self._channel)
        await pubsub.aclose()

    def _event(self, message: dict[str, object]) -> Wake | Resubscribed:
        if message["type"] == "subscribe":
            return Resubscribed()
        return Wake(self._redis.get_encoder().decode(message["data"], force=True))
