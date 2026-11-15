"""WakeChannel: tell the pods of one app that something changed, and when to re-read everything."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass

from logline import StepTimer
from redis import exceptions as redis_errors
from redis.asyncio.client import PubSub
from redis.asyncio.connection import AbstractConnection

from tandem.client import Client
from tandem.errors import (
    BACKOFF_FIRST_SECONDS,
    FailureReason,
    is_outage,
    next_backoff,
    store_error,
)
from tandem.keys import wake_channel

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Wake:
    address: str  # what changed, never its content


@dataclass(frozen=True)
class Resubscribed:
    """Redis acknowledged the subscription (again): re-read everything this pod serves."""


class WakeChannel:
    def __init__(self, redis: Client, *, key_prefix: str) -> None:
        self._redis = redis
        self._channel = wake_channel(key_prefix)

    async def publish(self, address: str) -> None:
        """Send one PUBLISH. A failure means the delivery is uncertain; it is not resent."""
        async with StepTimer("wake_publication.ended", logger):
            await self._publish(address)

    async def _publish(self, address: str) -> None:
        # The client's own execute_command would run the PUBLISH under the client's Retry, which can
        # send it again after a lost reply; the connection is driven directly instead.
        redis = self._redis
        try:
            await redis.initialize()
            if (own := redis.connection) is not None:
                # A single-connection client keeps the pool's connection and serializes its own
                # commands on this lock. redis-py offers no public way to take it without running
                # the command through its Retry, so this relies on the private attribute, as the
                # client's own execute_command does; a rename in redis-py fails loud below.
                try:
                    lock = redis._single_conn_lock
                except AttributeError as error:
                    raise RuntimeError(
                        "tandem publishes over a single-connection client through redis-py's "
                        "Redis._single_conn_lock, which this redis-py version does not have"
                    ) from error
                async with lock:
                    await self._send_publish(own, address)
            else:
                pool = redis.connection_pool
                connection = await pool.get_connection()
                try:
                    await self._send_publish(connection, address)
                finally:
                    await pool.release(connection)
        except redis_errors.RedisError as error:
            raise store_error(f"publish to {self._channel}", error) from error

    async def _send_publish(self, connection: AbstractConnection, address: str) -> None:
        await connection.send_command("PUBLISH", self._channel, address)
        await connection.read_response()

    async def received(self) -> AsyncIterator[Wake | Resubscribed]:
        """Yield every confirmation of the subscription and every wake after it, until cancelled.

        A payload that is no valid UTF-8 is logged and skipped.
        """
        backoff = BACKOFF_FIRST_SECONDS
        failed_attempts = 0
        while True:
            pubsub = self._redis.pubsub(ignore_subscribe_messages=False)
            try:
                await pubsub.subscribe(self._channel)
                while True:
                    event = await self._next_event(pubsub)
                    if isinstance(event, Resubscribed):
                        backoff = BACKOFF_FIRST_SECONDS
                        self._log_confirmed(failed_attempts)
                        failed_attempts = 0
                    yield event
            except UnicodeDecodeError as error:
                # A client that decodes responses fails inside get_message, which leaves the
                # connection unusable: the subscription is established again, with a Resubscribed.
                self._log_skipped_payload(error)
                continue
            except redis_errors.RedisError as error:
                if not is_outage(error):
                    raise store_error(f"subscribe to {self._channel}", error) from error
                failed_attempts += 1
                self._log_lost(error, failed_attempts)
            finally:
                await self._close(pubsub)
            await self._wait(backoff)
            backoff = next_backoff(backoff)

    async def _next_event(self, pubsub: PubSub) -> Wake | Resubscribed:
        while True:
            message = await pubsub.get_message(timeout=None)
            if message is None:  # the reply to redis-py's health check is no event
                continue
            try:
                return self._event(message)
            except UnicodeDecodeError as error:
                self._log_skipped_payload(error)

    async def _wait(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def _close(self, pubsub: PubSub) -> None:
        connection = pubsub.connection
        if connection is not None and connection.is_connected:
            with suppress(redis_errors.RedisError):
                await pubsub.unsubscribe(self._channel)
        await pubsub.aclose()

    def _log_skipped_payload(self, error: UnicodeDecodeError) -> None:
        # One payload no pod of this app published must not end the subscription for good.
        logger.warning(
            "wake_payload.skipped",
            extra={"reason": "invalid_utf8", "payload_bytes": len(error.object)},
        )

    def _log_lost(self, error: redis_errors.RedisError, failed_attempts: int) -> None:
        # One warning per outage; the attempts that follow are developer detail.
        level = logging.WARNING if failed_attempts == 1 else logging.DEBUG
        logger.log(
            level,
            "wake_subscription.lost",
            extra={
                "reason": FailureReason.OUTAGE,
                "error_type": type(error).__name__,
                "failed_attempt_count": failed_attempts,
            },
        )

    def _log_confirmed(self, failed_attempts: int) -> None:
        logger.info("wake_subscription.confirmed", extra={"failed_attempt_count": failed_attempts})

    def _event(self, message: dict[str, object]) -> Wake | Resubscribed:
        if message["type"] == "subscribe":
            return Resubscribed()
        return Wake(self._redis.get_encoder().decode(message["data"], force=True))
