"""WakeChannel: tell the pods of one app that something changed, and when to re-read everything."""

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass

from logline import StepTimer, current_traceparent, unit_trace
from redis import exceptions as redis_errors
from redis.asyncio.client import PubSub
from redis.asyncio.connection import AbstractConnection

from tandem.client import Client
from tandem.errors import (
    BACKOFF_FIRST_SECONDS,
    FailureReason,
    StoreError,
    is_outage,
    next_backoff,
    store_error,
)
from tandem.keys import wake_channel

logger: logging.Logger = logging.getLogger(__name__)

# A payload is the decimal byte length of the traceparent, a colon, the traceparent and the address.
_TRACEPARENT_LENGTH = re.compile(rb"(\d{1,9}):")


@dataclass(frozen=True)
class Wake:
    address: str  # what changed, never its content
    traceparent: str | None = None  # the sender's trace as it sent it; None when it had none


@dataclass(frozen=True)
class _Resubscribed:
    """Redis acknowledged the subscription (again): re-read everything this pod serves."""


class WakeChannel:
    def __init__(self, redis: Client, *, key_prefix: str) -> None:
        self._redis = redis
        self._channel = wake_channel(key_prefix)

    async def publish(self, address: str) -> None:
        """Send one PUBLISH of `address` with the caller's current trace.

        A failure means the delivery is uncertain; it is not resent.
        """
        async with StepTimer("wake_publication.ended", logger):
            await self._publish(address, current_traceparent())

    async def _publish(self, address: str, traceparent: str | None) -> None:
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
                    await self._send_publish(own, address, traceparent)
            else:
                pool = redis.connection_pool
                connection = await pool.get_connection()
                try:
                    await self._send_publish(connection, address, traceparent)
                finally:
                    await pool.release(connection)
        except redis_errors.RedisError as error:
            raise store_error(f"publish to {self._channel}", error) from error

    async def _send_publish(
        self, connection: AbstractConnection, address: str, traceparent: str | None
    ) -> None:
        trace = connection.encoder.encode(traceparent or "")
        payload = b"%d:%b%b" % (len(trace), trace, connection.encoder.encode(address))
        await connection.send_command("PUBLISH", self._channel, payload)
        await connection.read_response()

    @asynccontextmanager
    async def following(
        self,
        *,
        on_resync: Callable[[], None],
        on_wake: Callable[[Wake], None],
        on_failed: Callable[[Exception], None],
        ready_within: float,
    ) -> AsyncIterator[None]:
        """Follow the channel for the host while the context is open.

        Entering returns after Redis first confirmed the subscription and `on_resync` returned;
        with no confirmation within `ready_within` seconds it raises `StoreError(outage=True)`.
        `on_resync()` runs at every confirmation, the first and after every reconnect: re-read
        everything this pod serves. `on_wake(wake)` runs for every wake, inside the trace of its
        sender. The handlers are synchronous and run one at a time in arrival order.

        A refusal or a handler exception before entry returns is raised out of the entry. After it,
        either ends the following: one `wake_follow.failed` line carries the error type and
        `on_failed(error)` is called once. Leaving the context cancels the following, closes the
        subscription and raises nothing of its own.

        A payload that is no valid UTF-8 or no frame is logged and skipped.
        """
        ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        follower = asyncio.create_task(self._follow(ready, on_resync, on_wake, on_failed))
        try:
            confirmed, _ = await asyncio.wait({ready}, timeout=ready_within)
            if not confirmed:
                raise StoreError(
                    f"subscribe to {self._channel} was not confirmed within {ready_within} s",
                    outage=True,
                )
            ready.result()  # raises what stopped the following before it was ready
            yield
        finally:
            follower.cancel()
            with suppress(asyncio.CancelledError):
                await follower

    async def _follow(
        self,
        ready: asyncio.Future[None],
        on_resync: Callable[[], None],
        on_wake: Callable[[Wake], None],
        on_failed: Callable[[Exception], None],
    ) -> None:
        try:
            async for event in self._received():
                if isinstance(event, _Resubscribed):
                    on_resync()
                    if not ready.done():
                        ready.set_result(None)
                else:
                    with unit_trace(event.traceparent):
                        on_wake(event)
        except Exception as error:
            if not ready.done():
                ready.set_exception(error)
                return
            logger.error("wake_follow.failed", extra={"error_type": type(error).__name__})
            on_failed(error)

    async def _received(self) -> AsyncIterator[Wake | _Resubscribed]:
        """Yield every confirmation of the subscription and every wake after it, until cancelled."""
        backoff = BACKOFF_FIRST_SECONDS
        failed_attempts = 0
        while True:
            pubsub = self._redis.pubsub(ignore_subscribe_messages=False)
            try:
                await pubsub.subscribe(self._channel)
                while True:
                    event = await self._next_event(pubsub)
                    if isinstance(event, _Resubscribed):
                        backoff = BACKOFF_FIRST_SECONDS
                        self._log_confirmed(failed_attempts)
                        failed_attempts = 0
                    yield event
            except UnicodeDecodeError as error:
                # A client that decodes responses fails inside get_message, which leaves the
                # connection unusable: the subscription is established again, with a Resubscribed.
                self._log_skipped_payload("invalid_utf8", len(error.object))
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

    async def _next_event(self, pubsub: PubSub) -> Wake | _Resubscribed:
        while True:
            message = await pubsub.get_message(timeout=None)
            if message is None:  # the reply to redis-py's health check is no event
                continue
            try:
                event = self._event(message)
            except UnicodeDecodeError as error:
                self._log_skipped_payload("invalid_utf8", len(error.object))
                continue
            if event is not None:
                return event

    async def _wait(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def _close(self, pubsub: PubSub) -> None:
        connection = pubsub.connection
        if connection is not None and connection.is_connected:
            with suppress(redis_errors.RedisError):
                await pubsub.unsubscribe(self._channel)
        await pubsub.aclose()

    def _log_skipped_payload(self, reason: str, payload_bytes: int) -> None:
        # One payload no pod of this app published must not end the subscription for good.
        logger.warning(
            "wake_payload.skipped", extra={"reason": reason, "payload_bytes": payload_bytes}
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

    def _event(self, message: dict[str, object]) -> Wake | _Resubscribed | None:
        """The event of a message; None, after its log line, for a payload that is no frame."""
        if message["type"] == "subscribe":
            return _Resubscribed()
        encoder = self._redis.get_encoder()
        payload = encoder.encode(message["data"])
        encoder.decode(payload, force=True)  # raises UnicodeDecodeError for an undecodable payload
        wake = self._unframe(payload)
        if wake is None:
            self._log_skipped_payload("malformed_frame", len(payload))
        return wake

    def _unframe(self, payload: bytes) -> Wake | None:
        head = _TRACEPARENT_LENGTH.match(payload)
        if head is None:
            return None
        trace_end = head.end() + int(head[1])
        if trace_end > len(payload):
            return None
        encoder = self._redis.get_encoder()
        try:
            traceparent = encoder.decode(payload[head.end() : trace_end], force=True)
            address = encoder.decode(payload[trace_end:], force=True)
        except UnicodeDecodeError:  # the length cuts a character in two
            return None
        return Wake(address, traceparent or None)
