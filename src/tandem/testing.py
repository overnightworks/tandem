"""Helpers for a host's tests; production code must not call them."""

import asyncio
import time
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import NamedTuple

from logline import current_traceparent
from redis.asyncio import Redis

from tandem.deadlines import checked_span, milliseconds
from tandem.errors import StoreError
from tandem.keys import epoch_key
from tandem.wake import Wake, _Following, _Resubscribed

__all__ = [
    "EpochSet",
    "FakeWakeBus",
    "FakeWakeChannel",
    "PublishHold",
    "aged_epoch",
    "settle_epoch",
]

# Redis reads this integer back as the smallest signed 64-bit value, so elapsed time since the epoch
# exceeds any `lasts` a timedelta can express.
_EPOCH_FAR_PAST = -(2**63)
_NANOSECONDS_PER_MILLISECOND = 1_000_000


class EpochSet(NamedTuple):
    """The key and value of an app's epoch, which unpack into `redis.set(*epoch)` on any client."""

    key: str
    value: int


def aged_epoch(*, key_prefix: str, ran_for: timedelta | None = None) -> EpochSet:
    """The epoch of the app that owns `key_prefix`, dated for a `SET` on any client: sync, async
    or a fakeredis server.

    With `ran_for`, a positive `timedelta`, the app's Redis counts as having run for `ran_for`
    on this machine's clock: a lease or lock family whose `lasts` is at most `ran_for` reads
    settled, a longer one still reads UNKNOWN. That clock matches Redis `TIME` only where Redis
    runs on this machine (a container on it, or fakeredis). With `None` every family reads
    settled at once, as after `settle_epoch`.

    The epoch is the app's, shared by all families. Production code that sets it ends the
    restart grace: after a Redis restart a lease or lock that Redis lost then reads GONE or free
    at once, where it should read UNKNOWN until `lasts` have passed.
    """
    if ran_for is None:
        return EpochSet(epoch_key(key_prefix), _EPOCH_FAR_PAST)
    ran_for_ms = milliseconds(checked_span(ran_for, argument="ran_for"))
    return EpochSet(
        epoch_key(key_prefix), time.time_ns() // _NANOSECONDS_PER_MILLISECOND - ran_for_ms
    )


async def settle_epoch(redis: Redis, *, key_prefix: str) -> None:
    """Set the epoch of the app that owns `key_prefix` so far in the past that every lease and
    lock family reads settled at once.

    The epoch is the app's, shared by all families. Production code that calls this ends the
    restart grace: after a Redis restart a lease or lock that Redis lost then reads GONE or free
    at once, where it should read UNKNOWN until `lasts` have passed.
    """
    await redis.set(*aged_epoch(key_prefix=key_prefix))


class FakeWakeBus:
    """The pods of one app in one test: what any pod publishes reaches every pod that follows."""

    def __init__(self) -> None:
        self._pods: list[FakeWakeChannel] = []

    def pod(self) -> FakeWakeChannel:
        """The channel of one more pod of the app."""
        return FakeWakeChannel(self)

    def _broadcast(self, wake: Wake) -> None:
        for pod in self._pods:
            pod._deliver(wake)


class PublishHold:
    """The handle `FakeWakeChannel.hold_publishes` returns; publishes made while it holds wait."""

    def __init__(self) -> None:
        self._released = asyncio.Event()

    def release(self) -> None:
        """Let the held publishes finish, in the order they were made, and stop holding."""
        self._released.set()

    async def _wait(self) -> None:
        await self._released.wait()


class FakeWakeChannel(_Following):
    """A `WakeChannel` of one pod of a `FakeWakeBus`, without Redis.

    `publish` and `following` behave as the real channel's do: `following` is the same code, and
    its handlers see the same confirmation, wakes, trace and failures. The publisher's own pod
    receives its wake too, as on Redis. The methods below are the controls a test adds, the one
    way to make the fake lose what Redis would make it lose.
    """

    def __init__(self, bus: FakeWakeBus) -> None:
        super().__init__("the fake wake channel")
        self._bus = bus
        bus._pods.append(self)
        self._subscriptions: set[asyncio.Queue[Wake | _Resubscribed | StoreError]] = set()
        self._withholding_confirmation = False
        self._refusal: StoreError | None = None
        self._publish_failures: list[StoreError] = []
        self._publish_hold: PublishHold | None = None

    async def publish(self, address: str) -> None:
        """Deliver `address`, with the caller's current trace, to every pod that follows."""
        if self._publish_failures:
            raise self._publish_failures.pop(0)
        if self._publish_hold is not None:
            await self._publish_hold._wait()
        self._bus._broadcast(Wake(address, current_traceparent()))
        # publish does not guarantee handler completion when it returns; it only yields.
        await asyncio.sleep(0)

    def resync(self) -> None:
        """Confirm the subscription of every following of this pod again, as a reconnect does.

        A pod that is not followed has no connection to reconnect: its withholding stays.
        """
        if self._subscriptions:
            self._withholding_confirmation = False
        self._deliver(_Resubscribed())

    def withhold_confirmation(self) -> None:
        """Leave a following of this pod unconfirmed until `resync`, like an unreachable Redis."""
        self._withholding_confirmation = True

    def refuse(self) -> None:
        """End every following of this pod, and refuse the next, with a refusal `StoreError`."""
        self._refusal = StoreError(f"subscribe to {self._channel} failed: refused")
        self._deliver(self._refusal)

    def fail_publishes(self, *errors: StoreError) -> None:
        """Make the next publishes of this pod raise `errors` in order, each delivering nothing.

        Errors given again queue behind those not yet raised; publishes deliver once none is left.
        """
        self._publish_failures.extend(errors)

    def hold_publishes(self) -> PublishHold:
        """Keep the publishes of this pod pending, delivering nothing, until the handle's `release`.

        A publish already failing by `fail_publishes` raises before it waits. A later call holds
        the publishes made after it; earlier handles keep theirs.
        """
        self._publish_hold = PublishHold()
        return self._publish_hold

    def _deliver(self, event: Wake | _Resubscribed | StoreError) -> None:
        for inbox in self._subscriptions:
            inbox.put_nowait(event)

    async def _received(self) -> AsyncGenerator[Wake | _Resubscribed]:
        if self._refusal is not None:
            raise self._refusal
        inbox: asyncio.Queue[Wake | _Resubscribed | StoreError] = asyncio.Queue()
        self._subscriptions.add(inbox)
        try:
            confirmed = not self._withholding_confirmation
            if confirmed:
                yield _Resubscribed()
            while True:
                event = await inbox.get()
                if isinstance(event, StoreError):
                    raise event
                if isinstance(event, _Resubscribed):
                    confirmed = True
                # Redis delivers no wake before it confirmed the subscription.
                if confirmed:
                    yield event
        finally:
            self._subscriptions.discard(inbox)
