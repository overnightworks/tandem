"""The delays tandem waits between its attempts, recorded instead of waited."""

import asyncio
from typing import Any

from redis.asyncio import Redis

from tandem import Deadlines, Leases, WakeChannel
from tests.support.waiting import BOUND_SECONDS


class Backoffs:
    """The delays a `BackoffRecordingChannel` was asked to wait, in order."""

    def __init__(self, *, hold: bool) -> None:
        self.delays: list[float] = []
        self.hold = hold
        self._changed = asyncio.Condition()

    async def record(self, delay: float) -> None:
        """Note the delay; a holding record then waits until cancelled, any other returns soon."""
        async with self._changed:
            self.delays.append(delay)
            self._changed.notify_all()
        if self.hold:
            await asyncio.Event().wait()
        await asyncio.sleep(0)

    async def reached(self, count: int) -> None:
        async with asyncio.timeout(BOUND_SECONDS):
            async with self._changed:
                await self._changed.wait_for(lambda: len(self.delays) >= count)


class BackoffRecordingChannel(WakeChannel):
    """A `WakeChannel` whose waits between reconnect attempts go to `backoffs`."""

    def __init__(self, redis: Redis, *, key_prefix: str, backoffs: Backoffs) -> None:
        super().__init__(redis, key_prefix=key_prefix)
        self._backoffs = backoffs

    async def _wait(self, seconds: float) -> None:
        await self._backoffs.record(seconds)


class BackoffRecordingLeases(Leases):
    """`Leases` whose waits after a failed round go to `backoffs`."""

    def __init__(self, *args: Any, backoffs: Backoffs, **keywords: Any) -> None:
        super().__init__(*args, **keywords)
        self._backoffs = backoffs

    async def _back_off(self, seconds: float) -> None:
        await self._backoffs.record(seconds)


class BackoffRecordingDeadlines(Deadlines):
    """`Deadlines` whose waits after a failed round go to `backoffs`."""

    def __init__(self, *args: Any, backoffs: Backoffs, **keywords: Any) -> None:
        super().__init__(*args, **keywords)
        self._backoffs = backoffs

    async def _back_off(self, seconds: float) -> None:
        await self._backoffs.record(seconds)
