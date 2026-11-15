"""The delays tandem asked `asyncio.sleep` for."""

import asyncio

from tests.support.waiting import BOUND_SECONDS


class Backoffs:
    """The delays tandem asked `asyncio.sleep` for, in order."""

    def __init__(self) -> None:
        self.delays: list[float] = []
        self._changed = asyncio.Condition()

    async def record(self, delay: float) -> None:
        async with self._changed:
            self.delays.append(delay)
            self._changed.notify_all()

    async def reached(self, count: int) -> None:
        async with asyncio.timeout(BOUND_SECONDS):
            async with self._changed:
                await self._changed.wait_for(lambda: len(self.delays) >= count)
