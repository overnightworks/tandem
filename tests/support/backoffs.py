"""The delays tandem waited between its attempts."""

import asyncio

from tests.support.waiting import BOUND_SECONDS


class Backoffs:
    """The delays tandem waited, in order; `sleep` is what a test hands to `WakeChannel`."""

    def __init__(self) -> None:
        self.delays: list[float] = []
        self._changed = asyncio.Condition()

    async def sleep(self, delay: float) -> None:
        async with self._changed:
            self.delays.append(delay)
            self._changed.notify_all()
        await asyncio.sleep(delay)

    async def reached(self, count: int) -> None:
        async with asyncio.timeout(BOUND_SECONDS):
            async with self._changed:
                await self._changed.wait_for(lambda: len(self.delays) >= count)
