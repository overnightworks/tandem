"""A reader of a WakeChannel that a test steps through one event at a time."""

import asyncio
import contextlib
from types import TracebackType

from tandem import Resubscribed, Wake, WakeChannel
from tests.support.waiting import within


class Reader:
    def __init__(self, channel: WakeChannel) -> None:
        self._channel = channel
        self._events: asyncio.Queue[Wake | Resubscribed | BaseException] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Reader:
        self._task = asyncio.create_task(self._pump())
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.cancel()

    async def _pump(self) -> None:
        try:
            async for event in self._channel.received():
                self._events.put_nowait(event)
        except Exception as error:
            self._events.put_nowait(error)

    async def next(self) -> Wake | Resubscribed:
        event = await within(self._events.get())
        if isinstance(event, BaseException):
            raise event
        return event

    async def next_resubscribed(self) -> Resubscribed:
        event = await self.next()
        assert isinstance(event, Resubscribed), event
        return event

    async def next_wake(self) -> Wake:
        event = await self.next()
        assert isinstance(event, Wake), event
        return event

    def drain(self) -> list[Wake | Resubscribed]:
        """Everything already delivered and not yet read."""
        drained: list[Wake | Resubscribed] = []
        while not self._events.empty():
            event = self._events.get_nowait()
            if isinstance(event, BaseException):
                raise event
            drained.append(event)
        return drained

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def cancel(self) -> None:
        assert self._task is not None
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await within(self._task)
