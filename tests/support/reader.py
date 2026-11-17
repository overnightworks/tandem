"""A follower of a WakeChannel that a test steps through one event at a time."""

import asyncio
import contextlib
from dataclasses import dataclass
from types import TracebackType

from tandem import Wake, WakeChannel
from tests.support.waiting import BOUND_SECONDS, within


@dataclass(frozen=True)
class Resynced:
    """The host's `on_resync` was called."""


class Reader:
    def __init__(self, channel: WakeChannel, *, ready_within: float = BOUND_SECONDS) -> None:
        self._channel = channel
        self._ready_within = ready_within
        self._events: asyncio.Queue[Wake | Resynced | BaseException] = asyncio.Queue()
        self._stack = contextlib.AsyncExitStack()
        self._running = False

    async def __aenter__(self) -> Reader:
        following = self._channel.following(
            on_resync=lambda: self._events.put_nowait(Resynced()),
            on_wake=self._events.put_nowait,
            on_failed=self._failed,
            ready_within=self._ready_within,
        )
        await self._stack.enter_async_context(following)
        self._running = True
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._running = False
        await within(self._stack.aclose())

    def _failed(self, error: Exception) -> None:
        self._running = False
        self._events.put_nowait(error)

    async def next(self) -> Wake | Resynced:
        event = await within(self._events.get())
        if isinstance(event, BaseException):
            raise event
        return event

    async def next_resynced(self) -> Resynced:
        event = await self.next()
        assert isinstance(event, Resynced), event
        return event

    async def next_wake(self) -> Wake:
        event = await self.next()
        assert isinstance(event, Wake), event
        return event

    def drain(self) -> list[Wake | Resynced]:
        """Everything already delivered and not yet read."""
        drained: list[Wake | Resynced] = []
        while not self._events.empty():
            event = self._events.get_nowait()
            if isinstance(event, BaseException):
                raise event
            drained.append(event)
        return drained

    @property
    def running(self) -> bool:
        """True from entry until the follow fails or the reader is left."""
        return self._running
