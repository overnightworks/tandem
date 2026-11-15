"""Bounded waits: every wait in the tests ends within a bound and fails loud when it passes."""

import asyncio
from collections.abc import Awaitable, Callable

# `asyncio.sleep` is patched by a test that records tandem's back-offs; the poll keeps the original.
_pause = asyncio.sleep

BOUND_SECONDS = 15.0
_POLL_SECONDS = 0.01


async def within[T](awaitable: Awaitable[T], bound: float = BOUND_SECONDS) -> T:
    async with asyncio.timeout(bound):
        return await awaitable


async def until(condition: Callable[[], Awaitable[bool]], bound: float = BOUND_SECONDS) -> None:
    """Poll `condition` until it holds, failing when it does not within the bound."""
    async with asyncio.timeout(bound):
        while not await condition():
            await _pause(_POLL_SECONDS)
