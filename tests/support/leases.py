"""Tools of the lease tests: a running `serve_lapses`, what it handled and a wiped app."""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Lapse, Leases, Liveness
from tandem.keys import lease_keys
from tests.support.waiting import until, within

SERVING_EVERY = timedelta(milliseconds=50)


class Handled:
    """The lapses a `serve_lapses` handler received, and how it is held or released."""

    def __init__(self, *, hold: bool = False) -> None:
        self.lapses: list[Lapse] = []
        self._release = asyncio.Event()
        self._hold = hold

    def release(self) -> None:
        self._release.set()

    async def __call__(self, lapse: Lapse) -> None:
        self.lapses.append(lapse)
        if self._hold:
            await self._release.wait()

    async def received(self, count: int = 1) -> list[Lapse]:
        async def enough() -> bool:
            return len(self.lapses) >= count

        await until(enough)
        return self.lapses


@contextlib.asynccontextmanager
async def serving(
    leases: Leases, handle: Handled, *, claimer: str = "the test"
) -> AsyncIterator[asyncio.Task[None]]:
    """Run `serve_lapses` as a task for the body of the block."""
    task = asyncio.create_task(leases.serve_lapses(handle, claimer=claimer, every=SERVING_EVERY))
    try:
        yield task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def wipe(observer: Redis, key_prefix: str, family: str) -> None:
    """Delete every key of the family and the app's epoch: the state a Redis restart leaves."""
    keys = lease_keys(key_prefix, family)
    await observer.delete(
        keys.epoch,
        keys.expiry,
        keys.generation,
        keys.lapse.due,
        keys.lapse.claimed,
        keys.lapse.claim,
    )


async def ended(task: asyncio.Task[None]) -> BaseException | None:
    """The exception a task ended with, waiting for it within the bound."""
    await within(asyncio.wait([task]))
    return task.exception()


async def liveness_of(leases: Leases, holder: str) -> Liveness:
    """The liveness `state` reports for one holder."""
    return (await leases.state([holder]))[holder].liveness


async def until_gone(leases: Leases, holder: str) -> None:
    """Wait until `holder` reads GONE: absent, and `lasts` past since the epoch."""

    async def gone() -> bool:
        return await liveness_of(leases, holder) is Liveness.GONE

    await until(gone)
