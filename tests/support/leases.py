"""Tools of the lease tests: a running `serve_lapses`, what it handled and a wiped app."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import NoReturn

from redis.asyncio import Redis

from tandem import Lapse, Leases, Liveness
from tandem.keys import lease_keys
from tests.support.deadlines import SERVING_EVERY, Handler, running
from tests.support.waiting import until


class Handled(Handler[Lapse]):
    """The lapses a `serve_lapses` handler received, and how it is held or released."""


def serving(
    leases: Leases, handle: Callable[[Lapse], Awaitable[None]]
) -> AbstractAsyncContextManager[asyncio.Task[NoReturn]]:
    """Run `serve_lapses` with `handle` as a task for the body of the block."""
    return running(leases.serve_lapses(handle, every=SERVING_EVERY))


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


async def liveness_of(leases: Leases, holder: str) -> Liveness:
    """The liveness `state` reports for one holder."""
    return (await leases.state([holder]))[holder].liveness


async def until_gone(leases: Leases, holder: str) -> None:
    """Wait until `holder` reads GONE: absent, and `lasts` past since the epoch."""

    async def gone() -> bool:
        return await liveness_of(leases, holder) is Liveness.GONE

    await until(gone)
