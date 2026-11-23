"""Tools of the deadline tests: Redis's clock, a wait for a due key, a read of a key's state and a
running serving loop with the handler it calls."""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from datetime import timedelta
from typing import Any, NoReturn, cast

from redis.asyncio import Redis

from tandem import Deadlines, Due, Script, StoreError
from tests.support.waiting import until, within

TwoPods = tuple[Redis, Deadlines, Deadlines]

SERVING_EVERY = timedelta(milliseconds=50)

KEYS_OF_THE_SCRIPT = "local keys = {due = KEYS[1], claimed = KEYS[2], claim = KEYS[3]}\n"
_STATE_SCRIPT = KEYS_OF_THE_SCRIPT + "return tandem.deadline_state(keys, ARGV[1])"


def store_error_behind_a_host_error() -> Exception:
    """A host's own error that chains a `StoreError`, as a handler or `reconcile` may raise it."""
    try:
        raise StoreError("a refusal naming the store")
    except StoreError as refusal:
        wrapped = ValueError("the host gave up")
        wrapped.__cause__ = refusal
        return wrapped


async def redis_ms(client: Redis) -> int:
    seconds, microseconds = await client.time()
    return int(seconds) * 1000 + int(microseconds) // 1000


async def until_redis_ms(client: Redis, target_ms: int) -> None:
    """Wait until Redis's own clock has reached `target_ms`."""

    async def reached() -> bool:
        return await redis_ms(client) >= target_ms

    await until(reached)


async def claim_when_due(deadlines: Deadlines, *, limit: int = 10) -> list[Due]:
    """Claim at the poll that first finds something due."""
    claimed: list[Due] = []

    async def found() -> bool:
        claimed.extend(await deadlines.claim_due(limit=limit))
        return bool(claimed)

    await until(found)
    return claimed


def key_names(deadlines: Deadlines) -> list[str]:
    """The three keys of `deadlines` in the order `KEYS_OF_THE_SCRIPT` reads them."""
    return [deadlines.keys.due, deadlines.keys.claimed, deadlines.keys.claim]


async def state_of(client: Redis, deadlines: Deadlines, key: str) -> tuple[str, int] | None:
    """`deadline_state` of `key`: ("waiting", due ms), ("claimed", claim expiry ms) or None."""
    reply = await Script(client, _STATE_SCRIPT).run(key_names(deadlines), [key])
    if reply is None:
        return None
    name, milliseconds = cast(list[object], reply)
    return (client.get_encoder().decode(name, force=True), int(cast(int, milliseconds)))


async def claimed_key(deadlines: Deadlines, key: str = "room-1") -> Due:
    """Make `key` due now and claim it."""
    await deadlines.set(key, timedelta(0))
    (due,) = await claim_when_due(deadlines)
    return due


async def expired_claim(client: Redis, deadlines: Deadlines) -> tuple[Due, tuple[str, int]]:
    """Claim `room-1` and wait until the claim has run out; the claim and its `state_of`."""
    due = await claimed_key(deadlines)
    claimed = await state_of(client, deadlines, due.key)
    assert claimed is not None
    await until_redis_ms(client, claimed[1])
    return due, claimed


def assert_waits_for(
    state: tuple[str, int] | None, span: timedelta, *, before_ms: int, after_ms: int
) -> None:
    """`state` waits for a due time `span` after some moment between `before_ms` and `after_ms`."""
    assert state is not None
    assert state[0] == "waiting"
    span_ms = int(span.total_seconds() * 1000)
    assert before_ms + span_ms <= state[1] <= after_ms + span_ms


class Handler[Item]:
    """A serving loop's handler: it records what it received and may hold each until released."""

    def __init__(self, *, hold: bool = False) -> None:
        self.items: list[Item] = []
        self._release = asyncio.Event()
        self._hold = hold

    def release(self) -> None:
        self._release.set()

    async def __call__(self, item: Item) -> None:
        self.items.append(item)
        if self._hold:
            await self._release.wait()

    async def received(self, count: int = 1) -> list[Item]:
        async def enough() -> bool:
            return len(self.items) >= count

        await until(enough)
        return self.items


@contextlib.asynccontextmanager
async def running(serve: Coroutine[Any, Any, NoReturn]) -> AsyncIterator[asyncio.Task[NoReturn]]:
    """Run a serving loop as a task for the body of the block, then cancel it."""
    task = asyncio.create_task(serve)
    try:
        yield task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def serving(
    deadlines: Deadlines,
    handle: Callable[[Due], Awaitable[None]],
    *,
    reconcile: Callable[[], Awaitable[None]] | None = None,
) -> contextlib.AbstractAsyncContextManager[asyncio.Task[NoReturn]]:
    """Run `serve_due` with `handle` as a task for the body of the block."""
    return running(deadlines.serve_due(handle, every=SERVING_EVERY, reconcile=reconcile))


async def ended(task: asyncio.Task[Any]) -> BaseException | None:
    """The exception a task ended with, waiting for it within the bound."""
    await within(asyncio.wait([task]))
    return task.exception()
