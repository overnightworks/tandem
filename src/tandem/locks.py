"""Locks: a hold on a name that one generation owns, started only when none is held."""

import asyncio
import contextlib
import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

from logline import StepTimer

from tandem.client import Client
from tandem.deadlines import (
    checked_every,
    checked_span,
    milliseconds,
    paused_until_stopped,
    repeat_until_stopped,
)
from tandem.keys import LockKeys, lock_keys
from tandem.leases import Liveness, finished_despite_cancellation
from tandem.script import Script

logger: logging.Logger = logging.getLogger(__name__)

_TOKEN_BYTES = 16  # 128 random bits per generation
_EXPIRED_BATCH = 10  # expired entries removed by each acquire, so abandoned names leave in time

# Every script runs this first: `TIME`, the epoch (set only when absent) and the lock state are one
# snapshot. ARGV[1] is `lasts` in ms and ARGV[2] the name. KEYS: epoch, expiry, generation.
_PRELUDE = """
local keys = {epoch = KEYS[1], expiry = KEYS[2], generation = KEYS[3]}
local lasts_ms = tonumber(ARGV[1])
local name = ARGV[2]
local state = tandem.lock_state(keys, name, lasts_ms)

local function held_by(generation)
  return state and state[1] == 'held' and state[2] == generation
end
"""

# ARGV: a fresh generation, the most expired entries to remove. Replies the generation of the lock
# that started, or of the one a replay finds already started under it; else nil.
_ACQUIRE = (
    _PRELUDE
    + """
local now_ms = tandem.now_ms()
local expired = redis.call('ZRANGEBYSCORE', keys.expiry, '-inf', now_ms, 'LIMIT', 0, ARGV[4])
for _, gone in ipairs(expired) do
  redis.call('ZREM', keys.expiry, gone)
  redis.call('HDEL', keys.generation, gone)
end
if held_by(ARGV[3]) then
  return ARGV[3]
end
if state then
  return nil
end
redis.call('HSET', keys.generation, name, ARGV[3])
redis.call('ZADD', keys.expiry, now_ms + lasts_ms, name)
return ARGV[3]
"""
)

# ARGV: the generation. Only the live lock of that generation is extended.
_EXTEND = (
    _PRELUDE
    + """
if not held_by(ARGV[3]) then
  return 0
end
redis.call('ZADD', keys.expiry, tandem.now_ms() + lasts_ms, name)
return 1
"""
)

# ARGV: the generation. Only the live lock of that generation ends.
_RELEASE = (
    _PRELUDE
    + """
if not held_by(ARGV[3]) then
  return 0
end
redis.call('ZREM', keys.expiry, name)
redis.call('HDEL', keys.generation, name)
return 1
"""
)

# Replies {'held', generation, ms left}, {'unknown'} or nil.
_STATE = (
    _PRELUDE
    + """
if state and state[1] == 'held' then
  return {'held', state[2], math.max(state[3] - tandem.now_ms(), 0)}
end
return state
"""
)


@dataclass(frozen=True)
class LockState:
    liveness: Liveness  # ALIVE held, GONE free, UNKNOWN absent but possibly lost to a restart
    generation: str | None  # the holder's, while ALIVE
    remaining: timedelta | None  # until the lock expires, while ALIVE


class LockLost(Exception):
    """The lock of a `held` block is not the block's any more: it ran out, or Redis lost it."""


class _Keeping:
    """What one `held` block and its keeper share; the keeper cancels `entering` on a loss."""

    def __init__(self, entering: asyncio.Task[object] | None) -> None:
        self.stopping = asyncio.Event()  # the exit sets it before its first await
        self.lost = False
        self._entering = entering
        self._cancelling_at_entry = 0 if entering is None else entering.cancelling()
        self._delivered = 0

    async def cancel_until_stopped(self, every_seconds: float) -> None:
        """Cancel the entering task, again every `every_seconds`, while the block runs: a cancel
        scope inside the block can absorb one cancel."""
        if self._entering is None:
            return
        while not self.stopping.is_set():
            self._entering.cancel()
            self._delivered += 1
            await paused_until_stopped(self.stopping, every_seconds)

    def exits_in_the_entering_task(self) -> bool:
        return self._entering is None or self._entering is asyncio.current_task()

    def withdraw_cancels(self, block_error: BaseException | None) -> bool:
        """Withdraw every cancel the keeper delivered; True when they alone ended the block, which
        is a cancellation that no cancel of the host's outlasts, as `asyncio.timeout` reads it."""
        if self._entering is None:
            return False
        for _ in range(self._delivered):
            self._entering.uncancel()
        return (
            self._delivered > 0
            and isinstance(block_error, asyncio.CancelledError)
            and self._entering.cancelling() <= self._cancelling_at_entry
        )


class Locks:
    def __init__(self, redis: Client, *, key_prefix: str, family: str, lasts: timedelta) -> None:
        """The locks of one `family` in the app of `key_prefix`.

        A lock lasts `lasts` after its latest start or extension. Every pod of a family passes the
        same `lasts`. `key_prefix` and `family` are each one segment (`checked_segment`) and `lasts`
        is a positive `timedelta`.
        """
        self._redis = redis
        self._family = family
        self._lasts = checked_span(lasts, argument="lasts")
        self._keys = lock_keys(key_prefix, family)
        self._lasts_ms = milliseconds(lasts)
        self._acquire = Script(redis, _ACQUIRE)
        self._extend = Script(redis, _EXTEND)
        self._release = Script(redis, _RELEASE)
        self._state = Script(redis, _STATE)

    @property
    def keys(self) -> LockKeys:
        """The three keys of the family, for a host's script that runs `tandem.lock_state`."""
        return self._keys

    @property
    def lasts(self) -> timedelta:
        """How long a lock lasts, for the `lasts_ms` argument of `tandem.lock_state`."""
        return self._lasts

    async def acquire(self, name: str) -> str | None:
        """Start the lock on `name` under a fresh generation, or return None when it is held.

        The holder acquiring again gets None too. After a Redis restart it returns None until
        `lasts` have passed since the epoch. A reply lost under the client's `Retry` replays the
        script with the same generation, which then returns it.
        """
        generation = secrets.token_hex(_TOKEN_BYTES)
        async with self._step("lock_acquisition.ended"):
            reply = await self._acquire.run(
                self._key_names(), self._arguments(name, generation, _EXPIRED_BATCH)
            )
        return None if reply is None else self._text(reply)

    async def extend(self, name: str, generation: str) -> bool:
        """Keep the live lock of `generation` for another `lasts`; else change nothing."""
        async with self._step("lock_extension.ended"):
            return await self._extend_untimed(name, generation)

    async def _extend_untimed(self, name: str, generation: str) -> bool:
        return await self._acted(self._extend, name, generation)

    async def release(self, name: str, generation: str) -> bool:
        """End the live lock of `generation`; else change nothing.

        A reply lost under the client's `Retry` replays a release that succeeded as False.
        """
        async with self._step("lock_release.ended"):
            return await self._release_untimed(name, generation)

    async def _release_untimed(self, name: str, generation: str) -> bool:
        return await self._acted(self._release, name, generation)

    def held(
        self, name: str, generation: str, *, every: timedelta, cancel_on_loss: bool = True
    ) -> AbstractAsyncContextManager[None]:
        """Keep the lock of `generation` for the block: extend now, then every `every`, and release
        it at exit unless it was lost.

        The first extension completes before the block runs: a False raises `LockLost` and a
        `StoreError` raises out of the entry, and nothing is released. A later failed extension, an
        outage or a refusal, is retried after `every` and never ends the block; the first failure of
        a run is logged as a warning and the extension that ends the run at info, with
        `failed_run_count`. The first background False is a loss: it is logged, extending stops and
        the exit releases nothing.

        With `cancel_on_loss`, a loss cancels the task that entered the block, again every `every`
        until the block ends, and withdraws each cancel at the exit: when they ended the block,
        `LockLost` is raised; a block that ended first exits as it ended, and the host's own
        cancellation still propagates. The exit must run in the task that entered. Without it the
        keeper touches no task, and the block, which may exit in another task, is only told by the
        log line and fences its work itself. At exit an extension in flight completes, then the lock
        is released, even when the exit is cancelled; the cancellation then propagates.
        """
        return self._held(name, generation, checked_every(every, self._lasts), cancel_on_loss)

    @contextlib.asynccontextmanager
    async def _held(
        self, name: str, generation: str, every: timedelta, cancel_on_loss: bool
    ) -> AsyncIterator[None]:
        if not await self.extend(name, generation):
            raise LockLost
        keeping = _Keeping(asyncio.current_task() if cancel_on_loss else None)
        keeper = asyncio.create_task(self._keep(keeping, name, generation, every.total_seconds()))
        block_error: BaseException | None = None
        try:
            yield
        except BaseException as error:
            block_error = error
            raise
        finally:
            keeping.stopping.set()  # from here the keeper cancels nothing
            exits_in_the_entering_task = keeping.exits_in_the_entering_task()
            cancelled_by_the_keeper = keeping.withdraw_cancels(block_error)
            await self._leave(keeping, keeper, name, generation)
            if not exits_in_the_entering_task:
                raise RuntimeError(
                    "held(..., cancel_on_loss=True) must exit in the task that entered"
                )
            if cancelled_by_the_keeper:
                raise LockLost from None

    async def _keep(
        self, keeping: _Keeping, name: str, generation: str, every_seconds: float
    ) -> None:
        async def extend() -> bool:
            if await self._extend_untimed(name, generation):
                return True
            keeping.lost = True
            logger.warning("lock.lost", extra={"lock_family": self._family})
            return False

        await repeat_until_stopped(
            keeping.stopping, every_seconds, extend, lambda: self._step("lock_keeping.ended")
        )
        if keeping.lost:
            await keeping.cancel_until_stopped(every_seconds)

    async def _leave(
        self, keeping: _Keeping, keeper: asyncio.Task[None], name: str, generation: str
    ) -> None:
        """The exit's two phases, which a cancellation holds until both end: the keeper's last
        extension, untimed, then the release unless the lock was lost, which writes no release
        line and whose line is timed from its start."""
        cancelled: asyncio.CancelledError | None = None
        try:
            await finished_despite_cancellation(keeper, self._log_release_failed)
        except asyncio.CancelledError as cancellation:
            cancelled = cancellation
        if not keeping.lost:
            try:
                async with self._step("lock_release.ended"):
                    await finished_despite_cancellation(
                        asyncio.create_task(self._release_untimed(name, generation)),
                        self._log_release_failed,
                    )
            except asyncio.CancelledError as cancellation:
                cancelled = cancellation
            except Exception as failure:
                if cancelled is None:
                    raise
                self._log_release_failed(failure)
        if cancelled is not None:
            raise cancelled

    def _log_release_failed(self, failure: BaseException) -> None:
        logger.error(
            "lock_release.failed",
            extra={"error_type": type(failure).__name__, "lock_family": self._family},
        )

    async def state(self, name: str) -> LockState:
        """Whether `name` is held (ALIVE), free (GONE), or unsettled after a restart (UNKNOWN)."""
        async with self._step("lock_state_read.ended"):
            reply = await self._state.run(self._key_names(), self._arguments(name))
        if reply is None:
            return LockState(Liveness.GONE, None, None)
        _, *held = cast(list[object], reply)
        if not held:
            return LockState(Liveness.UNKNOWN, None, None)
        generation, remaining_ms = held
        return LockState(
            Liveness.ALIVE, self._text(generation), timedelta(milliseconds=cast(int, remaining_ms))
        )

    async def _acted(self, script: Script, name: str, generation: str) -> bool:
        """Whether `script` acted on the live lock of `generation`."""
        reply = await script.run(self._key_names(), self._arguments(name, generation))
        return reply == 1

    def _step(self, event: str) -> StepTimer:
        return StepTimer(event, logger, lock_family=self._family)

    def _key_names(self) -> list[str]:
        return [self._keys.epoch, self._keys.expiry, self._keys.generation]

    def _arguments(self, *rest: str | int) -> list[str | int | bytes]:
        return [self._lasts_ms, *rest]

    def _text(self, value: object) -> str:
        return self._redis.get_encoder().decode(value, force=True)
