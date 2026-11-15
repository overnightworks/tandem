"""Leases: a holder's claim to be alive, with a generation per start and a claim on each lapse."""

import asyncio
import contextlib
import logging
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Collection
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import NoReturn, cast

from tandem.client import Client
from tandem.deadlines import DeadlineQueue, Due, checked_span, milliseconds
from tandem.errors import BACKOFF_FIRST_SECONDS, StoreError, next_backoff
from tandem.keys import lease_keys
from tandem.script import Script

logger: logging.Logger = logging.getLogger(__name__)


def _split_due_key(due: Due) -> tuple[str, str]:
    """The generation and the holder of a lapse's due key; a generation is never logged."""
    # Split at the first ':' only: a generation holds none, a holder may.
    generation, _, holder = due.key.partition(":")
    return generation, holder


_TOKEN_BYTES = 16  # 128 random bits per generation
_BATCH = 8  # lapses claimed at once, so a claim does not run out while its batch waits
_CLAIM_EXTENSIONS_PER_CLAIM = 3

# Every script runs this first: `TIME`, the epoch (set only when absent) and the lease state are one
# snapshot. ARGV[1] is `lasts` in ms. KEYS: epoch, expiry, generation, then the lapse queue's due,
# claimed and claim.
_PRELUDE = """
local epoch_key, expiry, generations = KEYS[1], KEYS[2], KEYS[3]
local lapse = {due = KEYS[4], claimed = KEYS[5], claim = KEYS[6]}
local lasts_ms = tonumber(ARGV[1])
local now_ms = tandem.now_ms()
local settled = tandem.epoch_settled(epoch_key, now_ms, lasts_ms)

-- The expiry in ms of the holder's live lease, else false.
local function live_until(holder)
  local expires_ms = redis.call('ZSCORE', expiry, holder)
  if expires_ms and tonumber(expires_ms) > now_ms then
    return tonumber(expires_ms)
  end
  return false
end

-- The holder's generation leaves its HASH and enters the lapse queue, due now.
local function lapse_out(holder)
  local generation = redis.call('HGET', generations, holder)
  if generation then
    tandem.deadline_set(lapse, generation .. ':' .. holder, 0)
    redis.call('HDEL', generations, holder)
  end
end
"""

# ARGV: holder, a fresh generation. Replies {1 when a lease started, its generation}.
_RENEW = (
    _PRELUDE
    + """
local holder = ARGV[2]
if live_until(holder) then
  redis.call('ZADD', expiry, now_ms + lasts_ms, holder)
  return {0, redis.call('HGET', generations, holder)}
end
lapse_out(holder)
redis.call('HSET', generations, holder, ARGV[3])
redis.call('ZADD', expiry, now_ms + lasts_ms, holder)
return {1, ARGV[3]}
"""
)

# ARGV: holder, generation. Only a live lease of that generation ends.
_END = (
    _PRELUDE
    + """
local holder = ARGV[2]
if redis.call('HGET', generations, holder) ~= ARGV[3] or not live_until(holder) then
  return 0
end
redis.call('ZREM', expiry, holder)
lapse_out(holder)
return 1
"""
)

# ARGV: holders. One reply per holder: {1, generation, ms left} alive, {2} unknown, {0} gone.
_STATE = (
    _PRELUDE
    + """
local reply = {}
for index = 2, #ARGV do
  local holder = ARGV[index]
  local expires_ms = live_until(holder)
  if expires_ms then
    reply[#reply + 1] = {1, redis.call('HGET', generations, holder), expires_ms - now_ms}
  elseif settled then
    reply[#reply + 1] = {0}
  else
    reply[#reply + 1] = {2}
  end
end
return reply
"""
)

# Replies 1 when settled, else 0, then every live holder.
_LIVE = (
    _PRELUDE
    + """
local reply = {settled and 1 or 0}
for _, holder in ipairs(redis.call('ZRANGEBYSCORE', expiry, '(' .. now_ms, '+inf')) do
  reply[#reply + 1] = holder
end
return reply
"""
)

# Every expired lease moves into the lapse queue.
_SWEEP = (
    _PRELUDE
    + """
for _, holder in ipairs(redis.call('ZRANGEBYSCORE', expiry, '-inf', now_ms)) do
  redis.call('ZREM', expiry, holder)
  lapse_out(holder)
end
"""
)


class Liveness(StrEnum):
    ALIVE = "alive"
    GONE = "gone"
    UNKNOWN = (
        "unknown"  # no lease, and Redis may have lost it: `lasts` have not passed since the epoch
    )


_LIVENESS_OF_REPLY = {0: Liveness.GONE, 1: Liveness.ALIVE, 2: Liveness.UNKNOWN}


@dataclass(frozen=True)
class LeaseState:
    liveness: (
        Liveness  # ALIVE while the lease is live, GONE, UNKNOWN when a restart may have lost it
    )
    generation: str | None  # the lease's, while ALIVE; a value that can end the lease
    remaining: timedelta | None  # until the lease expires, while ALIVE


@dataclass(frozen=True)
class Renewal:
    started: bool  # a new lease began; False when a live one was extended
    generation: str  # random per start, to be compared by equality


@dataclass(frozen=True)
class Lapse:
    holder: str
    generation: str
    due: Due


@dataclass(frozen=True)
class Roster:
    holders: frozenset[str]
    settled: bool  # `lasts` have passed since the epoch: a missing holder is gone, not lost


@dataclass
class Hold:
    """What `Leases.kept` yields: the generation of the holder's current lease."""

    generation: str


async def _finished_despite_cancellation(cleanup: asyncio.Task[None]) -> None:
    """Wait for `cleanup` however often the caller is cancelled, then propagate the cancellation.

    A shield alone raises in the cancelled caller at once, lets `cleanup` outlive it and logs a
    later failure of `cleanup` itself. A failure of `cleanup` after a cancellation is logged once
    and never replaces the cancellation; a cancelled `cleanup` is no failure.
    """
    try:
        await asyncio.wait((cleanup,))
    except asyncio.CancelledError:
        while not cleanup.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait((cleanup,))
        failure = None if cleanup.cancelled() else cleanup.exception()
        if failure is not None:
            logger.error("the cleanup failed after its caller was cancelled", exc_info=failure)
        raise
    cleanup.result()


class Leases:
    def __init__(
        self,
        redis: Client,
        *,
        key_prefix: str,
        family: str,
        lasts: timedelta,
        claim_for: timedelta,
    ) -> None:
        """The leases of one `family` in the app of `key_prefix`.

        A lease lasts `lasts` after its latest renewal; a claim on a lapse holds for `claim_for`.
        Every pod of a family passes the same two. `key_prefix` and `family` are each one segment
        (`checked_segment`); both spans must be positive.
        """
        self._redis = redis
        self._lasts = checked_span(lasts, argument="lasts")
        self._claim_for = checked_span(claim_for, argument="claim_for")
        self._family = family
        self._keys = lease_keys(key_prefix, family)
        self._lasts_ms = milliseconds(lasts)
        self._lapses = DeadlineQueue(redis, self._keys.lapse, claim_for)
        self._renew = Script(redis, _RENEW)
        self._end = Script(redis, _END)
        self._state = Script(redis, _STATE)
        self._live = Script(redis, _LIVE)
        self._sweep = Script(redis, _SWEEP)

    async def renew(self, holder: str) -> Renewal:
        """Keep `holder`'s lease for another `lasts`, starting one under a new generation if none
        is live.

        A reply lost under the client's `Retry` can replay a renewal as `started=False`.
        """
        generation = secrets.token_hex(_TOKEN_BYTES)
        started, current = cast(
            list[object],
            await self._renew.run(self._key_names(), self._arguments(holder, generation)),
        )
        return Renewal(started=started == 1, generation=self._text(current))

    async def end(self, holder: str, generation: str) -> bool:
        """End the live lease of `generation`, its lapse due now; else change nothing."""
        reply = await self._end.run(self._key_names(), self._arguments(holder, generation))
        return reply == 1

    async def state(self, holders: Collection[str]) -> dict[str, LeaseState]:
        """The state of each of `holders`, read in one script; it renews and starts nothing.

        A holder is ALIVE with its generation and the time left, GONE, or UNKNOWN right after a
        Redis restart. A generation lets `end` the lease: an app keeps it to itself.
        """
        if not holders:
            return {}
        names = list(holders)
        reply = await self._state.run(self._key_names(), self._arguments(*names))
        return {
            name: self._lease_state(cast(list[object], held))
            for name, held in zip(names, cast(list[object], reply), strict=True)
        }

    async def live(self) -> Roster:
        """Every live holder, with whether the roster is complete (`settled`), in one read."""
        reply = cast(list[object], await self._live.run(self._key_names(), self._arguments()))
        return Roster(
            holders=frozenset(self._text(holder) for holder in reply[1:]), settled=reply[0] == 1
        )

    def kept(self, holder: str, *, every: timedelta) -> AbstractAsyncContextManager[Hold]:
        """Hold `holder`'s lease for the block: renew now, then every `every`, and end it at exit.

        The first renewal completes before the block runs and raises `StoreError` out of the entry.
        A later failed renewal, an outage or a refusal, is logged as an error and retried after
        `every`, and never ends the block. At exit an in-flight renewal completes, then the current
        generation ends, even when the exit is cancelled; the cancellation then propagates.
        """
        if checked_span(every, argument="every") >= self._lasts:
            raise ValueError(f"every must be below lasts ({self._lasts}), got {every!r}")
        return self._kept(holder, every)

    @contextlib.asynccontextmanager
    async def _kept(self, holder: str, every: timedelta) -> AsyncIterator[Hold]:
        hold = Hold(generation=(await self.renew(holder)).generation)
        stopping = asyncio.Event()
        renewer = asyncio.create_task(
            self._renew_until(stopping, holder, every.total_seconds(), hold)
        )
        try:
            yield hold
        finally:
            await _finished_despite_cancellation(
                asyncio.create_task(self._finish(stopping, renewer, holder, hold))
            )

    async def _finish(
        self, stopping: asyncio.Event, renewer: asyncio.Task[None], holder: str, hold: Hold
    ) -> None:
        stopping.set()
        await renewer
        await self.end(holder, hold.generation)

    async def _renew_until(
        self, stopping: asyncio.Event, holder: str, every_seconds: float, hold: Hold
    ) -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stopping.wait(), every_seconds)
            if stopping.is_set():
                return
            try:
                hold.generation = (await self.renew(holder)).generation
            except StoreError:
                logger.exception("the renewal of the lease of %s failed", holder)

    async def serve_lapses(
        self,
        handle: Callable[[Lapse], Awaitable[None]],
        *,
        claimer: str,
        every: timedelta,
    ) -> NoReturn:
        """Hand every lapse to `handle`, at least once, until cancelled.

        Each `every` the expired leases are swept into the lapse queue and the due lapses are
        claimed in small batches until none is due. While `handle` runs its claim is extended every
        `claim_for / 3`; an extension that fails stops, and `handle` keeps running. `done` follows a
        `handle` that returns; one that raises is logged and its lapse returns when its claim runs
        out. An outage of Redis is ridden out with a back-off; a refusal raises `StoreError`.
        """
        pause = checked_span(every, argument="every").total_seconds()
        backoff = BACKOFF_FIRST_SECONDS
        while True:
            try:
                await self._serve_round(handle, claimer)
            except StoreError as error:
                if not error.outage:
                    raise
                await self._back_off(backoff)
                backoff = next_backoff(backoff)
                continue
            backoff = BACKOFF_FIRST_SECONDS
            await asyncio.sleep(pause)

    async def _back_off(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def _serve_round(self, handle: Callable[[Lapse], Awaitable[None]], claimer: str) -> None:
        await self._sweep.run(self._key_names(), self._arguments())
        while batch := await self._lapses.claim_due(claimer=claimer, limit=_BATCH):
            await self._handle_batch(handle, batch)

    async def _handle_batch(
        self, handle: Callable[[Lapse], Awaitable[None]], batch: list[Due]
    ) -> None:
        pending = set(batch)
        extender = asyncio.create_task(self._extend_while_pending(pending))
        try:
            for due in batch:
                await self._handle_one(handle, due, pending)
        finally:
            extender.cancel()
            await asyncio.gather(extender, return_exceptions=True)

    async def _handle_one(
        self, handle: Callable[[Lapse], Awaitable[None]], due: Due, pending: set[Due]
    ) -> None:
        generation, holder = _split_due_key(due)
        lapse = Lapse(holder=holder, generation=generation, due=due)
        try:
            await handle(lapse)
        except Exception:
            logger.exception("the handler of the lapse of %s failed", holder)
            pending.discard(due)
            return
        pending.discard(due)
        await self._lapses.done(due)

    async def _extend_while_pending(self, pending: set[Due]) -> None:
        interval = self._claim_for.total_seconds() / _CLAIM_EXTENSIONS_PER_CLAIM
        while True:
            await asyncio.sleep(interval)
            for due in tuple(pending):
                try:
                    kept = await self._lapses.extend(due)
                except StoreError as error:
                    logger.warning("extending the claim of a lapse failed: %s", error)
                    pending.discard(due)
                    continue
                if not kept:
                    _, holder = _split_due_key(due)
                    logger.warning(
                        "the claim of the lapse of %s in family %s ran out", holder, self._family
                    )
                    pending.discard(due)

    def _lease_state(self, reply: list[object]) -> LeaseState:
        liveness = _LIVENESS_OF_REPLY[cast(int, reply[0])]
        if liveness is not Liveness.ALIVE:
            return LeaseState(liveness, None, None)
        _, generation, remaining_ms = reply
        return LeaseState(
            liveness, self._text(generation), timedelta(milliseconds=cast(int, remaining_ms))
        )

    def _key_names(self) -> list[str]:
        keys, lapse = self._keys, self._keys.lapse
        return [keys.epoch, keys.expiry, keys.generation, lapse.due, lapse.claimed, lapse.claim]

    def _arguments(self, *rest: str) -> list[str | int | bytes]:
        return [self._lasts_ms, *rest]

    def _text(self, value: object) -> str:
        return self._redis.get_encoder().decode(value, force=True)
