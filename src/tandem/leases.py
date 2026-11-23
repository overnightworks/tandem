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

from logline import StepTimer

from tandem.client import Client
from tandem.deadlines import DeadlineQueue, Due, checked_span, milliseconds, serve_queue
from tandem.errors import StoreError, failure_reason
from tandem.keys import lease_keys
from tandem.script import Script

logger: logging.Logger = logging.getLogger(__name__)

_TOKEN_BYTES = 16  # 128 random bits per generation

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


def _lapse_of(due: Due) -> Lapse:
    # Split at the first ':' only: a generation holds none, a holder may.
    generation, _, holder = due.key.partition(":")
    return Lapse(holder=holder, generation=generation, due=due)


class _LapseLines:
    """The lines of `serve_lapses`; each names the lapse's holder."""

    def __init__(self, family: str) -> None:
        self._family = family

    def round_counts(self, claimed: int, failed: int) -> dict[str, int]:
        return {"lapse_count": claimed, "failed_count": failed}

    def round_timer(self) -> StepTimer:
        return StepTimer(
            "lapse_round.ended", logger, lease_family=self._family, **self.round_counts(0, 0)
        )

    def handling_timer(self, due: Due) -> StepTimer:
        return _lease_step_timer("lapse_handling.ended", self._family, _lapse_of(due).holder)

    def handler_failed(self, due: Due, error: Exception) -> None:
        logger.error(
            "lapse_handler.failed",
            extra={
                "error_type": type(error).__name__,
                **_lease_fields(self._family, _lapse_of(due).holder),
            },
            exc_info=error,
        )

    def extension_failed(self, due: Due, error: StoreError) -> None:
        logger.warning(
            "lapse_claim_extension.failed",
            extra={
                "reason": failure_reason(error),
                "error_type": type(error).__name__,
                **_lease_fields(self._family, _lapse_of(due).holder),
            },
        )

    def claim_expired(self, due: Due) -> None:
        logger.warning(
            "lapse_claim.expired", extra=_lease_fields(self._family, _lapse_of(due).holder)
        )


def _lease_fields(family: str, holder: str) -> dict[str, str]:
    return {"lease_family": family, "holder_id": holder}


def _lease_step_timer(event: str, family: str, holder: str) -> StepTimer:
    return StepTimer(event, logger, lease_family=family, holder_id=holder)


@dataclass
class Hold:
    """What `Leases.kept` yields: the generation of the holder's current lease."""

    generation: str


async def _finished_despite_cancellation(
    cleanup: asyncio.Task[None], lease_fields: dict[str, str]
) -> None:
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
            logger.error(
                "lease_end.failed", extra={"error_type": type(failure).__name__, **lease_fields}
            )
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
        self._family = family
        self._lasts = checked_span(lasts, argument="lasts")
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

        `started` is True whenever the generation this call proposed is the live one afterwards, so
        a renewal that a reply lost under the client's `Retry` replayed still reads as the start it
        was.
        """
        async with _lease_step_timer("lease_renewal.ended", self._family, holder):
            return await self._renew_untimed(holder)

    async def _renew_untimed(self, holder: str) -> Renewal:
        generation = secrets.token_hex(_TOKEN_BYTES)
        started, current = cast(
            list[object],
            await self._renew.run(self._key_names(), self._arguments(holder, generation)),
        )
        live_generation = self._text(current)
        return Renewal(
            started=started == 1 or live_generation == generation, generation=live_generation
        )

    async def end(self, holder: str, generation: str) -> bool:
        """End the live lease of `generation`, its lapse due now; else change nothing."""
        async with _lease_step_timer("lease_end.ended", self._family, holder):
            return await self._end_untimed(holder, generation)

    async def _end_untimed(self, holder: str, generation: str) -> bool:
        reply = await self._end.run(self._key_names(), self._arguments(holder, generation))
        return reply == 1

    async def state(self, holders: Collection[str]) -> dict[str, LeaseState]:
        """The state of each of `holders`, read in one script; it renews and starts nothing.

        A holder is ALIVE with its generation and the time left, GONE, or UNKNOWN right after a
        Redis restart. A generation lets `end` the lease: an app keeps it to itself.
        """
        async with StepTimer("lease_state_read.ended", logger, lease_family=self._family):
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
        async with StepTimer("lease_roster_read.ended", logger, lease_family=self._family):
            reply = cast(list[object], await self._live.run(self._key_names(), self._arguments()))
        return Roster(
            holders=frozenset(self._text(holder) for holder in reply[1:]), settled=reply[0] == 1
        )

    def kept(self, holder: str, *, every: timedelta) -> AbstractAsyncContextManager[Hold]:
        """Hold `holder`'s lease for the block: renew now, then every `every`, and end it at exit.

        The first renewal completes before the block runs and raises `StoreError` out of the entry.
        A later failed renewal, an outage or a refusal, is logged as a warning and retried after
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
            async with _lease_step_timer("lease_end.ended", self._family, holder):
                await _finished_despite_cancellation(
                    asyncio.create_task(self._finish(stopping, renewer, holder, hold)),
                    _lease_fields(self._family, holder),
                )

    async def _finish(
        self, stopping: asyncio.Event, renewer: asyncio.Task[None], holder: str, hold: Hold
    ) -> None:
        stopping.set()
        await renewer
        await self._end_untimed(holder, hold.generation)

    async def _renew_until(
        self, stopping: asyncio.Event, holder: str, every_seconds: float, hold: Hold
    ) -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stopping.wait(), every_seconds)
            if stopping.is_set():
                return
            try:
                hold.generation = (await self._renew_untimed(holder)).generation
            except StoreError as error:
                logger.warning(
                    "lease_renewal.failed",
                    extra={
                        "reason": failure_reason(error),
                        "error_type": type(error).__name__,
                        **_lease_fields(self._family, holder),
                    },
                )

    async def serve_lapses(
        self,
        handle: Callable[[Lapse], Awaitable[None]],
        *,
        every: timedelta,
    ) -> NoReturn:
        """Hand every lapse to `handle`, at least once, until cancelled.

        Each `every` the expired leases are swept into the lapse queue and the due lapses are
        claimed in small batches until none is due. While `handle` runs its claim is extended every
        `claim_for / 3`; an extension that Redis fails stops, and `handle` keeps running; any other
        exception from it ends the loop. `done` follows a `handle` that returns; one that raises is
        logged and its lapse returns when its claim runs out. An outage or a refusal of Redis is
        ridden out with the back-off of the wake channel's following.
        """

        async def handle_due(due: Due) -> None:
            await handle(_lapse_of(due))

        await serve_queue(
            self._lapses,
            handle_due,
            every=every,
            before_claim=self._sweep_expired,
            back_off=self._back_off,
            lines=_LapseLines(self._family),
        )

    async def _sweep_expired(self) -> None:
        await self._sweep.run(self._key_names(), self._arguments())

    async def _back_off(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

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
