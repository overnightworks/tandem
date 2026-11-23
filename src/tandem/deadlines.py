"""Deadlines: keys that come due on Redis TIME and reach one claimer at a time."""

import asyncio
import logging
import math
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import NoReturn, Protocol, cast

from logline import StepTimer, unit_trace

from tandem.client import Client
from tandem.errors import BACKOFF_FIRST_SECONDS, StoreError, failure_reason, next_backoff
from tandem.keys import DeadlineKeys, deadline_keys
from tandem.script import Script

logger: logging.Logger = logging.getLogger(__name__)

_MILLISECOND = timedelta(milliseconds=1)
_TOKEN_BYTES = 16  # 128 random bits per claim
_BATCH = 8  # keys claimed at once, so a claim does not run out while its batch waits
_CLAIM_EXTENSIONS_PER_CLAIM = 3

_KEYS = "local keys = {due = KEYS[1], claimed = KEYS[2], claim = KEYS[3]}\n"

_SET = _KEYS + "tandem.deadline_set(keys, ARGV[1], ARGV[2])"

_ARM = _KEYS + "return tandem.deadline_arm(keys, ARGV[1], ARGV[2])"

# ARGV: key, token, the next due in ms. The token is checked as `_DONE` checks it; `deadline_set`
# then ends the claim and arms the key in the same script, so the key is never absent.
_RECUR = (
    _KEYS
    + """
if redis.call('HGET', keys.claim, ARGV[1]) ~= ARGV[2] then
  return 0
end
tandem.deadline_set(keys, ARGV[1], ARGV[3])
return 1
"""
)

_CANCEL = _KEYS + "return tandem.deadline_cancel(keys, ARGV[1])"

# ARGV: the most to claim, the claim's length in ms, then one fresh token per possible claim. Due
# keys come first, then claims that expired; each moves into the claimed ZSET under its new token.
_CLAIM_DUE = (
    _KEYS
    + """
local limit = tonumber(ARGV[1])
local now_ms = tandem.now_ms()
local expires_ms = now_ms + tonumber(ARGV[2])
local issued = 0
local reply = {}
local function claim(key)
  issued = issued + 1
  local token = ARGV[2 + issued]
  redis.call('ZREM', keys.due, key)
  redis.call('ZADD', keys.claimed, expires_ms, key)
  redis.call('HSET', keys.claim, key, token)
  reply[#reply + 1] = key
  reply[#reply + 1] = token
end
for _, key in ipairs(redis.call('ZRANGEBYSCORE', keys.due, '-inf', now_ms, 'LIMIT', 0, limit)) do
  claim(key)
end
if issued < limit then
  local room = limit - issued
  local expired = redis.call('ZRANGEBYSCORE', keys.claimed, '-inf', now_ms, 'LIMIT', 0, room)
  for _, key in ipairs(expired) do
    claim(key)
  end
end
return reply
"""
)

# ARGV: key, token, the claim's length in ms. A claim holds while its token is current and has not
# expired.
_EXTEND = (
    _KEYS
    + """
local claim_until_ms = redis.call('ZSCORE', keys.claimed, ARGV[1])
local now_ms = tandem.now_ms()
if redis.call('HGET', keys.claim, ARGV[1]) ~= ARGV[2] or not claim_until_ms
    or tonumber(claim_until_ms) <= now_ms then
  return 0
end
redis.call('ZADD', keys.claimed, now_ms + tonumber(ARGV[3]), ARGV[1])
return 1
"""
)

_DONE = (
    _KEYS
    + """
if redis.call('HGET', keys.claim, ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZREM', keys.claimed, ARGV[1])
redis.call('HDEL', keys.claim, ARGV[1])
return 1
"""
)


@dataclass(frozen=True)
class Due:
    key: str
    claim: str  # fresh random token of this claim


class DeadlineQueue:
    """The deadline transitions over any three keys; `Deadlines` names them by `kind`, `Leases`
    uses them for its lapse queue."""

    def __init__(self, redis: Client, keys: DeadlineKeys, claim_for: timedelta) -> None:
        self._redis = redis
        self._keys = keys
        self._claim_for = checked_span(claim_for, argument="claim_for")
        self._set = Script(redis, _SET)
        self._arm = Script(redis, _ARM)
        self._recur = Script(redis, _RECUR)
        self._cancel = Script(redis, _CANCEL)
        self._claim_due = Script(redis, _CLAIM_DUE)
        self._extend = Script(redis, _EXTEND)
        self._done = Script(redis, _DONE)

    @property
    def claim_for(self) -> timedelta:
        """How long a claim holds, which the serving loop reads for its extensions."""
        return self._claim_for

    @property
    def keys(self) -> DeadlineKeys:
        """The three key names, for a caller's script that runs `LUA_KIT`'s transitions on them."""
        return self._keys

    async def set(self, key: str, due_in: timedelta) -> None:
        """Make `key` come due `due_in` from now on Redis TIME; a claim on `key` is invalidated."""
        await self._set.run(self._key_names(), [key, milliseconds(due_in)])

    async def arm(self, key: str, due_in: timedelta) -> bool:
        """Make `key` come due `due_in` from now on Redis TIME only when it neither waits nor is
        claimed; True when this call's script armed it. A retry cannot tell a replay from a new
        call."""
        return await self._arm.run(self._key_names(), [key, milliseconds(due_in)]) == 1

    async def recur(self, due: Due, due_in: timedelta) -> bool:
        """Finish the claim and make its key come due `due_in` from now in one step; False, with
        nothing changed, once the claim's token is gone."""
        reply = await self._recur.run(self._key_names(), [due.key, due.claim, milliseconds(due_in)])
        return reply == 1

    async def cancel(self, key: str) -> bool:
        """Remove `key`, waiting or claimed; True when there was something to remove."""
        return await self._cancel.run(self._key_names(), [key]) == 1

    async def claim_due(self, *, limit: int) -> list[Due]:
        """Claim up to `limit` due keys or keys whose claim expired, each under a fresh token."""
        if limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit!r}")
        tokens = [secrets.token_hex(_TOKEN_BYTES) for _ in range(limit)]
        reply = await self._claim_due.run(
            self._key_names(), [limit, milliseconds(self._claim_for), *tokens]
        )
        pairs = [self._text(item) for item in cast(list[object], reply)]
        return [
            Due(key=key, claim=token) for key, token in zip(pairs[::2], pairs[1::2], strict=True)
        ]

    async def extend(self, due: Due) -> bool:
        """Hold the claim for another `claim_for`; False once its token is gone or it expired."""
        reply = await self._extend.run(
            self._key_names(), [due.key, due.claim, milliseconds(self._claim_for)]
        )
        return reply == 1

    async def done(self, due: Due) -> bool:
        """Finish the claim; False once its token is gone."""
        return await self._done.run(self._key_names(), [due.key, due.claim]) == 1

    def _key_names(self) -> list[str]:
        return [self._keys.due, self._keys.claimed, self._keys.claim]

    def _text(self, value: object) -> str:
        return self._redis.get_encoder().decode(value, force=True)


class Deadlines(DeadlineQueue):
    def __init__(self, redis: Client, *, key_prefix: str, kind: str, claim_for: timedelta) -> None:
        """The deadlines of one `kind` in the app of `key_prefix`; a claim holds for `claim_for`.

        `key_prefix` and `kind` are each one segment (`checked_segment`); the claim's length must be
        positive.
        """
        super().__init__(redis, deadline_keys(key_prefix, kind), claim_for)
        self._kind = kind

    async def serve_due(
        self,
        handle: Callable[[Due], Awaitable[None]],
        *,
        every: timedelta,
        reconcile: Callable[[], Awaitable[None]] | None = None,
    ) -> NoReturn:
        """Hand every due key to `handle`, at least once, until cancelled.

        Each `every`, `reconcile` runs when given, where a host arms its keys, and the due keys are
        claimed in small batches until none is due. While `handle` runs its claim is extended every
        `claim_for / 3`; an extension that Redis fails or refuses stops, and `handle` keeps running,
        its line saying `claim_kept` false when the claim was lost or may have run out. Any other
        exception from an extension ends the loop. `done` follows a `handle` that returns; one that
        ends the claim itself (`recur`, `done`, `cancel`) leaves that standing. One that raises is
        logged and its key returns when its claim runs out, unless it ended the claim first. An
        outage or a refusal of Redis, from `reconcile`, a claim or a `done`, is ridden out with a
        back-off; any other exception from `reconcile` ends the loop. Cancelling the loop cancels
        the running `handle` and leaves its claim to run out.
        """
        await serve_queue(
            self,
            handle,
            every=every,
            before_claim=reconcile,
            back_off=self._back_off,
            lines=_DeadlineLines(self._kind),
        )

    async def _back_off(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class ServingLines(Protocol):
    """What a serving loop writes: each owner names its own events, which logline's check reads as
    literals. The `fields` of a due key come from the owner's `due_fields` and identify it."""

    def round_counts(self, claimed: int, failed: int) -> dict[str, int]: ...

    def round_timer(self) -> StepTimer: ...

    def handling_timer(self, fields: Mapping[str, str]) -> StepTimer: ...

    def handler_failed(self, fields: Mapping[str, str], error: Exception) -> None: ...


class _DeadlineLines:
    def __init__(self, kind: str) -> None:
        self._kind = kind

    def round_counts(self, claimed: int, failed: int) -> dict[str, int]:
        return {"deadline_count": claimed, "failed_count": failed}

    def round_timer(self) -> StepTimer:
        return StepTimer(
            "deadline_round.ended", logger, deadline_kind=self._kind, **self.round_counts(0, 0)
        )

    def handling_timer(self, fields: Mapping[str, str]) -> StepTimer:
        timer = StepTimer("deadline_handling.ended", logger, deadline_kind=self._kind)
        timer.add(**fields)
        return timer

    def handler_failed(self, fields: Mapping[str, str], error: Exception) -> None:
        logger.error(
            "deadline_handler.failed",
            extra={"error_type": type(error).__name__, "deadline_kind": self._kind, **fields},
            exc_info=error,
        )


def _loop_time() -> float:
    return asyncio.get_running_loop().time()


@dataclass
class _Claim:
    """What a handling knows of its claim: when it surely still runs, in loop time, which is just
    before the last claim or extension that succeeded plus `claim_for`; whether an extension
    answered that it is gone; and whether extensions go on. A refusal or an outage leaves the time
    as it is: a refusal changes nothing and an outage may have applied the extension."""

    surely_until: float
    lost: bool = False
    extending: bool = True

    def report_unless_kept(self, timer: StepTimer) -> None:
        """Mark the handling's line when the claim was lost or may have run out by now."""
        if self.lost or _loop_time() >= self.surely_until:
            timer.degraded()
            timer.add(claim_kept=False)


@dataclass
class _RoundTally:
    """What one round did, for its line."""

    claimed: int = 0
    failed: int = 0

    def end(self, timer: StepTimer, lines: ServingLines, *, cancelled: bool = False) -> None:
        timer.add(**lines.round_counts(self.claimed, self.failed))
        if cancelled:
            timer.end_cancelled()
        else:
            timer.end()


def _store_error_in(failure: BaseException) -> StoreError | None:
    """The `StoreError`, which names Redis, that `failure` is or chains or groups; else None."""
    unvisited, visited = [failure], set[int]()
    while unvisited:
        error = unvisited.pop()
        if isinstance(error, StoreError):
            return error
        if id(error) in visited:
            continue
        visited.add(id(error))
        unvisited.extend(link for link in (error.__cause__, error.__context__) if link is not None)
        if isinstance(error, BaseExceptionGroup):
            unvisited.extend(error.exceptions)
    return None


async def serve_queue(
    queue: DeadlineQueue,
    handle: Callable[[Due], Awaitable[None]],
    *,
    every: timedelta,
    before_claim: Callable[[], Awaitable[None]] | None,
    back_off: Callable[[float], Awaitable[None]],
    lines: ServingLines,
    due_fields: Callable[[Due], Mapping[str, str]] | None = None,
) -> NoReturn:
    """Serve what comes due on `queue` until cancelled: the loop of `serve_due` and `serve_lapses`.

    Each round runs `before_claim`, then claims batches until none is due and hands each key to
    `handle`. A round fails when a `StoreError`, or an exception chaining one, leaves the
    `before_claim`, a claim or a `done`; the loop backs off and goes on. Any other exception that
    is not `handle`'s ends the loop.
    """
    pause = checked_span(every, argument="every").total_seconds()
    serving = _Serving(queue, handle, before_claim, lines, due_fields)
    backoff = BACKOFF_FIRST_SECONDS
    failed_runs = 0
    while True:
        if await serving.round(failed_runs):
            failed_runs += 1
            await back_off(backoff)
            backoff = next_backoff(backoff)
            continue
        failed_runs = 0
        backoff = BACKOFF_FIRST_SECONDS
        await asyncio.sleep(pause)


class _Serving:
    def __init__(
        self,
        queue: DeadlineQueue,
        handle: Callable[[Due], Awaitable[None]],
        before_claim: Callable[[], Awaitable[None]] | None,
        lines: ServingLines,
        due_fields: Callable[[Due], Mapping[str, str]] | None,
    ) -> None:
        self._queue = queue
        self._handle = handle
        self._claim_seconds = queue.claim_for.total_seconds()
        self._before_claim = before_claim
        self._lines = lines
        self._due_fields = due_fields

    async def round(self, failed_runs: int) -> bool:
        """One round in a trace of its own; True when it failed on Redis.

        It writes a line when it claimed a key, when it is the first failed round of a run and when
        it ends such a run; `failed_runs` is the number of failed rounds just before it.
        """
        timer = self._lines.round_timer()
        tally = _RoundTally()
        with unit_trace(None):
            try:
                if self._before_claim is not None:
                    await self._before_claim()
                claims = await self._claim_batch()
                if not claims and not failed_runs:
                    return False
                while claims:
                    await self._handle_batch(claims, tally)
                    claims = await self._claim_batch()
            except Exception as error:
                store_error = _store_error_in(error)
                if store_error is None:
                    raise
                if not failed_runs:
                    timer.handled()
                    timer.fail(failure_reason(store_error))
                    tally.end(timer, self._lines)
                return True
            except asyncio.CancelledError:
                if tally.claimed:
                    tally.end(timer, self._lines, cancelled=True)
                raise
            if failed_runs:
                timer.add(failed_run_count=failed_runs)
            tally.end(timer, self._lines)
        return False

    def _fields_of(self, due: Due) -> Mapping[str, str]:
        return {} if self._due_fields is None else self._due_fields(due)

    async def _claim_batch(self) -> dict[Due, _Claim]:
        claimed_at = _loop_time()
        batch = await self._queue.claim_due(limit=_BATCH)
        return {due: _Claim(surely_until=claimed_at + self._claim_seconds) for due in batch}

    async def _handle_batch(self, claims: dict[Due, _Claim], tally: _RoundTally) -> None:
        extender = asyncio.create_task(self._extend_while_pending(claims))
        handling = asyncio.create_task(self._handle_each(claims, tally))
        try:
            await asyncio.wait({extender, handling}, return_when=asyncio.FIRST_COMPLETED)
            if extender.done():
                extender.result()  # it only ends by raising: its failure ends the loop
            await handling
        finally:
            extender.cancel()
            handling.cancel()
            await asyncio.gather(extender, handling, return_exceptions=True)

    async def _handle_each(self, claims: dict[Due, _Claim], tally: _RoundTally) -> None:
        for due in tuple(claims):
            await self._handle_one(due, claims, tally)

    async def _handle_one(self, due: Due, claims: dict[Due, _Claim], tally: _RoundTally) -> None:
        tally.claimed += 1
        claim = claims[due]
        fields = self._fields_of(due)
        failure = None
        with unit_trace(None):
            try:
                async with self._lines.handling_timer(fields) as timer:
                    try:
                        await self._handle(due)
                    except Exception:
                        claim.report_unless_kept(timer)
                        raise
                    claim.report_unless_kept(timer)
            except Exception as error:
                failure = error
                if _store_error_in(error) is None:
                    self._lines.handler_failed(fields, error)
        del claims[due]
        if failure is not None:
            tally.failed += 1
            return
        await self._queue.done(due)

    async def _extend_while_pending(self, claims: dict[Due, _Claim]) -> None:
        while True:
            await asyncio.sleep(self._claim_seconds / _CLAIM_EXTENSIONS_PER_CLAIM)
            for due, claim in tuple(claims.items()):
                if claim.extending:
                    await self._extend_claim(due, claim)

    async def _extend_claim(self, due: Due, claim: _Claim) -> None:
        """Extend the claim of `due`; the first `StoreError` or `False` ends its extensions."""
        extension_began_at = _loop_time()
        try:
            extended = await self._queue.extend(due)
        except StoreError:
            claim.extending = False
            return
        claim.extending = extended
        if extended:
            claim.surely_until = extension_began_at + self._claim_seconds
        else:
            claim.lost = True


def checked_span(value: timedelta, *, argument: str) -> timedelta:
    """Return `value` when it is a positive timedelta, else raise ValueError naming the argument."""
    if not isinstance(value, timedelta) or value <= timedelta(0):
        raise ValueError(f"{argument} must be a positive timedelta, got {value!r}")
    return value


def milliseconds(span: timedelta) -> int:
    return math.ceil(span / _MILLISECOND)
