"""Locks: a hold on a name that one generation owns, started only when none is held."""

import logging
import secrets
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

from logline import StepTimer

from tandem.client import Client
from tandem.deadlines import checked_span, milliseconds
from tandem.keys import LockKeys, lock_keys
from tandem.leases import Liveness
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
            reply = await self._extend.run(self._key_names(), self._arguments(name, generation))
        return reply == 1

    async def release(self, name: str, generation: str) -> bool:
        """End the live lock of `generation`; else change nothing.

        A reply lost under the client's `Retry` replays a release that succeeded as False.
        """
        async with self._step("lock_release.ended"):
            reply = await self._release.run(self._key_names(), self._arguments(name, generation))
        return reply == 1

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

    def _step(self, event: str) -> StepTimer:
        return StepTimer(event, logger, lock_family=self._family)

    def _key_names(self) -> list[str]:
        return [self._keys.epoch, self._keys.expiry, self._keys.generation]

    def _arguments(self, *rest: str | int) -> list[str | int | bytes]:
        return [self._lasts_ms, *rest]

    def _text(self, value: object) -> str:
        return self._redis.get_encoder().decode(value, force=True)
