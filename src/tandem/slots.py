"""Slots: a counted hold, at most N live holders per pool across pods, expiring on Redis TIME."""

import logging
import secrets
from collections.abc import Mapping
from datetime import timedelta

from logline import StepTimer

from tandem.client import Client
from tandem.deadlines import checked_span, milliseconds
from tandem.keys import slot_key
from tandem.script import Script

logger: logging.Logger = logging.getLogger(__name__)

_TOKEN_BYTES = 16  # 128 random bits per hold

# ARGV: `lasts` in ms, the hold's random part, then each pool followed by its cap. The member of a
# hold is `cjson.encode` of {random part, pool...}; it is the token the host keeps. A replay finds
# its member and replies the same token. Replies the token, or nil when any pool is full.
_TAKE = """
local now_ms = tandem.now_ms()
local lasts_ms = tonumber(ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
local member = {ARGV[2]}
local caps = {}
for index = 3, #ARGV, 2 do
  member[#member + 1] = ARGV[index]
  caps[ARGV[index]] = tonumber(ARGV[index + 1])
end
local token = cjson.encode(member)
if redis.call('ZSCORE', KEYS[1], token) then
  return token
end
local held = {}
for _, holder in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local pools = cjson.decode(holder)
  for index = 2, #pools do
    held[pools[index]] = (held[pools[index]] or 0) + 1
  end
end
for pool, cap in pairs(caps) do
  if (held[pool] or 0) >= cap then
    return nil
  end
end
redis.call('ZADD', KEYS[1], now_ms + lasts_ms, token)
redis.call('PEXPIRE', KEYS[1], lasts_ms)
return token
"""

# ARGV: the token. Only a live hold ends; an expired one is removed on the way.
_RELEASE = """
local expires_ms = redis.call('ZSCORE', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
if expires_ms and tonumber(expires_ms) > tandem.now_ms() then
  return 1
end
return 0
"""


class Slots:
    def __init__(self, redis: Client, *, key_prefix: str, family: str, lasts: timedelta) -> None:
        """The slots of one `family` in the app of `key_prefix`.

        A hold lasts `lasts` after its take. Every pod of a family passes the same `lasts`.
        `key_prefix` and `family` are each one segment (`checked_segment`) and `lasts` is a positive
        `timedelta`.
        """
        self._redis = redis
        self._family = family
        self._lasts_ms = milliseconds(checked_span(lasts, argument="lasts"))
        self._key = slot_key(key_prefix, family)
        self._take = Script(redis, _TAKE)
        self._release = Script(redis, _RELEASE)

    async def take(self, limits: Mapping[str, int]) -> str | None:
        """Hold one slot in every pool of `limits`, which maps a pool to its cap, or in none.

        Returns the token, which is opaque: the host keeps it, passes it back to `release` unchanged
        and never logs it, as it embeds the pools. Returns None when any pool is at its cap, and
        never waits. A cap of 0 refuses every take. A reply lost under the client's `Retry` replays
        the script with the same random part, which returns the same token, so the hold counts once.
        After a Redis restart takes are granted at once, so a cap can be exceeded by the lost holds
        for at most `lasts`.
        """
        _check_limits(limits)
        arguments: list[str | int | bytes] = [self._lasts_ms, secrets.token_hex(_TOKEN_BYTES)]
        for pool, cap in limits.items():
            arguments += [pool, cap]
        async with self._step("slot_take.ended"):
            reply = await self._take.run([self._key], arguments)
        return None if reply is None else self._redis.get_encoder().decode(reply, force=True)

    async def release(self, token: str) -> bool:
        """End the hold of `token` in all its pools; True only when the hold was live.

        An unknown or expired token returns False. A reply lost under the client's `Retry` replays a
        release that succeeded as False.
        """
        async with self._step("slot_release.ended"):
            reply = await self._release.run([self._key], [token])
        return reply == 1

    def _step(self, event: str) -> StepTimer:
        return StepTimer(event, logger, slot_family=self._family)


def _check_limits(limits: Mapping[str, int]) -> None:
    if not limits:
        raise ValueError("limits must name at least one pool")
    for pool, cap in limits.items():
        if not isinstance(cap, int) or isinstance(cap, bool) or cap < 0:
            raise ValueError(
                f"limits must map each pool to a count of 0 or more, got {pool!r}: {cap!r}"
            )
