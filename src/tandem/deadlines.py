"""Deadlines: keys that come due on Redis TIME and reach one claimer at a time."""

import math
import secrets
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

from tandem.client import Client
from tandem.keys import DeadlineKeys, deadline_keys
from tandem.script import Script

_MILLISECOND = timedelta(milliseconds=1)
_TOKEN_BYTES = 16  # 128 random bits per claim

_KEYS = "local keys = {due = KEYS[1], claimed = KEYS[2], claim = KEYS[3]}\n"

_SET = _KEYS + "tandem.deadline_set(keys, ARGV[1], ARGV[2])"

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


class Deadlines:
    def __init__(self, redis: Client, *, key_prefix: str, kind: str, claim_for: timedelta) -> None:
        """The deadlines of one `kind` in the app of `key_prefix`; a claim holds for `claim_for`.

        `key_prefix` and `kind` are each one segment (`checked_segment`); the claim's length must be
        positive.
        """
        if not isinstance(claim_for, timedelta) or claim_for <= timedelta(0):
            raise ValueError(f"claim_for must be a positive timedelta, got {claim_for!r}")
        self._redis = redis
        self._keys = deadline_keys(key_prefix, kind)
        self._claim_for_ms = _milliseconds(claim_for)
        self._set = Script(redis, _SET)
        self._cancel = Script(redis, _CANCEL)
        self._claim_due = Script(redis, _CLAIM_DUE)
        self._extend = Script(redis, _EXTEND)
        self._done = Script(redis, _DONE)

    @property
    def keys(self) -> DeadlineKeys:
        """The three key names, for a caller's script that runs `LUA_KIT`'s transitions on them."""
        return self._keys

    async def set(self, key: str, due_in: timedelta) -> None:
        """Make `key` come due `due_in` from now on Redis TIME; a claim on `key` is invalidated."""
        await self._set.run(self._key_names(), [key, _milliseconds(due_in)])

    async def cancel(self, key: str) -> bool:
        """Remove `key`, waiting or claimed; True when there was something to remove."""
        return await self._cancel.run(self._key_names(), [key]) == 1

    async def claim_due(
        self,
        *,
        claimer: str,  # NOSONAR(python:S1172) - the signature names the claimer, no record kept
        limit: int,
    ) -> list[Due]:
        """Claim up to `limit` keys that are due or whose claim expired, each under a fresh token.

        `claimer` names the caller; tandem keeps no record of it.
        """
        if limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit!r}")
        tokens = [secrets.token_hex(_TOKEN_BYTES) for _ in range(limit)]
        reply = await self._claim_due.run(self._key_names(), [limit, self._claim_for_ms, *tokens])
        pairs = [self._text(item) for item in cast(list[object], reply)]
        return [
            Due(key=key, claim=token) for key, token in zip(pairs[::2], pairs[1::2], strict=True)
        ]

    async def extend(self, due: Due) -> bool:
        """Hold the claim for another `claim_for`; False once its token is gone or it expired."""
        reply = await self._extend.run(self._key_names(), [due.key, due.claim, self._claim_for_ms])
        return reply == 1

    async def done(self, due: Due) -> bool:
        """Finish the claim; False once its token is gone."""
        return await self._done.run(self._key_names(), [due.key, due.claim]) == 1

    def _key_names(self) -> list[str]:
        return [self._keys.due, self._keys.claimed, self._keys.claim]

    def _text(self, value: object) -> str:
        return self._redis.get_encoder().decode(value, force=True)


def _milliseconds(span: timedelta) -> int:
    return math.ceil(span / _MILLISECOND)
