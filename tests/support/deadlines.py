"""Tools of the deadline tests: Redis's clock, a wait for a due key and a read of a key's state."""

from datetime import timedelta
from typing import cast

from redis.asyncio import Redis

from tandem import Deadlines, Due, Script
from tests.support.waiting import until

TwoPods = tuple[Redis, Deadlines, Deadlines]

KEYS_OF_THE_SCRIPT = "local keys = {due = KEYS[1], claimed = KEYS[2], claim = KEYS[3]}\n"
_STATE_SCRIPT = KEYS_OF_THE_SCRIPT + "return tandem.deadline_state(keys, ARGV[1])"


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
