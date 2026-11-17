from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Deadlines
from tests.support.backends import Backend
from tests.support.deadlines import claim_when_due, redis_ms, state_of, until_redis_ms

DUE_IN = timedelta(milliseconds=300)
SHORT_CLAIM = timedelta(milliseconds=300)


@pytest.mark.parametrize("decode_responses", [False, True])
async def test_a_deadline_is_claimed_once_with_a_fresh_token_and_not_before_it_is_due(
    backend: Backend, new_deadlines: Callable[..., Deadlines], decode_responses: bool
):
    setter = new_deadlines(backend.new_client(decode_responses=decode_responses))
    claimer = new_deadlines(backend.new_client(decode_responses=decode_responses))
    clock = backend.new_client()
    before_set = await redis_ms(clock)
    await setter.set("room-1", DUE_IN)

    assert await claimer.claim_due(limit=10) == []
    claimed = await claim_when_due(claimer)

    assert await redis_ms(clock) >= before_set + 300
    assert [due.key for due in claimed] == ["room-1"]
    assert len(claimed[0].claim) >= 32
    assert await claimer.claim_due(limit=10) == []


async def test_a_claim_that_runs_out_passes_the_key_to_another_claimer_and_voids_the_old_token(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    client = make_client(redis_url)
    first = new_deadlines(client, claim_for=SHORT_CLAIM)
    second = new_deadlines(make_client(redis_url), claim_for=SHORT_CLAIM)
    await first.set("room-1", timedelta(0))
    (old,) = await claim_when_due(first)
    await until_redis_ms(client, await redis_ms(client) + 300)

    assert await first.extend(old) is False
    (new,) = await claim_when_due(second)

    assert new.key == old.key
    assert new.claim != old.claim
    assert await first.extend(old) is False
    assert await first.done(old) is False
    assert await state_of(client, second, "room-1") is not None
    assert await second.done(new) is True


async def test_a_claim_that_is_extended_keeps_the_key_from_other_claimers(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    client = make_client(redis_url)
    deadlines = new_deadlines(client, claim_for=timedelta(seconds=30))
    await deadlines.set("room-1", timedelta(0))
    (due,) = await claim_when_due(deadlines)
    before_extend = await redis_ms(client)

    assert await deadlines.extend(due) is True

    state = await state_of(client, deadlines, "room-1")
    assert state is not None
    assert state[0] == "claimed"
    assert state[1] >= before_extend + 30_000
    assert await deadlines.claim_due(limit=10) == []


async def test_claim_due_takes_at_most_the_limit_each_under_a_distinct_token_and_leaves_the_rest(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    deadlines = new_deadlines(make_client(redis_url))
    keys = [f"room-{number}" for number in range(5)]
    for key in keys:
        await deadlines.set(key, timedelta(0))

    first = await claim_when_due(deadlines, limit=3)
    rest = await deadlines.claim_due(limit=10)

    assert len(first) == 3
    assert len({due.claim for due in first}) == 3
    assert sorted(due.key for due in [*first, *rest]) == keys
    assert len(rest) == 2


@pytest.mark.parametrize(
    "limit",
    [0, -1],
)
async def test_claim_due_without_room_for_a_claim_fails_loud(
    backend: Backend,
    new_deadlines: Callable[..., Deadlines],
    limit: int,
):
    deadlines = new_deadlines(backend.new_client())

    with pytest.raises(ValueError, match="limit"):
        await deadlines.claim_due(limit=limit)
