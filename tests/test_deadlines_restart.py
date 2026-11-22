from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Deadlines
from tests.support.deadlines import claim_when_due, claimed_key, state_of
from tests.support.restart import restart


async def test_a_redis_restart_takes_every_pending_deadline_with_it(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    client = make_client(restartable_redis_url)
    deadlines = new_deadlines(client)
    await deadlines.set("room-1", timedelta(0))
    assert await state_of(client, deadlines, "room-1") is not None

    await restart(client)

    assert await deadlines.claim_due(limit=10) == []
    assert await state_of(client, deadlines, "room-1") is None


async def test_after_a_redis_restart_recur_of_a_claim_is_false_and_arm_brings_both_keys_back(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-13, DEADLINE-18, DEADLINE-20."""
    client = make_client(restartable_redis_url)
    deadlines = new_deadlines(client)
    claimed = await claimed_key(deadlines, "room-claimed")
    await deadlines.set("room-waiting", timedelta(hours=1))

    await restart(client)

    assert await deadlines.recur(claimed, timedelta(0)) is False
    assert await state_of(client, deadlines, "room-claimed") is None
    assert await deadlines.arm("room-claimed", timedelta(0)) is True
    assert await deadlines.arm("room-waiting", timedelta(0)) is True
    assert {due.key for due in await claim_when_due(deadlines)} == {"room-claimed", "room-waiting"}
