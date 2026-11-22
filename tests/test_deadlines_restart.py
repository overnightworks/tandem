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


async def test_arm_brings_back_a_key_a_restart_took_and_it_comes_due(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-13."""
    client = make_client(restartable_redis_url)
    deadlines = new_deadlines(client)
    await deadlines.set("room-1", timedelta(0))
    await restart(client)

    assert await deadlines.arm("room-1", timedelta(0)) is True

    assert [due.key for due in await claim_when_due(deadlines)] == ["room-1"]


async def test_recur_after_a_restart_returns_false_and_leaves_the_key_absent_until_arm_repairs_it(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-18, DEADLINE-20, DEADLINE-13."""
    client = make_client(restartable_redis_url)
    deadlines = new_deadlines(client)
    due = await claimed_key(deadlines)
    await restart(client)

    assert await deadlines.recur(due, timedelta(0)) is False
    assert await state_of(client, deadlines, "room-1") is None
    assert await deadlines.arm("room-1", timedelta(0)) is True

    assert [claimed.key for claimed in await claim_when_due(deadlines)] == ["room-1"]
