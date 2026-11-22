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


# The two cells below flush instead of restarting: Docker doubles the restart delay of the
# server's container with every restart, so each further restart slows the later ones.


async def test_arm_brings_back_a_key_the_server_lost_and_it_comes_due(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-13."""
    client = make_client(restartable_redis_url)
    deadlines = new_deadlines(client)
    await deadlines.set("room-1", timedelta(0))
    await client.flushall()

    assert await deadlines.arm("room-1", timedelta(0)) is True

    assert [due.key for due in await claim_when_due(deadlines)] == ["room-1"]


async def test_recur_after_the_server_lost_its_keys_returns_false_until_arm_repairs_the_key(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-18, DEADLINE-20, DEADLINE-13."""
    client = make_client(restartable_redis_url)
    deadlines = new_deadlines(client)
    due = await claimed_key(deadlines)
    await client.flushall()

    assert await deadlines.recur(due, timedelta(0)) is False
    assert await state_of(client, deadlines, "room-1") is None
    assert await deadlines.arm("room-1", timedelta(0)) is True

    assert [claimed.key for claimed in await claim_when_due(deadlines)] == ["room-1"]
