from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Deadlines
from tests.support.deadlines import state_of
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

    assert await deadlines.claim_due(claimer="b", limit=10) == []
    assert await state_of(client, deadlines, "room-1") is None
