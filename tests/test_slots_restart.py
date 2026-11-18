from collections.abc import Callable

from redis.asyncio import Redis

from tandem import Slots
from tests.support.restart import restart


async def test_after_a_redis_restart_takes_are_granted_at_once(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
):
    client = make_client(restartable_redis_url)
    slots = new_slots(client)
    assert await slots.take({"person-a": 1}) is not None
    assert await slots.take({"person-a": 1}) is None

    await restart(client)

    assert await slots.take({"person-a": 1}) is not None
