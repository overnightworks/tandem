import contextlib
from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from tandem import Deadlines
from tests.support.deadlines import state_of
from tests.support.waiting import until


async def test_a_redis_restart_takes_every_pending_deadline_with_it(
    restartable_redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    client = make_client(restartable_redis_url)
    deadlines = new_deadlines(client)
    await deadlines.set("room-1", timedelta(0))
    assert await state_of(client, deadlines, "room-1") is not None

    with contextlib.suppress(RedisConnectionError):
        await client.execute_command("SHUTDOWN", "NOSAVE")

    async def answers() -> bool:
        try:
            return bool(await client.ping())
        except RedisError:
            return False

    await until(answers)
    assert await deadlines.claim_due(claimer="b", limit=10) == []
    assert await state_of(client, deadlines, "room-1") is None
