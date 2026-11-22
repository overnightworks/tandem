import asyncio
from collections.abc import Callable

from redis.asyncio import Redis

from tests.support.restart import restart

RESTARTS = 8
# Well above a supervisor-loop restart (tens of milliseconds) and below the sixth delay of Docker's
# restart policy (3.2 s), which doubles from 100 ms with every restart.
WAIT_BOUND_SECONDS = 2.0


async def test_consecutive_restarts_each_wait_under_the_bound(
    restartable_redis_url: str, make_client: Callable[..., Redis]
):
    client = make_client(restartable_redis_url)
    loop = asyncio.get_running_loop()
    waits: list[float] = []
    for _ in range(RESTARTS):
        started = loop.time()
        await restart(client)
        waits.append(loop.time() - started)

    assert max(waits) <= WAIT_BOUND_SECONDS, f"restart waits in seconds: {waits}"
