"""A Redis restart as a test drives it: SHUTDOWN, then a wait until the server answers again."""

import contextlib

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from tests.support.waiting import until


async def restart(client: Redis) -> None:
    """Shut the server down without saving and return once it answers again."""
    with contextlib.suppress(RedisConnectionError):
        await client.execute_command("SHUTDOWN", "NOSAVE")

    async def answers() -> bool:
        try:
            return bool(await client.ping())
        except RedisError:
            return False

    await until(answers)
