import asyncio
import signal
import sys
from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Locks
from tests.support.deadlines import redis_ms
from tests.support.locks import expiry_of
from tests.support.waiting import until, within

LASTS_MS = 1500


async def test_the_lock_of_a_killed_holder_is_refused_before_lasts_and_given_after_it(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
):
    client = make_client(redis_url)
    locks = new_locks(client, lasts=timedelta(milliseconds=LASTS_MS))
    holder = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.support.lock_process",
        redis_url,
        key_prefix,
        "gpu-0",
        str(LASTS_MS),
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        assert holder.stdout is not None
        assert (await within(holder.stdout.readline())).decode().strip() == "ACQUIRED"
        expiry = await expiry_of(observer, key_prefix, "gpu-0")
        holder.send_signal(signal.SIGKILL)

        assert await locks.acquire("gpu-0") is None

        async def given() -> bool:
            return await locks.acquire("gpu-0") is not None

        await until(given)
        assert await redis_ms(client) >= expiry
    finally:
        if holder.returncode is None:
            holder.kill()
        await holder.wait()
