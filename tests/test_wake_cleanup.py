import asyncio
from collections.abc import Callable

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import WakeChannel
from tests.conftest import Backoffs
from tests.support.proxy import TcpProxy
from tests.support.reader import Reader
from tests.support.waiting import until


async def _nothing_is_left(
    observer: Redis, key_prefix: str, tasks_before: set[asyncio.Task[object]], proxy: TcpProxy
) -> None:
    channel = f"{key_prefix}:tandem:v1:wake"

    async def subscribers_gone() -> bool:
        return (await observer.pubsub_numsub(channel))[0][1] == 0

    async def connections_gone() -> bool:
        connections = await observer.client_list()
        return not any(connection["name"].startswith(key_prefix) for connection in connections)

    await until(subscribers_gone)
    await until(connections_gone)
    assert asyncio.all_tasks() <= tasks_before | proxy.tasks


async def test_cancelling_while_reading_unsubscribes_and_closes_the_connection(
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    redis_url: str,
):
    tasks_before = asyncio.all_tasks()
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
    assert not reader.running
    await _nothing_is_left(observer, key_prefix, tasks_before, proxy)


async def test_cancelling_during_a_back_off_leaves_no_connection_and_no_task(
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    backoffs: Backoffs,
):
    tasks_before = asyncio.all_tasks()
    client = make_client(f"redis://127.0.0.1:{proxy.port}", retry=Retry(NoBackoff(), 0))
    async with Reader(WakeChannel(client, key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        proxy.cut()
        await backoffs.reached(1)
    assert not reader.running
    await _nothing_is_left(observer, key_prefix, tasks_before, proxy)
