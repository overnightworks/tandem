import asyncio
from collections.abc import Callable

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import WakeChannel
from tandem.keys import wake_channel
from tests.support.backoffs import BackoffRecordingChannel, Backoffs
from tests.support.proxy import TcpProxy
from tests.support.reader import Reader
from tests.support.waiting import BOUND_SECONDS, until, within


def _no_task_is_left(tasks_before: set[asyncio.Task[object]], proxy: TcpProxy) -> None:
    # What leaving the context awaits is gone at once; only Redis's own bookkeeping lags.
    assert asyncio.all_tasks() <= tasks_before | proxy.tasks


async def _nothing_is_left(
    observer: Redis, key_prefix: str, tasks_before: set[asyncio.Task[object]], proxy: TcpProxy
) -> None:
    channel = wake_channel(key_prefix)
    _no_task_is_left(tasks_before, proxy)

    async def subscribers_gone() -> bool:
        return (await observer.pubsub_numsub(channel))[0][1] == 0

    async def connections_gone() -> bool:
        connections = await observer.client_list()
        return not any(connection["name"].startswith(key_prefix) for connection in connections)

    await until(subscribers_gone)
    await until(connections_gone)


async def test_cancelling_while_reading_unsubscribes_and_closes_the_connection(
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    redis_url: str,
):
    tasks_before = asyncio.all_tasks()
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resynced()
    assert not reader.running
    await _nothing_is_left(observer, key_prefix, tasks_before, proxy)


async def test_cancelling_during_a_back_off_leaves_no_connection_and_no_task(
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    held_backoffs: Backoffs,
):
    tasks_before = asyncio.all_tasks()
    client = make_client(proxy.url, retry=Retry(NoBackoff(), 0))
    channel = BackoffRecordingChannel(client, key_prefix=key_prefix, backoffs=held_backoffs)
    async with Reader(channel) as reader:
        await reader.next_resynced()
        proxy.cut()
        await held_backoffs.reached(1)
    assert not reader.running
    await _nothing_is_left(observer, key_prefix, tasks_before, proxy)


@pytest.mark.parametrize("failing_handler", ["on_resync", "on_wake"])
async def test_a_handler_failure_leaves_no_subscriber_no_connection_and_no_task(
    failing_handler: str,
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    redis_url: str,
):
    def fail(*_: object) -> None:
        raise RuntimeError("the handler failed")

    tasks_before = asyncio.all_tasks()
    channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    failed = asyncio.Event()
    following = channel.following(
        on_resync=fail if failing_handler == "on_resync" else lambda: None,
        on_wake=fail,
        on_failed=lambda error: failed.set(),
        ready_within=BOUND_SECONDS,
    )

    if failing_handler == "on_resync":
        with pytest.raises(RuntimeError):
            async with following:
                pytest.fail("the entry returned although on_resync raised")
    else:
        publisher_client = make_client(redis_url)
        async with following:
            await WakeChannel(publisher_client, key_prefix=key_prefix).publish("orders/7")
            await within(failed.wait())
        _no_task_is_left(tasks_before, proxy)
        await publisher_client.aclose()

    await _nothing_is_left(observer, key_prefix, tasks_before, proxy)
