import asyncio
from collections.abc import Callable
from typing import cast

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import StoreError, Wake, WakeChannel
from tests.support.host import ScenarioHost
from tests.support.proxy import TcpProxy
from tests.support.reader import Reader


async def _publisher_through(
    proxy: TcpProxy, make_client: Callable[..., Redis], key_prefix: str
) -> WakeChannel:
    """A publisher whose connection is open and whose failures surface at once, as a timeout."""
    client = make_client(proxy.url, retry=Retry(NoBackoff(), 0), socket_timeout=0.3)
    await client.ping()
    return WakeChannel(client, key_prefix=key_prefix)


async def test_a_publish_that_never_reaches_redis_fails_and_wakes_nobody(
    proxy: TcpProxy, make_client: Callable[..., Redis], redis_url: str, key_prefix: str
):
    publisher = await _publisher_through(proxy, make_client, key_prefix)
    direct = make_client(redis_url)
    host = ScenarioHost(direct, key_prefix)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        await host.commit("orders/7", "shipped")
        proxy.drop_requests()

        with pytest.raises(StoreError):
            await publisher.publish("orders/7")

        await WakeChannel(direct, key_prefix=key_prefix).publish("barrier")
        assert await reader.next_wake() == Wake("barrier")
        assert reader.drain() == []
    await host.reread()
    assert host.served == {"orders/7": "shipped"}


async def test_a_publish_whose_reply_is_lost_fails_yet_wakes_and_a_republish_wakes_again(
    proxy: TcpProxy, make_client: Callable[..., Redis], redis_url: str, key_prefix: str
):
    publisher = await _publisher_through(proxy, make_client, key_prefix)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        proxy.drop_replies()

        with pytest.raises(StoreError):
            await publisher.publish("orders/7")

        assert await reader.next_wake() == Wake("orders/7")
        proxy.restore()
        await publisher.publish("orders/7")
        assert await reader.next_wake() == Wake("orders/7")
        assert reader.drain() == []


async def test_a_publish_is_sent_once_although_the_clients_retry_would_send_it_again(
    proxy: TcpProxy, make_client: Callable[..., Redis], redis_url: str, key_prefix: str
):
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 3), socket_timeout=0.3)
    await retrying.ping()
    publisher = WakeChannel(retrying, key_prefix=key_prefix)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        proxy.drop_replies_of_open_connections()

        with pytest.raises(StoreError):
            await publisher.publish("orders/7")

        await WakeChannel(make_client(redis_url), key_prefix=key_prefix).publish("barrier")
        assert [await reader.next_wake(), await reader.next_wake()] == [
            Wake("orders/7"),
            Wake("barrier"),
        ]
        assert reader.drain() == []


async def test_a_single_connection_client_publishes_over_its_own_connection_beside_other_commands(
    make_client: Callable[..., Redis], redis_url: str, key_prefix: str
):
    client = make_client(redis_url, single_connection_client=True, max_connections=1)
    await client.ping()
    publisher = WakeChannel(client, key_prefix=key_prefix)
    addresses = [f"orders/{number}" for number in range(5)]
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()

        await asyncio.gather(client.ping(), *(publisher.publish(address) for address in addresses))

        assert {await reader.next_wake() for _ in addresses} == {Wake(a) for a in addresses}
        assert reader.drain() == []


class _SingleConnectionClientWithoutTheLock:
    """What a redis-py that renamed its single-connection lock presents to WakeChannel."""

    connection = object()

    async def initialize(self) -> _SingleConnectionClientWithoutTheLock:
        return self


async def test_a_single_connection_client_without_the_lock_tandem_relies_on_fails_with_its_name(
    key_prefix: str,
):
    client = cast(Redis, _SingleConnectionClientWithoutTheLock())
    channel = WakeChannel(client, key_prefix=key_prefix)

    with pytest.raises(RuntimeError, match="_single_conn_lock"):
        await channel.publish("orders/7")
