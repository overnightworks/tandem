from collections.abc import Callable

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
    client = make_client(
        f"redis://127.0.0.1:{proxy.port}", retry=Retry(NoBackoff(), 0), socket_timeout=0.3
    )
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
