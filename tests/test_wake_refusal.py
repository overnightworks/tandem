from collections.abc import Awaitable, Callable

import pytest
from redis.asyncio import Redis

from tandem import StoreError, WakeChannel
from tests.support.reader import Reader


async def test_a_publish_that_redis_refuses_raises_store_error(
    url_denying: Callable[[str], Awaitable[str]], make_client: Callable[..., Redis], key_prefix: str
):
    channel = WakeChannel(make_client(await url_denying("publish")), key_prefix=key_prefix)

    with pytest.raises(StoreError):
        await channel.publish("orders/7")


async def test_a_subscription_that_redis_refuses_raises_store_error_from_received(
    url_denying: Callable[[str], Awaitable[str]], make_client: Callable[..., Redis], key_prefix: str
):
    channel = WakeChannel(make_client(await url_denying("subscribe")), key_prefix=key_prefix)

    async with Reader(channel) as reader:
        with pytest.raises(StoreError):
            await reader.next()
