from collections.abc import Awaitable, Callable

import pytest
from redis.asyncio import Redis

from tandem import StoreError, WakeChannel
from tests.support.reader import Reader


async def test_a_publish_that_redis_refuses_raises_store_error(
    url_denying: Callable[[str], Awaitable[str]], make_client: Callable[..., Redis], key_prefix: str
):
    """Spec: WAKE-5."""
    channel = WakeChannel(make_client(await url_denying("publish")), key_prefix=key_prefix)

    with pytest.raises(StoreError):
        await channel.publish("orders/7")


async def test_a_subscription_that_redis_refuses_raises_store_error_out_of_the_entry(
    url_denying: Callable[[str], Awaitable[str]], make_client: Callable[..., Redis], key_prefix: str
):
    """Spec: WAKE-27."""
    channel = WakeChannel(make_client(await url_denying("subscribe")), key_prefix=key_prefix)

    with pytest.raises(StoreError):
        async with Reader(channel):
            pass


async def test_credentials_that_redis_rejects_raise_store_error_from_the_entry_and_publish(
    url_with_wrong_password: str, make_client: Callable[..., Redis], key_prefix: str
):
    """Spec: WAKE-5, WAKE-27."""
    channel = WakeChannel(make_client(url_with_wrong_password), key_prefix=key_prefix)

    with pytest.raises(StoreError):
        await channel.publish("orders/7")
    with pytest.raises(StoreError):
        async with Reader(channel):
            pass
