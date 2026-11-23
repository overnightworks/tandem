from collections.abc import Awaitable, Callable

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import Slots, StoreError
from tests.support.proxy import TcpProxy

Operation = Callable[[Slots], Awaitable[object]]

OPERATIONS: list[Operation] = [
    lambda slots: slots.take({"person-a": 1}),
    lambda slots: slots.release("a-token"),
]


@pytest.mark.parametrize("operation", OPERATIONS)
async def test_a_cut_connection_raises_store_error_as_an_outage_and_a_refusal_as_no_outage(
    operation: Operation,
    proxy: TcpProxy,
    proxied_client: Redis,
    url_denying: Callable[[str], Awaitable[str]],
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
):
    """Spec: STORE-1."""
    cut = new_slots(proxied_client)
    refused = new_slots(make_client(await url_denying("evalsha")))
    proxy.cut()

    with pytest.raises(StoreError) as outage:
        await operation(cut)
    with pytest.raises(StoreError) as refusal:
        await operation(refused)

    assert outage.value.outage is True
    assert refusal.value.outage is False


async def test_a_take_whose_reply_is_lost_is_replayed_to_the_same_token_and_counts_once(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
):
    """Spec: SLOT-6."""
    direct = new_slots(make_client(redis_url))
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 3), socket_timeout=0.3)
    proxy_pod = new_slots(retrying)
    # A cold script loses its reply as NOSCRIPT, before anything ran; the warm-up loads it.
    assert await proxy_pod.take({"warm-up": 1}) is not None
    proxy.drop_replies_of_open_connections()

    token = await proxy_pod.take({"person-a": 2})

    assert token is not None
    assert await direct.take({"person-a": 2}) is not None
    assert await direct.take({"person-a": 2}) is None
    assert await direct.release(token) is True
    assert await direct.take({"person-a": 2}) is not None


async def test_a_release_whose_reply_is_lost_is_replayed_to_false_and_the_hold_is_gone(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
):
    """Spec: SLOT-8."""
    direct = new_slots(make_client(redis_url))
    token = await direct.take({"person-a": 1})
    assert token is not None
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 3), socket_timeout=0.3)
    proxy_pod = new_slots(retrying)
    assert await proxy_pod.release("warm-up") is False  # warms the script
    proxy.drop_replies_of_open_connections()

    released = await proxy_pod.release(token)

    assert released is False
    assert await direct.take({"person-a": 1}) is not None
