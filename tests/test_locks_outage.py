from collections.abc import Awaitable, Callable

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import Liveness, Locks, StoreError
from tests.support.proxy import TcpProxy

Operation = Callable[[Locks], Awaitable[object]]

OPERATIONS: list[Operation] = [
    lambda locks: locks.acquire("gpu-0"),
    lambda locks: locks.extend("gpu-0", "a-generation"),
    lambda locks: locks.release("gpu-0", "a-generation"),
    lambda locks: locks.state("gpu-0"),
]


@pytest.mark.parametrize("operation", OPERATIONS)
async def test_a_cut_connection_raises_store_error_as_an_outage_and_a_refusal_raises_it_as_none(
    operation: Operation,
    proxy: TcpProxy,
    url_denying: Callable[[str], Awaitable[str]],
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
):
    cut = new_locks(make_client(proxy.url, retry=Retry(NoBackoff(), 0)))
    refused = new_locks(make_client(await url_denying("evalsha")))
    proxy.cut()

    with pytest.raises(StoreError) as outage:
        await operation(cut)
    with pytest.raises(StoreError) as refusal:
        await operation(refused)

    assert outage.value.outage is True
    assert refusal.value.outage is False


async def test_an_acquire_whose_reply_is_lost_is_replayed_to_the_same_generation(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
):
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 3), socket_timeout=0.3)
    await retrying.ping()
    proxy_pod = new_locks(retrying)
    proxy.drop_replies_of_open_connections()

    generation = await proxy_pod.acquire("gpu-0")

    assert generation is not None
    assert (await new_locks(make_client(redis_url)).state("gpu-0")).generation == generation


async def test_a_release_whose_reply_is_lost_is_replayed_to_false_and_the_lock_is_gone(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
):
    direct = new_locks(make_client(redis_url))
    generation = await direct.acquire("gpu-0")
    assert generation is not None
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 3), socket_timeout=0.3)
    await retrying.ping()
    proxy.drop_replies_of_open_connections()

    released = await new_locks(retrying).release("gpu-0", generation)

    assert released is False
    assert (await direct.state("gpu-0")).liveness is Liveness.GONE
