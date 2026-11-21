from collections.abc import Awaitable, Callable

import pytest
from logline import Outcome, current_traceparent, unit_trace
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import StoreError, Wake, WakeChannel
from tests.support.host import ScenarioHost
from tests.support.logs import LogLines
from tests.support.proxy import TcpProxy
from tests.support.reader import Reader
from tests.support.waiting import within


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
    """Spec: WAKE-6."""
    publisher = await _publisher_through(proxy, make_client, key_prefix)
    direct = make_client(redis_url)
    host = ScenarioHost(direct, key_prefix)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resynced()
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
    """Spec: WAKE-4, WAKE-6."""
    publisher = await _publisher_through(proxy, make_client, key_prefix)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resynced()
        proxy.drop_replies()

        with pytest.raises(StoreError):
            await publisher.publish("orders/7")

        assert await reader.next_wake() == Wake("orders/7")
        proxy.restore()
        await publisher.publish("orders/7")
        assert await reader.next_wake() == Wake("orders/7")
        assert reader.drain() == []


async def test_a_publish_whose_reply_is_lost_is_sent_again_under_the_clients_retry(
    proxy: TcpProxy, make_client: Callable[..., Redis], redis_url: str, key_prefix: str
):
    """Spec: WAKE-4."""
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 3), socket_timeout=0.3)
    await retrying.ping()
    publisher = WakeChannel(retrying, key_prefix=key_prefix)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resynced()
        proxy.drop_replies_of_open_connections()

        await publisher.publish("orders/7")

        await WakeChannel(make_client(redis_url), key_prefix=key_prefix).publish("barrier")
        assert [await reader.next_wake() for _ in range(3)] == [
            Wake("orders/7"),
            Wake("orders/7"),
            Wake("barrier"),
        ]
        assert reader.drain() == []


async def test_a_publish_whose_replies_are_all_lost_fails_once_the_clients_retry_gives_up(
    proxy: TcpProxy, make_client: Callable[..., Redis], redis_url: str, key_prefix: str
):
    """Spec: WAKE-6, WAKE-7."""
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 2), socket_timeout=0.3)
    await retrying.ping()
    publisher = WakeChannel(retrying, key_prefix=key_prefix)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resynced()
        proxy.drop_replies()

        bounded_publish = within(publisher.publish("orders/7"))
        with pytest.raises(StoreError):
            await bounded_publish

        await WakeChannel(make_client(redis_url), key_prefix=key_prefix).publish("barrier")
        wakes = [await reader.next_wake()]
        while wakes[-1] != Wake("barrier"):
            wakes.append(await reader.next_wake())
        assert wakes[:-1] != []
        assert set(wakes[:-1]) == {Wake("orders/7")}


async def test_a_publish_writes_one_line_in_the_hosts_trace_whether_it_succeeds_or_is_refused(
    url_denying: Callable[[str], Awaitable[str]],
    redis_url: str,
    make_client: Callable[..., Redis],
    key_prefix: str,
    log_lines: LogLines,
):
    """Spec: LOG-7."""
    accepted = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    refused = WakeChannel(make_client(await url_denying("publish")), key_prefix=key_prefix)

    with unit_trace(None):
        traceparent = current_traceparent()
        assert traceparent is not None
        await accepted.publish("orders/7")
        with pytest.raises(StoreError):
            await refused.publish("orders/7")

    done, failed = log_lines.of("wake_publication.ended", "tandem.wake")
    assert done["level"] == "info"
    assert done["outcome"] == Outcome.OK
    assert failed["level"] == "error"
    assert failed["outcome"] == Outcome.FAILED
    assert failed["error_type"] == StoreError.__name__
    assert done["duration_ms"] >= 0
    assert failed["duration_ms"] >= 0
    assert done["trace_id"] == traceparent.split("-")[1]
    assert failed["trace_id"] == traceparent.split("-")[1]
    log_lines.assert_none_holds("orders/7")
