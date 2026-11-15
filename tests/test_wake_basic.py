import asyncio
from collections.abc import AsyncGenerator, Callable
from typing import cast

import pytest
from redis.asyncio import Redis

from tandem import Resubscribed, StoreError, Wake, WakeChannel, checked_segment
from tests.support.backends import Backend
from tests.support.reader import Reader
from tests.support.waiting import until, within


async def test_resubscribed_comes_first_and_every_wake_after_it(backend: Backend, key_prefix: str):
    writer = WakeChannel(backend.new_client(), key_prefix=key_prefix)
    observer = backend.new_client()
    channel = WakeChannel(backend.new_client(), key_prefix=key_prefix)
    stop = asyncio.Event()

    async def publish_in_a_loop() -> None:
        number = 0
        while not stop.is_set():
            await writer.publish(str(number))
            number += 1
            await asyncio.sleep(0)

    publishing = asyncio.create_task(publish_in_a_loop())
    try:
        async with Reader(channel) as reader:
            first = await reader.next()
            subscribers = await observer.pubsub_numsub(f"{key_prefix}:tandem:v1:wake")
            wakes = [await reader.next_wake() for _ in range(3)]
    finally:
        stop.set()
        await publishing

    assert first == Resubscribed()
    assert subscribers[0][1] >= 1
    assert all(isinstance(wake, Wake) for wake in wakes)


@pytest.mark.parametrize("decode_responses", [False, True])
async def test_a_wake_published_through_one_client_arrives_through_another(
    backend: Backend, key_prefix: str, decode_responses: bool
):
    publisher = WakeChannel(backend.new_client(), key_prefix=key_prefix)
    subscriber = backend.new_client(decode_responses=decode_responses)
    async with Reader(WakeChannel(subscriber, key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        await publisher.publish("orders/7")
        assert await reader.next_wake() == Wake("orders/7")


async def test_an_address_arrives_in_the_encoding_of_the_hosts_client(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    host_client = make_client(redis_url, encoding="latin-1")
    channel = WakeChannel(host_client, key_prefix=key_prefix)
    async with Reader(channel) as reader:
        await reader.next_resubscribed()
        await channel.publish("caf\u00e9/7")
        assert await reader.next_wake() == Wake("caf\u00e9/7")


async def test_publish_fails_during_an_outage_and_delivers_after_it(
    backend: Backend, key_prefix: str
):
    subscriber_client = backend.new_client()
    publisher_client, outage = backend.publisher_with_outage()
    publisher = WakeChannel(publisher_client, key_prefix=key_prefix)
    async with Reader(WakeChannel(subscriber_client, key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        outage.begin()
        with pytest.raises(StoreError):
            await publisher.publish("during")
        outage.end()
        await publisher.publish("after")
        assert await reader.next_wake() == Wake("after")


@pytest.mark.parametrize("key_prefix", ["a:b", "", "Upper", "-lead", "a b", "a\n"])
def test_construction_refuses_a_prefix_that_is_not_one_segment(backend: Backend, key_prefix: str):
    client = backend.new_client()
    with pytest.raises(ValueError, match="key_prefix"):
        WakeChannel(client, key_prefix=key_prefix)
    with pytest.raises(ValueError, match="prefix_of_the_host"):
        checked_segment(key_prefix, argument="prefix_of_the_host")


@pytest.mark.parametrize("value", ["skat", "skat-agents", "0a"])
def test_checked_segment_returns_a_prefix_that_wake_channel_accepts(value: str):
    assert checked_segment(value, argument="key_prefix") == value
    WakeChannel(Redis(), key_prefix=value)


async def test_a_client_that_checks_its_connection_health_still_yields_only_wakes(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    interval = 0.05
    channel = WakeChannel(
        make_client(redis_url, health_check_interval=interval), key_prefix=key_prefix
    )
    publisher = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    events = cast(AsyncGenerator[Wake | Resubscribed], channel.received())
    try:
        assert await within(anext(events)) == Resubscribed()
        # redis-py sends its PING at the next read once the loop clock passes the interval.
        clock = asyncio.get_running_loop()
        health_check_falls_due = clock.time() + interval

        async def health_check_is_due() -> bool:
            return clock.time() > health_check_falls_due

        await until(health_check_is_due)
        pending = asyncio.ensure_future(anext(events))
        await publisher.publish("a")
        await publisher.publish("b")
        assert [await within(pending), await within(anext(events))] == [Wake("a"), Wake("b")]
    finally:
        await events.aclose()
