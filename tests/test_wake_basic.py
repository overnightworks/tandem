import asyncio
from collections.abc import AsyncGenerator, Callable
from typing import cast

import pytest
from logline import current_traceparent, unit_trace
from redis.asyncio import Redis

from tandem import Resubscribed, StoreError, Wake, WakeChannel, checked_segment
from tandem.keys import wake_channel
from tests.support.backends import Backend
from tests.support.logs import LogLines
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
            subscribers = await observer.pubsub_numsub(wake_channel(key_prefix))
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


_TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
_INCOMING_TRACEPARENT = f"00-{_TRACE_ID}-b7ad6b7169203331-01"
_ADDRESSES = ["orders/7", "", "12:orders", ":", "0:", "5:abc", "caf\u00e9/7", "a\nb", "a\x00b", "-"]


@pytest.mark.parametrize("address", _ADDRESSES)
async def test_a_wake_arrives_with_its_address_byte_for_byte_and_no_trace_when_the_sender_has_none(
    address: str, redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    publisher = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()

        await publisher.publish(address)

        assert await reader.next_wake() == Wake(address, None)


@pytest.mark.parametrize("address", _ADDRESSES)
@pytest.mark.parametrize("decode_responses", [False, True])
async def test_a_wake_arrives_with_its_address_and_the_trace_of_its_sender(
    address: str,
    decode_responses: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    key_prefix: str,
):
    publisher = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    subscriber = make_client(redis_url, decode_responses=decode_responses)
    async with Reader(WakeChannel(subscriber, key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()

        with unit_trace(_INCOMING_TRACEPARENT):
            sent = current_traceparent()
            await publisher.publish(address)

        wake = await reader.next_wake()
    assert wake == Wake(address, sent)
    assert sent is not None
    assert sent.split("-")[1] == _TRACE_ID


@pytest.mark.parametrize(
    "payload",
    [b"orders/7", b"", b":orders", b"x:orders", b"-1:a", b"5:abc", b"1234567890:a", b"1:\xc3\xa9"],
)
@pytest.mark.parametrize("decode_responses", [False, True])
async def test_a_payload_that_is_no_frame_is_skipped_and_the_next_wake_still_arrives(
    payload: bytes,
    decode_responses: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    key_prefix: str,
    log_lines: LogLines,
):
    publisher = make_client(redis_url)
    subscriber = make_client(redis_url, decode_responses=decode_responses)
    async with Reader(WakeChannel(subscriber, key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        await publisher.publish(wake_channel(key_prefix), payload)
        await WakeChannel(publisher, key_prefix=key_prefix).publish("orders/7")
        wake = await reader.next_wake()
        assert reader.drain() == []

    assert wake.address == "orders/7"
    (skipped,) = log_lines.of("wake_payload.skipped", "tandem.wake")
    assert skipped["level"] == "warning"
    assert skipped["reason"] == "malformed_frame"
    assert skipped["payload_bytes"] == len(payload)


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


@pytest.mark.parametrize("decode_responses", [False, True])
async def test_a_payload_that_is_no_utf8_is_skipped_and_the_next_wake_still_arrives(
    decode_responses: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    key_prefix: str,
    log_lines: LogLines,
):
    malformed_text = "malformed-wake"
    malformed = b"0:" + malformed_text.encode() + b"\xff"
    publisher = make_client(redis_url)
    subscriber = make_client(redis_url, decode_responses=decode_responses)
    async with Reader(WakeChannel(subscriber, key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        await publisher.publish(wake_channel(key_prefix), malformed)
        if decode_responses:
            # redis-py fails inside get_message on a decode error: tandem subscribes again.
            await reader.next_resubscribed()
        await WakeChannel(publisher, key_prefix=key_prefix).publish("orders/7")
        wake = await reader.next_wake()
        assert reader.running

    assert wake == Wake("orders/7")
    (skipped,) = log_lines.of("wake_payload.skipped", "tandem.wake")
    assert skipped["level"] == "warning"
    assert skipped["reason"] == "invalid_utf8"
    assert skipped["payload_bytes"] == len(malformed)
    log_lines.assert_none_holds(malformed_text)
    log_lines.assert_none_holds("orders/7")
