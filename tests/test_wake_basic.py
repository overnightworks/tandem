import asyncio

import pytest

from tandem import Resubscribed, StoreError, Wake, WakeChannel
from tests.support.backends import Backend
from tests.support.reader import Reader


async def test_resubscribed_comes_first_and_every_wake_after_it(backend: Backend, key_prefix: str):
    """C1"""
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


async def test_a_wake_published_through_one_client_arrives_through_another(
    backend: Backend, key_prefix: str
):
    """C2"""
    publisher = WakeChannel(backend.new_client(), key_prefix=key_prefix)
    async with Reader(WakeChannel(backend.new_client(), key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        await publisher.publish("orders/7")
        assert await reader.next_wake() == Wake("orders/7")


async def test_publish_fails_during_an_outage_and_delivers_after_it(
    backend: Backend, key_prefix: str
):
    """C10"""
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
    """C12"""
    with pytest.raises(ValueError, match="key_prefix"):
        WakeChannel(backend.new_client(), key_prefix=key_prefix)
