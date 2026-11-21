"""The controls of `FakeWakeChannel` that no real Redis cell has a counterpart for."""

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any

import pytest

from tandem import StoreError, Wake
from tandem.testing import FakeWakeBus, FakeWakeChannel
from tests.support.waiting import BOUND, until


async def enter_and_leave(
    channel: FakeWakeChannel,
    on_resync: Callable[[], None] = lambda: None,
    on_wake: Callable[[Wake], None] = lambda wake: None,
) -> None:
    async with channel.following(
        on_resync=on_resync,
        on_wake=on_wake,
        on_failed=lambda error: None,
        ready_within=BOUND,
    ):
        pass


async def entered_after_resyncs(
    channel: FakeWakeChannel, following: Coroutine[Any, Any, None]
) -> None:
    entering = asyncio.create_task(following)

    async def entered() -> bool:
        channel.resync()  # a resync before the subscription exists reaches nothing
        return entering.done()

    await until(entered)
    await entering


async def test_a_following_that_was_withheld_its_confirmation_enters_once_resync_confirms():
    """Spec: TESTING-4."""
    channel = FakeWakeBus().pod()
    channel.withhold_confirmation()
    resyncs: list[str] = []

    await entered_after_resyncs(
        channel, enter_and_leave(channel, on_resync=lambda: resyncs.append("resync"))
    )

    assert resyncs


async def test_a_wake_published_while_the_confirmation_is_withheld_is_lost_before_the_resync():
    """Spec: TESTING-4."""
    bus = FakeWakeBus()
    channel, other_pod = bus.pod(), bus.pod()
    channel.withhold_confirmation()
    calls: list[str] = []

    entering = asyncio.create_task(
        enter_and_leave(
            channel,
            on_resync=lambda: calls.append("resync"),
            on_wake=lambda wake: calls.append("wake"),
        )
    )

    async def entered() -> bool:
        if calls:
            return True
        await other_pod.publish("lost")  # reaches the following only once it is subscribed
        channel.resync()
        return False

    await until(entered)

    await entering
    assert calls == ["resync"]


async def test_a_following_after_the_resync_of_a_withheld_confirmation_is_confirmed_at_once():
    """Spec: TESTING-4."""
    channel = FakeWakeBus().pod()
    channel.withhold_confirmation()

    await entered_after_resyncs(channel, enter_and_leave(channel))

    await enter_and_leave(channel)


async def test_a_channel_constructed_on_a_bus_hears_every_wake_once():
    """Spec: TESTING-2."""
    bus = FakeWakeBus()
    channel = FakeWakeChannel(bus)
    addresses: list[str] = []

    async with channel.following(
        on_resync=lambda: None,
        on_wake=lambda wake: addresses.append(wake.address),
        on_failed=lambda error: None,
        ready_within=BOUND,
    ):
        await channel.publish("first")
        await channel.publish("second")

        async def heard_both() -> bool:
            return "second" in addresses

        await until(heard_both)

    assert addresses == ["first", "second"]


@asynccontextmanager
async def heard_by_a_following_pod(bus: FakeWakeBus, addresses: list[str]) -> AsyncIterator[None]:
    async with bus.pod().following(
        on_resync=lambda: None,
        on_wake=lambda wake: addresses.append(wake.address),
        on_failed=lambda error: None,
        ready_within=BOUND,
    ):
        yield


async def test_publishes_raise_the_given_errors_in_order_and_deliver_nothing_then_deliver_again():
    """Spec: TESTING-5."""
    bus = FakeWakeBus()
    addresses: list[str] = []
    publisher = bus.pod()
    first, second = StoreError("first"), StoreError("second")
    publisher.fail_publishes(first, second)
    raised: list[StoreError] = []

    async with heard_by_a_following_pod(bus, addresses):
        for address in ("lost-1", "lost-2"):
            try:
                await publisher.publish(address)
            except StoreError as error:
                raised.append(error)
        await publisher.publish("delivered")

        async def heard() -> bool:
            return addresses == ["delivered"]

        await until(heard)

    assert raised == [first, second]


async def test_a_publish_made_while_held_stays_pending_and_release_delivers_in_order():
    """Spec: TESTING-6."""
    bus = FakeWakeBus()
    addresses: list[str] = []
    publisher = bus.pod()
    hold = publisher.hold_publishes()

    async with heard_by_a_following_pod(bus, addresses):
        publishing = [asyncio.create_task(publisher.publish(a)) for a in ("one", "two", "three")]
        for _ in range(5):
            await asyncio.sleep(0)
        assert not any(task.done() for task in publishing)
        assert addresses == []

        hold.release()
        await asyncio.gather(*publishing)

        async def heard() -> bool:
            return addresses == ["one", "two", "three"]

        await until(heard)


async def test_a_held_publish_cancelled_by_its_caller_delivers_nothing_even_after_release():
    """Spec: TESTING-6."""
    bus = FakeWakeBus()
    addresses: list[str] = []
    publisher = bus.pod()
    hold = publisher.hold_publishes()

    async with heard_by_a_following_pod(bus, addresses):
        publishing = asyncio.create_task(publisher.publish("cancelled"))
        await asyncio.sleep(0)
        publishing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await publishing
        hold.release()
        await publisher.publish("after")

        async def heard() -> bool:
            return addresses == ["after"]

        await until(heard)
