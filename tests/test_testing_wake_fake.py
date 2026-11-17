"""The controls of `FakeWakeChannel` that no real Redis cell has a counterpart for."""

import asyncio

from tandem.testing import FakeWakeBus, FakeWakeChannel
from tests.support.waiting import BOUND, until


async def test_a_following_that_was_withheld_its_confirmation_enters_once_resync_confirms():
    channel = FakeWakeBus().pod()
    channel.withhold_confirmation()
    resyncs: list[str] = []

    async def enter_and_leave() -> None:
        async with channel.following(
            on_resync=lambda: resyncs.append("resync"),
            on_wake=lambda wake: None,
            on_failed=lambda error: None,
            ready_within=BOUND,
        ):
            pass

    entering = asyncio.create_task(enter_and_leave())

    async def entered() -> bool:
        channel.resync()  # a resync before the subscription exists reaches nothing
        return entering.done()

    await until(entered)

    await entering
    assert resyncs


async def test_a_wake_published_while_the_confirmation_is_withheld_is_lost_before_the_resync():
    bus = FakeWakeBus()
    channel, other_pod = bus.pod(), bus.pod()
    channel.withhold_confirmation()
    calls: list[str] = []

    async def enter_and_leave() -> None:
        async with channel.following(
            on_resync=lambda: calls.append("resync"),
            on_wake=lambda wake: calls.append("wake"),
            on_failed=lambda error: None,
            ready_within=BOUND,
        ):
            pass

    entering = asyncio.create_task(enter_and_leave())

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
    channel = FakeWakeBus().pod()
    channel.withhold_confirmation()

    async def withheld_following_entered() -> bool:
        async with channel.following(
            on_resync=lambda: None,
            on_wake=lambda wake: None,
            on_failed=lambda error: None,
            ready_within=BOUND,
        ):
            return True

    withheld = asyncio.create_task(withheld_following_entered())

    async def confirmed() -> bool:
        channel.resync()
        return withheld.done()

    await until(confirmed)
    assert await withheld

    async with channel.following(
        on_resync=lambda: None,
        on_wake=lambda wake: None,
        on_failed=lambda error: None,
        ready_within=BOUND,
    ):
        pass


async def test_a_channel_constructed_on_a_bus_hears_every_wake_once():
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
