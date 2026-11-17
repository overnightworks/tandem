"""The controls of `FakeWakeChannel` that no real Redis cell has a counterpart for."""

import asyncio

from tandem.testing import FakeWakeBus
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
