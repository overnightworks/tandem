import asyncio
from collections.abc import Callable

import pytest

from tandem import Slots
from tests.support.backends import Backend


@pytest.mark.parametrize("cap", [1, 3])
async def test_takes_up_to_the_cap_succeed_and_the_next_returns_none(
    backend: Backend, new_slots: Callable[..., Slots], cap: int
):
    slots = new_slots(backend.new_client())

    tokens = [await slots.take({"person-a": cap}) for _ in range(cap)]

    assert None not in tokens
    assert len(set(tokens)) == cap
    assert await slots.take({"person-a": cap}) is None


async def test_a_cap_of_zero_refuses_every_take(backend: Backend, new_slots: Callable[..., Slots]):
    slots = new_slots(backend.new_client())

    assert await slots.take({"person-a": 0}) is None
    assert await slots.take({"person-a": 0}) is None


async def test_a_pool_another_pod_filled_refuses_this_pod(
    backend: Backend, new_slots: Callable[..., Slots]
):
    first = new_slots(backend.new_client())
    second = new_slots(backend.new_client())
    assert await first.take({"person-a": 1}) is not None

    assert await second.take({"person-a": 1}) is None


async def test_a_full_global_pool_takes_nothing_from_the_persons_pool(
    backend: Backend, new_slots: Callable[..., Slots]
):
    slots = new_slots(backend.new_client())
    other_persons = await slots.take({"global": 1, "person-b": 1})
    assert other_persons is not None

    assert await slots.take({"global": 1, "person-a": 1}) is None

    assert await slots.release(other_persons)
    assert await slots.take({"global": 1, "person-a": 1}) is not None


async def test_a_full_persons_pool_takes_nothing_from_the_global_pool(
    backend: Backend, new_slots: Callable[..., Slots]
):
    slots = new_slots(backend.new_client())
    assert await slots.take({"global": 2, "person-a": 1}) is not None

    assert await slots.take({"global": 2, "person-a": 1}) is None

    assert await slots.take({"global": 2, "person-b": 1}) is not None
    assert await slots.take({"global": 2, "person-c": 1}) is None


async def test_concurrent_takes_behind_a_barrier_never_exceed_a_cap(
    backend: Backend, new_slots: Callable[..., Slots]
):
    cap, contenders = 3, 12
    pods = [new_slots(backend.new_client()) for _ in range(contenders)]
    barrier = asyncio.Barrier(contenders)

    async def contend(pod: Slots) -> str | None:
        await barrier.wait()
        return await pod.take({"global": cap, "person-a": cap})

    results = await asyncio.gather(*(contend(pod) for pod in pods))

    assert len([token for token in results if token is not None]) == cap
