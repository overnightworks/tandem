from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Slots
from tandem.keys import slot_key
from tests.support.backends import Backend
from tests.support.slots import FAMILY, take_hold_left_expired_beside_a_live_one

LASTS_MS = 1000
LASTS = timedelta(milliseconds=LASTS_MS)


async def test_release_frees_every_pool_of_the_hold_and_a_second_release_returns_false(
    backend: Backend, new_slots: Callable[..., Slots]
):
    slots = new_slots(backend.new_client())
    token = await slots.take({"global": 1, "person-a": 1})
    assert token is not None

    assert await slots.release(token) is True
    assert await slots.release(token) is False

    assert await slots.take({"global": 1, "person-b": 1}) is not None
    assert await slots.take({"global": 2, "person-a": 1}) is not None


async def test_releasing_an_unknown_token_returns_false_and_changes_nothing(
    backend: Backend, new_slots: Callable[..., Slots]
):
    slots = new_slots(backend.new_client())
    token = await slots.take({"person-a": 1})
    assert token is not None

    assert await slots.release("not-a-token") is False

    assert await slots.take({"person-a": 1}) is None
    assert await slots.release(token) is True


async def test_releasing_an_expired_but_unpruned_hold_returns_false_and_removes_its_member(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
    observer: Redis,
    key_prefix: str,
):
    client = make_client(redis_url)
    slots = new_slots(client, lasts=LASTS)
    expired = await take_hold_left_expired_beside_a_live_one(
        slots, client, observer, key_prefix, lasts=LASTS
    )
    family = slot_key(key_prefix, FAMILY)
    assert await observer.zcard(family) == 2

    assert await slots.release(expired) is False

    assert await observer.zcard(family) == 1
