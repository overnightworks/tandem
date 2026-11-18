from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Slots
from tandem.keys import slot_key
from tests.support.slots import FAMILY, take_hold_left_expired_beside_a_live_one
from tests.support.waiting import until

LASTS = timedelta(milliseconds=300)
LONG_LASTS_MS = 1000


async def test_a_hold_frees_its_slot_on_redis_time_and_the_family_key_goes_with_the_last_hold(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
    observer: Redis,
    key_prefix: str,
):
    slots = new_slots(make_client(redis_url), lasts=LASTS)
    assert await slots.take({"person-a": 1}) is not None
    assert await slots.take({"person-a": 1}) is None

    async def given() -> bool:
        return await slots.take({"person-a": 1}) is not None

    await until(given)

    async def key_gone() -> bool:
        return not await observer.exists(slot_key(key_prefix, FAMILY))

    await until(key_gone)


async def test_a_deleted_family_key_heals_with_the_next_take(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
    observer: Redis,
    key_prefix: str,
):
    slots = new_slots(make_client(redis_url))
    assert await slots.take({"person-a": 1}) is not None
    await observer.delete(slot_key(key_prefix, FAMILY))

    assert await slots.take({"person-a": 1}) is not None
    assert await slots.take({"person-a": 1}) is None


async def test_a_take_frees_the_expired_hold_while_a_later_live_hold_keeps_the_family_key_alive(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
    observer: Redis,
    key_prefix: str,
):
    client = make_client(redis_url)
    long_lasts = timedelta(milliseconds=LONG_LASTS_MS)
    slots = new_slots(client, lasts=long_lasts)
    expired, _ = await take_hold_left_expired_beside_a_live_one(
        slots, client, observer, key_prefix, lasts=long_lasts
    )
    family = slot_key(key_prefix, FAMILY)
    assert await observer.zcard(family) == 2

    assert await slots.take({"person-a": 1}) is not None

    assert await observer.zscore(family, expired) is None
    assert await observer.zcard(family) == 2
