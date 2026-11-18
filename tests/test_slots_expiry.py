from collections.abc import Callable, Mapping
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Slots
from tandem.keys import slot_key
from tests.support.deadlines import redis_ms, until_redis_ms
from tests.support.slots import FAMILY, take_hold_left_expired_beside_a_live_one
from tests.support.waiting import until

LASTS = timedelta(milliseconds=300)
LONG_LASTS_MS = 1000


class StalledSecondTake(Slots):
    """Slots whose second take starts only after the first hold has expired."""

    def __init__(self, client: Redis, *, key_prefix: str, lasts: timedelta) -> None:
        super().__init__(client, key_prefix=key_prefix, family=FAMILY, lasts=lasts)
        self._clock = client
        self._takes = 0

    async def take(self, limits: Mapping[str, int]) -> str | None:
        self._takes += 1
        if self._takes == 2:
            await until_redis_ms(self._clock, await redis_ms(self._clock) + LONG_LASTS_MS + 50)
        return await super().take(limits)


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
    expired = await take_hold_left_expired_beside_a_live_one(
        slots, client, observer, key_prefix, lasts=long_lasts
    )
    family = slot_key(key_prefix, FAMILY)
    assert await observer.zcard(family) == 2

    assert await slots.take({"person-a": 1}) is not None

    assert await observer.zscore(family, expired) is None
    assert await observer.zcard(family) == 2


async def test_the_expired_hold_builder_starts_over_when_a_stall_lets_the_first_hold_expire_early(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
):
    client = make_client(redis_url)
    long_lasts = timedelta(milliseconds=LONG_LASTS_MS)
    slots = StalledSecondTake(client, key_prefix=key_prefix, lasts=long_lasts)

    expired = await take_hold_left_expired_beside_a_live_one(
        slots, client, observer, key_prefix, lasts=long_lasts
    )

    family = slot_key(key_prefix, FAMILY)
    assert await observer.zcard(family) == 2
    assert await observer.zscore(family, expired) is not None
