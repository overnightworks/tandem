from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Deadlines, Leases, Locks, Slots
from tandem.keys import deadline_keys, lapse_keys, lease_keys, lock_keys, slot_key, wake_channel
from tests.support.slots import FAMILY, keys_of


async def test_every_key_after_use_is_the_slot_key(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
    observer: Redis,
    key_prefix: str,
):
    """Spec: LAYOUT-9."""
    slots = new_slots(make_client(redis_url))
    token = await slots.take({"global": 2, "person-a": 1})
    assert token is not None
    assert await keys_of(observer, key_prefix) == {slot_key(key_prefix, FAMILY)}

    await slots.release(token)
    await slots.take({"person:with:colons": 1})

    assert await keys_of(observer, key_prefix) == {slot_key(key_prefix, FAMILY)}


@pytest.mark.parametrize(
    "name", ["x", "epoch", "wake", "d", "l", "lock", "slot", "expiry", "generation", "holders"]
)
async def test_a_family_named_like_another_structures_family_or_a_fixed_token_shares_no_key(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
    observer: Redis,
    key_prefix: str,
    name: str,
):
    client = make_client(redis_url)
    slots = new_slots(client, family=name)
    locks = Locks(client, key_prefix=key_prefix, family=name, lasts=timedelta(seconds=30))
    leases = Leases(
        client,
        key_prefix=key_prefix,
        family=name,
        lasts=timedelta(seconds=30),
        claim_for=timedelta(seconds=30),
    )
    deadlines = Deadlines(client, key_prefix=key_prefix, kind=name, claim_for=timedelta(seconds=30))
    lease = lease_keys(key_prefix, name)
    lock = lock_keys(key_prefix, name)
    others = {
        lease.epoch,
        lease.expiry,
        lease.generation,
        *vars(lapse_keys(key_prefix, name)).values(),
        lock.expiry,
        lock.generation,
        *vars(deadline_keys(key_prefix, name)).values(),
        wake_channel(key_prefix),
    }

    token = await slots.take({"person-a": 1})
    await locks.acquire("pod-a")
    await leases.renew("pod-a")
    await deadlines.set("pod-a", timedelta(0))

    assert slot_key(key_prefix, name) not in others
    assert token is not None
    assert await slots.release(token) is True
    assert slot_key(key_prefix, name) not in await keys_of(observer, key_prefix)


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("key_prefix", "Upper"),
        ("key_prefix", "with:colon"),
        ("key_prefix", ""),
        ("family", "Upper"),
        ("family", "with:colon"),
        ("family", "-leading-dash"),
        ("family", ""),
        ("lasts", timedelta(0)),
        ("lasts", timedelta(seconds=-1)),
        ("lasts", 30),
    ],
)
def test_a_prefix_a_family_or_lasts_that_is_invalid_fails_at_construction_naming_the_argument(
    make_client: Callable[..., Redis], redis_url: str, argument: str, value: object
):
    """Spec: SLOT-1."""
    arguments = {"key_prefix": "skat", "family": "gpu", "lasts": timedelta(seconds=30)}
    client = make_client(redis_url)

    with pytest.raises(ValueError, match=argument):
        Slots(client, **{**arguments, argument: value})  # pyright: ignore[reportArgumentType] - the case passes a wrong type on purpose


@pytest.mark.parametrize(
    "limits", [{}, {"person-a": -1}, {"person-a": True}, {"person-a": 1.5}, {"person-a": "3"}]
)
async def test_limits_that_are_empty_or_hold_a_cap_that_is_not_a_count_raise_naming_limits(
    make_client: Callable[..., Redis],
    redis_url: str,
    new_slots: Callable[..., Slots],
    limits: dict[str, object],
):
    """Spec: SLOT-4."""
    slots = new_slots(make_client(redis_url))

    with pytest.raises(ValueError, match="limits"):
        await slots.take(limits)  # pyright: ignore[reportArgumentType] - the case passes a wrong type on purpose
