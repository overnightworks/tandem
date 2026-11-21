from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Deadlines, Leases, Locks
from tandem.keys import deadline_keys, lease_keys, lock_keys, wake_channel
from tests.support.locks import expiry_of
from tests.support.slots import keys_of
from tests.support.strings import ANY_STRINGS


@pytest.mark.parametrize("name", ["x", "epoch", "wake", "d", "l", "lock", "expiry", "generation"])
async def test_a_family_named_like_a_lease_family_or_a_fixed_key_shares_no_key_and_no_type(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    new_leases: Callable[..., Leases],
    new_deadlines: Callable[..., Deadlines],
    observer: Redis,
    key_prefix: str,
    name: str,
):
    """Spec: LAYOUT-4."""
    client = make_client(redis_url)
    locks = new_locks(client, family=name)
    leases = new_leases(client, family=name)
    deadlines = new_deadlines(client, kind=name)
    lease = lease_keys(key_prefix, name)
    lock = lock_keys(key_prefix, name)
    others = {
        lease.expiry,
        lease.generation,
        lease.lapse.due,
        lease.lapse.claimed,
        lease.lapse.claim,
        *vars(deadline_keys(key_prefix, name)).values(),
        wake_channel(key_prefix),
    }

    generation = await locks.acquire("pod-a")
    expiry = await expiry_of(observer, key_prefix, "pod-a", name)
    await leases.renew("pod-a")
    await deadlines.set("pod-a", timedelta(0))

    assert {lock.expiry, lock.generation}.isdisjoint(others)
    assert lock.epoch == lease.epoch
    assert (await locks.state("pod-a")).generation == generation
    assert await expiry_of(observer, key_prefix, "pod-a", name) == expiry


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
    """Spec: LOCK-1."""
    arguments = {"key_prefix": "skat", "family": "gpu", "lasts": timedelta(seconds=30)}
    client = make_client(redis_url)

    with pytest.raises(ValueError, match=argument):
        Locks(client, **{**arguments, argument: value})  # pyright: ignore[reportArgumentType] - the case passes a wrong type on purpose


@pytest.mark.parametrize("name", ANY_STRINGS)
async def test_a_name_may_be_any_string_and_is_never_a_key_segment(
    name: str,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
):
    """Spec: LOCK-2, LAYOUT-5."""
    locks = new_locks(make_client(redis_url))
    keys = lock_keys(key_prefix, "gpu")

    generation = await locks.acquire(name)

    assert generation is not None
    assert (await locks.state(name)).generation == generation
    assert await keys_of(observer, key_prefix) == {keys.epoch, keys.expiry, keys.generation}
