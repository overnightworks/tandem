from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Deadlines, Leases
from tandem.keys import deadline_keys, lease_keys, wake_channel
from tests.support.leases import Handled, serving

LASTS = timedelta(milliseconds=200)


@pytest.mark.parametrize("name", ["epoch", "wake", "d", "l", "lapse"])
async def test_a_family_named_like_a_fixed_key_shares_no_key_and_meets_no_wrongtype(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    new_deadlines: Callable[..., Deadlines],
    key_prefix: str,
    name: str,
):
    client = make_client(redis_url)
    leases = new_leases(client, family=name, lasts=LASTS)
    deadlines = new_deadlines(client, kind=name)
    lease = lease_keys(key_prefix, name)
    own = {
        lease.epoch,
        lease.expiry,
        lease.generation,
        lease.lapse.due,
        lease.lapse.claimed,
        lease.lapse.claim,
    }
    others = {*vars(deadline_keys(key_prefix, name)).values(), wake_channel(key_prefix)}

    await deadlines.set("room-1", timedelta(0))
    await leases.renew("pod-a")
    handled = Handled()
    async with serving(leases, handled):
        await handled.received()
    claimed = await deadlines.claim_due(claimer="b", limit=1)

    assert own.isdisjoint(others)
    assert [due.key for due in claimed] == ["room-1"]


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("lasts", timedelta(0)),
        ("lasts", timedelta(seconds=-1)),
        ("lasts", 30),
        ("claim_for", timedelta(0)),
        ("claim_for", timedelta(seconds=-1)),
        ("claim_for", 30),
    ],
)
def test_a_lasts_or_claim_for_that_is_no_positive_timedelta_fails_loud(
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    redis_url: str,
    argument: str,
    value: object,
):
    options = {"lasts": timedelta(seconds=30), "claim_for": timedelta(seconds=30)}
    client = make_client(redis_url)

    with pytest.raises(ValueError, match=argument):
        new_leases(client, **{**options, argument: value})


@pytest.mark.parametrize(
    "every",
    [timedelta(0), timedelta(seconds=-1), timedelta(seconds=30), timedelta(seconds=31), 5],
)
def test_an_every_that_is_not_positive_or_not_below_lasts_fails_loud(
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    redis_url: str,
    every: object,
):
    leases = new_leases(make_client(redis_url), lasts=timedelta(seconds=30))

    with pytest.raises(ValueError, match="every"):
        leases.kept("pod-a", every=every)  # pyright: ignore[reportArgumentType]
