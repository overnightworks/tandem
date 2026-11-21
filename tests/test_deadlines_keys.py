from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Deadlines, WakeChannel
from tests.support.backends import Backend
from tests.support.deadlines import claim_when_due
from tests.support.strings import ANY_STRINGS


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("kind", "Upper"),
        ("kind", "with:colon"),
        ("kind", "-leading-dash"),
        ("kind", ""),
        ("key_prefix", "Upper"),
        ("key_prefix", "with:colon"),
        ("key_prefix", ""),
    ],
)
def test_a_kind_or_prefix_that_is_no_segment_fails_at_construction_naming_the_argument(
    backend: Backend, argument: str, value: str
):
    """Spec: DEADLINE-1."""
    arguments = {"key_prefix": "skat", "kind": "work", argument: value}
    client = backend.new_client()
    claim_for = timedelta(seconds=1)

    with pytest.raises(ValueError, match=argument):
        Deadlines(client, claim_for=claim_for, **arguments)


@pytest.mark.parametrize("claim_for", [timedelta(0), timedelta(seconds=-1), 5])
def test_a_claim_that_is_no_positive_timedelta_fails_at_construction_naming_the_argument(
    backend: Backend, claim_for: timedelta | int
):
    """Spec: DEADLINE-1."""
    client = backend.new_client()

    with pytest.raises(ValueError, match="claim_for"):
        Deadlines(client, key_prefix="skat", kind="work", claim_for=claim_for)  # pyright: ignore[reportArgumentType] - the case passes the wrong type on purpose


async def test_kinds_named_like_the_fixed_tokens_share_a_prefix_with_a_wake_channel_without_a_clash(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    observer: Redis,
    key_prefix: str,
):
    """Spec: LAYOUT-13."""
    client = make_client(redis_url)
    kinds = ["wake", "epoch", "d", "l"]
    by_kind = {kind: new_deadlines(client, kind=kind) for kind in kinds}
    channel = WakeChannel(client, key_prefix=key_prefix)

    for kind, deadlines in by_kind.items():
        await deadlines.set(f"{kind}-room", timedelta(0))
    await channel.publish("orders/7")
    claimed = {kind: await claim_when_due(deadlines) for kind, deadlines in by_kind.items()}

    assert {kind: [due.key for due in dues] for kind, dues in claimed.items()} == {
        kind: [f"{kind}-room"] for kind in kinds
    }
    found = {key async for key in observer.scan_iter(match=f"{key_prefix}:*")}
    assert found == {
        name for deadlines in by_kind.values() for name in _structures_of_a_claimed_key(deadlines)
    }
    assert len(found) == 2 * len(kinds)


def _structures_of_a_claimed_key(deadlines: Deadlines) -> list[str]:
    return [deadlines.keys.claimed, deadlines.keys.claim]


@pytest.mark.parametrize("key", ANY_STRINGS)
async def test_a_deadline_key_may_be_any_string(
    key: str, backend: Backend, new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-2."""
    deadlines = new_deadlines(backend.new_client())
    await deadlines.set(key, timedelta(0))

    claimed = await claim_when_due(deadlines)

    assert [due.key for due in claimed] == [key]
    assert await deadlines.done(claimed[0]) is True
