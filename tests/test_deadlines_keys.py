from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Deadlines, WakeChannel
from tests.support.backends import Backend
from tests.support.deadlines import claim_when_due


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
    arguments = {"key_prefix": "skat", "kind": "work", argument: value}
    client = backend.new_client()
    claim_for = timedelta(seconds=1)

    with pytest.raises(ValueError, match=argument):
        Deadlines(client, claim_for=claim_for, **arguments)


@pytest.mark.parametrize("claim_for", [timedelta(0), timedelta(seconds=-1)])
def test_a_claim_that_lasts_no_time_fails_at_construction_naming_the_argument(
    backend: Backend, claim_for: timedelta
):
    client = backend.new_client()

    with pytest.raises(ValueError, match="claim_for"):
        Deadlines(client, key_prefix="skat", kind="work", claim_for=claim_for)


async def test_kinds_named_like_the_fixed_tokens_share_a_prefix_with_a_wake_channel_without_a_clash(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    observer: Redis,
    key_prefix: str,
):
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
