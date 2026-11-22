import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import Deadlines, Due
from tests.support.backends import Backend
from tests.support.deadlines import (
    TwoPods,
    assert_waits_for,
    claim_when_due,
    claimed_key,
    expired_claim,
    redis_ms,
    state_of,
)
from tests.support.proxy import TcpProxy

LATER = timedelta(seconds=60)


@pytest.mark.parametrize(
    ("claim_for", "claim_runs_out"),
    [
        pytest.param(timedelta(seconds=30), False, id="live claim"),
        pytest.param(timedelta(milliseconds=300), True, id="expired claim nobody took over"),
    ],
)
async def test_recur_ends_the_claim_and_arms_the_key_for_the_next_due_time_in_one_step(
    backend: Backend,
    new_deadlines: Callable[..., Deadlines],
    claim_for: timedelta,
    claim_runs_out: bool,
):
    """Spec: DEADLINE-17."""
    client = backend.new_client()
    deadlines = new_deadlines(client, claim_for=claim_for)
    if claim_runs_out:
        due, _ = await expired_claim(client, deadlines)
    else:
        due = await claimed_key(deadlines)
    before = await redis_ms(client)

    assert await deadlines.recur(due, LATER) is True

    after = await redis_ms(client)
    waiting = await state_of(client, deadlines, "room-1")
    assert_waits_for(waiting, LATER, before_ms=before, after_ms=after)
    assert await deadlines.done(due) is False
    assert await state_of(client, deadlines, "room-1") == waiting


async def test_recur_after_a_takeover_returns_false_and_the_new_holders_claim_stays(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-18."""
    client = make_client(redis_url)
    pod_a = new_deadlines(client, claim_for=timedelta(milliseconds=300))
    pod_b = new_deadlines(make_client(redis_url))
    old, _ = await expired_claim(client, pod_a)
    (taken,) = await claim_when_due(pod_b)
    claimed = await state_of(client, pod_b, "room-1")

    assert await pod_a.recur(old, LATER) is False

    assert await state_of(client, pod_b, "room-1") == claimed
    assert await pod_b.done(taken) is True


@pytest.mark.parametrize("voiding", ["cancel", "set"])
async def test_recur_after_a_cancel_or_a_set_returns_false_and_changes_nothing(
    backend: Backend, new_deadlines: Callable[..., Deadlines], voiding: str
):
    """Spec: DEADLINE-18."""
    client = backend.new_client()
    deadlines = new_deadlines(client)
    due = await claimed_key(deadlines)
    if voiding == "cancel":
        assert await deadlines.cancel("room-1") is True
    else:
        await deadlines.set("room-1", LATER * 2)
    state = await state_of(client, deadlines, "room-1")

    assert await deadlines.recur(due, LATER) is False

    assert await state_of(client, deadlines, "room-1") == state


async def test_a_concurrent_arm_never_finds_the_key_absent_while_recur_re_arms_it(
    two_pods: TwoPods,
):
    """Spec: DEADLINE-19."""
    client, pod_a, pod_b = two_pods
    keys = [f"room-{number}" for number in range(20)]
    for key in keys:
        await pod_a.set(key, timedelta(0))
    dues = await pod_a.claim_due(limit=len(keys))
    barrier = asyncio.Barrier(2)

    async def at_the_barrier(transition: Awaitable[bool]) -> bool:
        await barrier.wait()
        return await transition

    before = await redis_ms(client)
    for due in dues:
        recurred, armed = await asyncio.gather(
            at_the_barrier(pod_a.recur(due, LATER)),
            at_the_barrier(pod_b.arm(due.key, timedelta(0))),
        )
        assert recurred is True
        assert armed is False
    after = await redis_ms(client)

    for key in keys:
        assert_waits_for(
            await state_of(client, pod_a, key), LATER, before_ms=before, after_ms=after
        )


async def test_a_recur_whose_reply_is_lost_is_replayed_to_false_and_the_key_waits_at_the_first_due(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-20."""
    direct = make_client(redis_url)
    due = await claimed_key(new_deadlines(direct))
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 3), socket_timeout=0.3)
    proxy_pod = new_deadlines(retrying)
    assert await proxy_pod.recur(Due("warm-up", "no-claim"), LATER) is False  # warms the script
    proxy.drop_replies_of_open_connections()
    before = await redis_ms(direct)

    recurred = await proxy_pod.recur(due, LATER)

    after = await redis_ms(direct)
    assert recurred is False
    assert_waits_for(
        await state_of(direct, proxy_pod, "room-1"), LATER, before_ms=before, after_ms=after
    )
