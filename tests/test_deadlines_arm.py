import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import Deadlines, Script
from tests.support.backends import Backend
from tests.support.deadlines import (
    KEYS_OF_THE_SCRIPT,
    TwoPods,
    assert_waits_for,
    claim_when_due,
    claimed_key,
    key_names,
    redis_ms,
    state_of,
    until_redis_ms,
)
from tests.support.proxy import TcpProxy

LATER = timedelta(seconds=60)
_ARM_IN_A_SCRIPT = KEYS_OF_THE_SCRIPT + "return tandem.deadline_arm(keys, ARGV[1], ARGV[2])"

Arm = Callable[[Redis, Deadlines, str, timedelta], Awaitable[bool]]


async def _arm_through_deadlines(
    client: Redis, deadlines: Deadlines, key: str, due_in: timedelta
) -> bool:
    return await deadlines.arm(key, due_in)


async def _arm_through_a_script(
    client: Redis, deadlines: Deadlines, key: str, due_in: timedelta
) -> bool:
    reply = await Script(client, _ARM_IN_A_SCRIPT).run(
        key_names(deadlines), [key, int(due_in.total_seconds() * 1000)]
    )
    return reply == 1


ARMS = [_arm_through_deadlines, _arm_through_a_script]


@pytest.mark.parametrize("arm", ARMS)
async def test_arm_of_an_absent_key_makes_it_come_due_later_on_redis_time_and_returns_true(
    backend: Backend, new_deadlines: Callable[..., Deadlines], arm: Arm
):
    """Spec: DEADLINE-13, LUA-8."""
    client = backend.new_client()
    deadlines = new_deadlines(client)
    before = await redis_ms(client)

    assert await arm(client, deadlines, "room-1", LATER) is True

    after = await redis_ms(client)
    assert_waits_for(
        await state_of(client, deadlines, "room-1"), LATER, before_ms=before, after_ms=after
    )


async def test_arm_of_a_waiting_key_returns_false_and_changes_nothing(
    backend: Backend, new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-14."""
    client = backend.new_client()
    deadlines = new_deadlines(client)
    await deadlines.set("room-1", LATER)
    waiting = await state_of(client, deadlines, "room-1")

    assert await deadlines.arm("room-1", timedelta(0)) is False

    assert await state_of(client, deadlines, "room-1") == waiting


async def test_arm_leaves_a_past_due_key_for_the_next_claim_which_takes_it_once(
    two_pods: TwoPods,
):
    """Spec: DEADLINE-14, DEADLINE-6."""
    client, pod_a, pod_b = two_pods
    await pod_a.set("room-1", timedelta(0))

    assert await pod_b.arm("room-1", LATER) is False

    state = await state_of(client, pod_b, "room-1")
    assert state is not None
    assert state[1] <= await redis_ms(client)
    assert [due.key for due in await pod_b.claim_due(limit=10)] == ["room-1"]
    assert await pod_b.claim_due(limit=10) == []


@pytest.mark.parametrize("arm", ARMS)
async def test_arm_of_a_key_a_live_claim_holds_returns_false_and_the_holders_done_returns_true(
    two_pods: TwoPods, arm: Arm
):
    """Spec: DEADLINE-14, LUA-8."""
    client, pod_a, pod_b = two_pods
    due = await claimed_key(pod_a)
    claimed = await state_of(client, pod_a, "room-1")

    assert await arm(client, pod_b, "room-1", LATER) is False

    assert await state_of(client, pod_b, "room-1") == claimed
    assert await pod_a.done(due) is True
    assert await state_of(client, pod_a, "room-1") is None


async def test_arm_of_a_key_whose_claim_expired_leaves_it_for_the_takeover_and_voids_the_old_done(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-14."""
    client = make_client(redis_url)
    pod_a = new_deadlines(client, claim_for=timedelta(milliseconds=300))
    pod_b = new_deadlines(make_client(redis_url))
    old = await claimed_key(pod_a)
    claimed = await state_of(client, pod_a, "room-1")
    assert claimed is not None
    await until_redis_ms(client, claimed[1])

    assert await pod_b.arm("room-1", LATER) is False

    assert await state_of(client, pod_b, "room-1") == claimed
    (taken,) = await claim_when_due(pod_b)
    assert taken.key == "room-1"
    assert taken.claim != old.claim
    assert await pod_a.done(old) is False


async def test_of_concurrent_arms_on_an_absent_key_exactly_one_returns_true(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-15."""
    pods = [new_deadlines(make_client(redis_url)) for _ in range(8)]
    barrier = asyncio.Barrier(len(pods))

    async def arm_at_the_barrier(deadlines: Deadlines) -> bool:
        await barrier.wait()
        return await deadlines.arm("room-1", LATER)

    client = make_client(redis_url)
    before = await redis_ms(client)
    results = await asyncio.gather(*(arm_at_the_barrier(pod) for pod in pods))
    after = await redis_ms(client)

    assert results.count(True) == 1
    assert_waits_for(
        await state_of(client, pods[0], "room-1"), LATER, before_ms=before, after_ms=after
    )


@pytest.mark.parametrize("ending", ["done", "cancel"])
async def test_arm_after_the_key_was_done_or_cancelled_arms_it_again(
    backend: Backend, new_deadlines: Callable[..., Deadlines], ending: str
):
    """Spec: DEADLINE-13."""
    client = backend.new_client()
    deadlines = new_deadlines(client)
    due = await claimed_key(deadlines)
    ended = await deadlines.done(due) if ending == "done" else await deadlines.cancel("room-1")
    assert ended is True
    before = await redis_ms(client)

    assert await deadlines.arm("room-1", LATER) is True

    after = await redis_ms(client)
    assert_waits_for(
        await state_of(client, deadlines, "room-1"), LATER, before_ms=before, after_ms=after
    )


async def test_an_arm_whose_reply_is_lost_is_replayed_to_false_and_the_key_waits_at_the_first_due(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
):
    """Spec: DEADLINE-16."""
    direct = make_client(redis_url)
    retrying = make_client(proxy.url, retry=Retry(NoBackoff(), 3), socket_timeout=0.3)
    proxy_pod = new_deadlines(retrying)
    assert await proxy_pod.arm("warm-up", LATER) is True  # warms the script
    proxy.drop_replies_of_open_connections()
    before = await redis_ms(direct)

    armed = await proxy_pod.arm("room-1", LATER)

    after = await redis_ms(direct)
    assert armed is False
    assert_waits_for(
        await state_of(direct, proxy_pod, "room-1"), LATER, before_ms=before, after_ms=after
    )


async def test_an_arm_the_client_runs_again_after_another_pod_finished_the_key_arms_it_again(
    two_pods: TwoPods,
):
    """Spec: DEADLINE-16."""
    client, first_pod, other_pod = two_pods
    assert await first_pod.arm("room-1", timedelta(0)) is True
    (due,) = await claim_when_due(other_pod)
    assert await other_pod.done(due) is True
    before = await redis_ms(client)

    replayed = await first_pod.arm("room-1", LATER)

    after = await redis_ms(client)
    assert replayed is True
    assert_waits_for(
        await state_of(client, first_pod, "room-1"), LATER, before_ms=before, after_ms=after
    )
