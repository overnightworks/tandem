from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Deadlines, Due, Script
from tests.support.backends import Backend
from tests.support.deadlines import (
    KEYS_OF_THE_SCRIPT,
    claim_when_due,
    key_names,
    redis_ms,
    state_of,
    until_redis_ms,
)

LATER = timedelta(seconds=60)
_SET_IN_A_SCRIPT = KEYS_OF_THE_SCRIPT + "tandem.deadline_set(keys, ARGV[1], ARGV[2])"

Rearm = Callable[[Redis, Deadlines, str], Awaitable[None]]


async def _rearm_through_deadlines(client: Redis, deadlines: Deadlines, key: str) -> None:
    await deadlines.set(key, LATER)


async def _rearm_through_a_script(client: Redis, deadlines: Deadlines, key: str) -> None:
    await Script(client, _SET_IN_A_SCRIPT).run(key_names(deadlines), [key, 60_000])


async def _claimed_key(client: Redis, deadlines: Deadlines) -> Due:
    await deadlines.set("room-1", timedelta(0))
    (due,) = await claim_when_due(deadlines)
    return due


@pytest.mark.parametrize(
    ("claim_for", "claim_runs_out"),
    [
        pytest.param(timedelta(seconds=30), False, id="live claim"),
        pytest.param(timedelta(milliseconds=300), True, id="expired claim"),
    ],
)
@pytest.mark.parametrize("rearm", [_rearm_through_deadlines, _rearm_through_a_script])
async def test_a_set_on_a_claimed_key_rearms_it_and_the_old_claimers_done_leaves_it_waiting(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    rearm: Rearm,
    claim_for: timedelta,
    claim_runs_out: bool,
):
    """Spec: DEADLINE-4, DEADLINE-9, LUA-3."""
    client = make_client(redis_url)
    deadlines = new_deadlines(client, claim_for=claim_for)
    old = await _claimed_key(client, deadlines)
    if claim_runs_out:
        claimed = await state_of(client, deadlines, "room-1")
        assert claimed is not None
        await until_redis_ms(client, claimed[1])
    before = await redis_ms(client)
    await rearm(client, deadlines, "room-1")
    after = await redis_ms(client)

    waiting = await state_of(client, deadlines, "room-1")
    assert await deadlines.done(old) is False
    assert await state_of(client, deadlines, "room-1") == waiting

    assert waiting is not None
    assert waiting[0] == "waiting"
    assert before + 60_000 <= waiting[1] <= after + 60_000


async def test_a_set_the_client_runs_again_invalidates_the_claim_another_pod_took_in_between(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-12."""
    client = make_client(redis_url)
    first_pod = new_deadlines(client)
    other_pod = new_deadlines(make_client(redis_url))
    await first_pod.set("room-1", timedelta(0))
    (claimed,) = await claim_when_due(other_pod)

    await first_pod.set("room-1", timedelta(0))

    assert await other_pod.done(claimed) is False


async def test_a_stale_extend_after_a_superseding_set_leaves_the_new_due_untouched(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-8, LUA-3."""
    client = make_client(redis_url)
    deadlines = new_deadlines(client)
    old = await _claimed_key(client, deadlines)
    await deadlines.set("room-1", LATER)
    waiting = await state_of(client, deadlines, "room-1")

    assert await deadlines.extend(old) is False

    assert await state_of(client, deadlines, "room-1") == waiting


@pytest.mark.parametrize("claimed_first", [False, True])
async def test_cancel_removes_a_pending_or_a_claimed_key_and_voids_its_claim(
    backend: Backend, new_deadlines: Callable[..., Deadlines], claimed_first: bool
):
    """Spec: DEADLINE-5, LUA-4."""
    client = backend.new_client()
    deadlines = new_deadlines(client)
    await deadlines.set("room-1", timedelta(0))
    claim = await claim_when_due(deadlines) if claimed_first else []

    assert await deadlines.cancel("room-1") is True

    assert await state_of(client, deadlines, "room-1") is None
    assert await deadlines.claim_due(limit=10) == []
    for due in claim:
        assert await deadlines.extend(due) is False
        assert await deadlines.done(due) is False


async def test_cancel_of_an_absent_key_is_false(
    backend: Backend, new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-5."""
    deadlines = new_deadlines(backend.new_client())

    assert await deadlines.cancel("never-set") is False


async def test_deadline_state_reads_waiting_with_the_due_and_claimed_with_the_expiry(
    backend: Backend, new_deadlines: Callable[..., Deadlines]
):
    """Spec: LUA-5."""
    client = backend.new_client()
    deadlines = new_deadlines(client, claim_for=timedelta(seconds=30))
    before_set = await redis_ms(client)
    await deadlines.set("room-1", LATER)
    after_set = await redis_ms(client)

    waiting = await state_of(client, deadlines, "room-1")
    assert waiting is not None
    assert waiting[0] == "waiting"
    assert before_set + 60_000 <= waiting[1] <= after_set + 60_000

    await deadlines.set("room-1", timedelta(0))
    before_claim = await redis_ms(client)
    await claim_when_due(deadlines)
    after_claim = await redis_ms(client)

    claimed = await state_of(client, deadlines, "room-1")
    assert claimed is not None
    assert claimed[0] == "claimed"
    assert before_claim + 30_000 <= claimed[1] <= after_claim + 30_000


@pytest.mark.parametrize("ending", ["done", "cancel", "never set"])
async def test_deadline_state_is_nil_once_a_key_is_done_cancelled_or_never_set(
    backend: Backend, new_deadlines: Callable[..., Deadlines], ending: str
):
    """Spec: LUA-5."""
    client = backend.new_client()
    deadlines = new_deadlines(client)
    if ending != "never set":
        due = await _claimed_key(client, deadlines)
        if ending == "done":
            assert await deadlines.done(due) is True
        else:
            assert await deadlines.cancel("room-1") is True

    assert await state_of(client, deadlines, "room-1") is None


async def test_a_cancel_the_client_runs_again_removes_the_deadline_another_pod_armed_in_between(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-12."""
    client = make_client(redis_url)
    first_pod = new_deadlines(client)
    other_pod = new_deadlines(make_client(redis_url))
    await first_pod.set("room-1", LATER)
    assert await first_pod.cancel("room-1") is True
    await other_pod.set("room-1", LATER)

    replayed = await first_pod.cancel("room-1")

    assert replayed is True
    assert await state_of(client, first_pod, "room-1") is None


async def test_a_set_the_client_runs_again_overwrites_the_due_time_another_pod_armed_in_between(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
):
    """Spec: DEADLINE-12."""
    client = make_client(redis_url)
    first_pod = new_deadlines(client)
    other_pod = new_deadlines(make_client(redis_url))
    await first_pod.set("room-1", LATER)
    await other_pod.set("room-1", LATER * 2)
    before_replay = await redis_ms(client)

    await first_pod.set("room-1", LATER)

    state = await state_of(client, first_pod, "room-1")
    assert state is not None
    assert state[1] < before_replay + 2 * 60_000 - 1_000
