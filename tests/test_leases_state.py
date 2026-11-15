import asyncio
from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Leases, LeaseState, Liveness
from tandem.keys import lease_keys
from tests.support.leases import liveness_of, until_gone
from tests.support.waiting import within

LASTS = timedelta(milliseconds=300)
LONG_LASTS = timedelta(seconds=30)
READERS = 3
CYCLES = 8


async def test_a_live_holder_reads_alive_with_its_generation_and_its_expiry_unchanged(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    leases = new_leases(make_client(redis_url), lasts=LONG_LASTS)
    renewal = await leases.renew("pod-a")
    expiry = lease_keys(key_prefix, "owner").expiry
    expires_before = await observer.zscore(expiry, "pod-a")

    state = (await leases.state(["pod-a"]))["pod-a"]

    assert state.liveness is Liveness.ALIVE
    assert state.generation == renewal.generation
    assert state.remaining is not None
    assert timedelta(0) < state.remaining <= LONG_LASTS
    assert await observer.zscore(expiry, "pod-a") == expires_before


async def test_reading_the_state_of_an_absent_holder_creates_no_lease(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    leases = new_leases(make_client(redis_url))
    keys = lease_keys(key_prefix, "owner")

    await leases.state(["pod-a"])

    assert await observer.zcard(keys.expiry) == 0
    assert await observer.hlen(keys.generation) == 0


@pytest.mark.parametrize("ended_first", [True, False], ids=["ended", "expired"])
async def test_an_ended_or_expired_holder_reads_gone_without_generation_or_remaining(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    ended_first: bool,
):
    leases = new_leases(make_client(redis_url), lasts=LASTS)
    renewal = await leases.renew("pod-a")
    if ended_first:
        assert await leases.end("pod-a", renewal.generation) is True

    await until_gone(leases, "pod-a")

    assert (await leases.state(["pod-a"]))["pod-a"] == LeaseState(Liveness.GONE, None, None)


async def test_a_lapsed_and_renewed_holder_reads_the_new_generation_and_the_old_one_ends_nothing(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    leases = new_leases(make_client(redis_url), lasts=LASTS)
    old = await leases.renew("pod-a")
    await until_gone(leases, "pod-a")

    new = await leases.renew("pod-a")
    state = (await leases.state(["pod-a"]))["pod-a"]

    assert new.generation != old.generation
    assert state.generation == new.generation
    assert await leases.end("pod-a", old.generation) is False
    assert await liveness_of(leases, "pod-a") is Liveness.ALIVE


async def test_a_state_racing_renewals_and_ends_of_its_holder_reads_the_generation_live_then(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    settled_epoch: None,
):
    # The writer counts its transitions in `ticks`, four per cycle: renew called, renew returned
    # (live), end called, end returned (gone). A read that saw ticks `before` to `after` may report
    # only what the holder could read at one of those ticks. After the returned renew and the
    # returned end, the writer waits for a read wholly inside that tick.
    writer = new_leases(make_client(redis_url), lasts=LONG_LASTS)
    readers = [new_leases(make_client(redis_url), lasts=LONG_LASTS) for _ in range(READERS)]
    ticks = 0
    generations: list[str] = []
    finished = asyncio.Event()
    read_within_tick = asyncio.Event()
    reads: list[tuple[int, int, LeaseState]] = []

    async def settle_into_next_tick() -> None:
        nonlocal ticks
        ticks += 1
        read_within_tick.clear()
        await within(read_within_tick.wait())

    async def write() -> None:
        nonlocal ticks
        try:
            for _ in range(CYCLES):
                ticks += 1
                renewal = await writer.renew("pod-a")
                generations.append(renewal.generation)
                await settle_into_next_tick()
                ticks += 1
                await writer.end("pod-a", renewal.generation)
                await settle_into_next_tick()
        finally:
            finished.set()

    async def read(reader: Leases) -> None:
        while not finished.is_set():
            before = ticks
            state = (await reader.state(["pod-a"]))["pod-a"]
            after = ticks
            reads.append((before, after, state))
            if before == after and after % 4 in (0, 2):
                read_within_tick.set()

    def readable_at(tick: int) -> set[tuple[Liveness, str | None]]:
        if tick == 0:
            return {(Liveness.GONE, None)}
        cycle, step = divmod(tick - 1, 4)
        gone, alive = (Liveness.GONE, None), (Liveness.ALIVE, generations[cycle])
        return [{gone, alive}, {alive}, {alive, gone}, {gone}][step]

    await asyncio.gather(write(), *(read(reader) for reader in readers))

    for before, after, state in reads:
        readable = set().union(*(readable_at(tick) for tick in range(before, after + 1)))
        assert (state.liveness, state.generation) in readable
        assert (state.remaining is not None) is (state.liveness is Liveness.ALIVE)


async def test_many_holders_read_in_one_round_trip_each_by_its_own_liveness(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    settled_epoch: None,
    monkeypatch: pytest.MonkeyPatch,
):
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LONG_LASTS)
    alive = await leases.renew("alive")
    ended = await leases.renew("ended")
    await leases.end("ended", ended.generation)
    await leases.state(["alive"])  # loads the script, so the counted read is one command
    commands: list[object] = []
    execute_command = client.execute_command

    async def counted(*arguments: object, **options: object) -> object:
        commands.append(arguments[0])
        return await execute_command(*arguments, **options)

    monkeypatch.setattr(client, "execute_command", counted)

    states = await leases.state(["alive", "ended", "never"])

    assert len(commands) == 1
    assert states["alive"].generation == alive.generation
    assert states["alive"].liveness is Liveness.ALIVE
    assert states["ended"] == LeaseState(Liveness.GONE, None, None)
    assert states["never"] == LeaseState(Liveness.GONE, None, None)
