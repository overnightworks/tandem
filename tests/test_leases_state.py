import asyncio
from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Leases, LeaseState, Liveness
from tandem.keys import lease_keys
from tests.support.leases import liveness_of, until_gone

LASTS = timedelta(milliseconds=300)
LONG_LASTS = timedelta(seconds=30)
READERS = 6
READS_PER_READER = 40
WRITES = 40


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


async def test_an_ended_holder_reads_gone_without_generation_or_remaining(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    leases = new_leases(make_client(redis_url), lasts=LASTS)
    renewal = await leases.renew("pod-a")

    assert await leases.end("pod-a", renewal.generation) is True
    await until_gone(leases, "pod-a")

    assert (await leases.state(["pod-a"]))["pod-a"] == LeaseState(Liveness.GONE, None, None)


async def test_an_expired_holder_reads_gone_without_generation_or_remaining(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    leases = new_leases(make_client(redis_url), lasts=LASTS)
    await leases.renew("pod-a")

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


async def test_a_state_racing_renewals_and_ends_of_its_holder_reads_a_consistent_snapshot(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    writer = new_leases(make_client(redis_url), lasts=LONG_LASTS)
    readers = [new_leases(make_client(redis_url), lasts=LONG_LASTS) for _ in range(READERS)]
    started = asyncio.Event()
    issued: set[str] = set()

    async def write() -> None:
        await started.wait()
        for _ in range(WRITES):
            renewal = await writer.renew("pod-a")
            issued.add(renewal.generation)
            await writer.end("pod-a", renewal.generation)

    async def read(reader: Leases) -> list[LeaseState]:
        await started.wait()
        return [(await reader.state(["pod-a"]))["pod-a"] for _ in range(READS_PER_READER)]

    tasks = [asyncio.create_task(write()), *(asyncio.create_task(read(r)) for r in readers)]
    started.set()
    _, *reads = await asyncio.gather(*tasks)

    states = [state for reader_reads in reads for state in reader_reads]
    assert any(state.liveness is Liveness.ALIVE for state in states)
    for state in states:
        if state.liveness is Liveness.ALIVE:
            assert state.generation in issued
            assert state.remaining is not None
        else:
            assert (state.generation, state.remaining) == (None, None)


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
