from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

import pytest
from redis.asyncio import Redis

from tandem import Leases
from tests.support.deadlines import redis_ms
from tests.support.host_script import HostScript
from tests.support.leases import Handled, serving, until_gone, wipe
from tests.support.logs import LogLines
from tests.support.waiting import until

LASTS = timedelta(milliseconds=300)
HOLDER = "pod-a"
OTHER_HOLDER = "pod-b"
_LOGGER = "tandem.leases"

# A host's commit, fenced by a generation: it reads the holder's lease with `tandem.lease_state` and
# writes its own key only while that generation is live. KEYS: the lease's keys, then the host's
# key. ARGV: `lasts` in ms, the holder and the generation the commit is fenced by.
COMMIT = """
local keys = {epoch = KEYS[1], expiry = KEYS[2], generation = KEYS[3]}
local state = tandem.lease_state(keys, ARGV[2], ARGV[1])
if state and state[1] == 'live' and state[2] == ARGV[3] then
  redis.call('SET', KEYS[4], ARGV[3])
end
return state
"""


class Host:
    """A host whose store is the leases' Redis: it commits under a generation of a holder."""

    def __init__(self, client: Redis, leases: Leases, observer: Redis, committed: str) -> None:
        self._script = HostScript(client, COMMIT, leases, committed)
        self._client = client
        self._observer = observer
        self._committed = committed

    async def commit(self, holder: str, generation: str) -> list[object] | None:
        reply = await self._script.run(holder, generation)
        if reply is None:
            return None
        decode = self._client.get_encoder().decode
        return [
            decode(item, force=True) if isinstance(item, bytes) else item
            for item in cast(list[object], reply)
        ]

    async def written(self) -> str | None:
        """The generation the host's key holds, None while no commit wrote it."""
        return cast(str | None, await self._observer.get(self._committed))


@dataclass(frozen=True)
class World:
    leases: Leases
    host: Host
    log_lines: LogLines
    observer: Redis
    key_prefix: str


@pytest.fixture
def world(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    observer: Redis,
    key_prefix: str,
    log_lines: LogLines,
) -> World:
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS)
    host = Host(client, leases, observer, f"{key_prefix}:committed")
    return World(leases, host, log_lines, observer, key_prefix)


async def _started(world: World, holder: str = HOLDER) -> str:
    return (await world.leases.renew(holder)).generation


async def _past_its_expiry(world: World) -> tuple[str, str]:
    generation = await _started(world)
    await until_gone(world.leases, HOLDER)
    return HOLDER, generation


async def _swept_and_claimed(world: World) -> tuple[str, str]:
    holder, generation = await _past_its_expiry(world)
    claiming = Handled(hold=True)
    async with serving(world.leases, claiming):
        await claiming.received()
    return holder, generation


async def _lapse_served(world: World) -> tuple[str, str]:
    holder, generation = await _past_its_expiry(world)
    async with serving(world.leases, Handled()):
        await world.log_lines.await_of("lapse_handling.ended", _LOGGER)
    return holder, generation


async def _ended(world: World) -> tuple[str, str]:
    generation = await _started(world)
    assert await world.leases.end(HOLDER, generation)
    return HOLDER, generation


async def _replaced_by_a_new_generation(world: World) -> tuple[str, str]:
    holder, generation = await _past_its_expiry(world)
    assert (await world.leases.renew(holder)).generation != generation
    return holder, generation


async def _of_another_holder(world: World) -> tuple[str, str]:
    await _started(world)
    return HOLDER, await _started(world, OTHER_HOLDER)


async def _never_started(world: World) -> tuple[str, str]:
    return HOLDER, "a-generation-nobody-started"


async def test_a_host_commit_under_the_live_generation_writes_and_replies_live_with_its_expiry(
    world: World, observer: Redis
):
    """Spec: LEASE-23, LUA-9."""
    generation = await _started(world)
    before_ms = await redis_ms(observer)

    reply = await world.host.commit(HOLDER, generation)

    assert reply is not None
    assert reply[:2] == ["live", generation]
    expires_ms = reply[2]
    assert isinstance(expires_ms, int)
    assert before_ms < expires_ms <= await redis_ms(observer) + int(LASTS.total_seconds() * 1000)
    assert await world.host.written() == generation


async def test_a_host_commit_under_a_generation_that_replaced_the_lapsed_one_writes_only_the_new(
    world: World,
):
    """Spec: LEASE-23, LUA-9."""
    holder, lapsed = await _past_its_expiry(world)
    live = (await world.leases.renew(holder)).generation

    await world.host.commit(holder, lapsed)
    assert await world.host.written() is None
    await world.host.commit(holder, live)

    assert await world.host.written() == live


@pytest.mark.parametrize(
    "arrange",
    [
        pytest.param(_past_its_expiry, id="past-its-expiry-before-any-sweep"),
        pytest.param(_swept_and_claimed, id="swept-into-the-lapse-queue-and-not-served"),
        pytest.param(_lapse_served, id="lapse-served"),
        pytest.param(_ended, id="ended"),
        pytest.param(_replaced_by_a_new_generation, id="replaced-by-a-new-generation"),
        pytest.param(_of_another_holder, id="of-another-holder"),
        pytest.param(_never_started, id="never-started"),
    ],
)
async def test_a_host_commit_under_a_generation_that_is_not_live_writes_nothing(
    arrange: Callable[[World], Awaitable[tuple[str, str]]], world: World
):
    """Spec: LEASE-23, LUA-9."""
    holder, generation = await arrange(world)

    await world.host.commit(holder, generation)

    assert await world.host.written() is None


async def test_a_host_commit_after_a_wipe_creates_the_epoch_and_writes_nothing_while_unknown(
    world: World,
):
    """Spec: LEASE-23, LUA-9."""
    generation = await _started(world)
    await wipe(world.observer, world.key_prefix, "owner")
    assert not await world.observer.exists(world.leases.keys.epoch)

    reply = await world.host.commit(HOLDER, generation)

    assert reply == ["unknown"]
    assert await world.observer.exists(world.leases.keys.epoch)
    assert await world.host.written() is None


async def test_a_host_commit_after_a_wipe_replies_nil_and_writes_nothing_once_lasts_have_passed(
    world: World,
):
    """Spec: LEASE-23, LUA-9."""
    generation = await _started(world)
    await wipe(world.observer, world.key_prefix, "owner")

    async def settled() -> bool:
        return await world.host.commit(HOLDER, generation) is None

    await until(settled)

    assert await world.host.written() is None
