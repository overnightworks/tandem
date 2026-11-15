import asyncio
from collections.abc import Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from tandem import Locks, Script
from tests.support.locks import wipe
from tests.support.waiting import until

LASTS = timedelta(milliseconds=300)

# A host's admit: it reads the lock with `tandem.lock_state` and counts an admission only when the
# lock is free. KEYS: the lock's keys, then the depth counter. ARGV: the name and `lasts` in ms.
ADMIT = """
local keys = {epoch = KEYS[1], expiry = KEYS[2], generation = KEYS[3]}
if tandem.lock_state(keys, ARGV[1], ARGV[2]) then
  return 0
end
redis.call('INCR', KEYS[4])
return 1
"""


class Admit:
    def __init__(self, client: Redis, locks: Locks, depth: str) -> None:
        self._script = Script(client, ADMIT)
        self._locks = locks
        self._depth = depth

    async def __call__(self, name: str) -> bool:
        keys = self._locks.keys
        lasts_ms = self._locks.lasts // timedelta(milliseconds=1)
        reply = await self._script.run(
            [keys.epoch, keys.expiry, keys.generation, self._depth], [name, lasts_ms]
        )
        return reply == 1


@pytest.fixture
def depth(key_prefix: str) -> str:
    return f"{key_prefix}:depth"


async def _depth_of(observer: Redis, depth: str) -> int:
    return int(await observer.get(depth) or 0)


async def test_a_host_admit_refuses_while_the_lock_is_held_and_admits_once_it_is_free(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    depth: str,
):
    client = make_client(redis_url)
    locks = new_locks(client)
    admit = Admit(client, locks, depth)
    generation = await locks.acquire("gpu-0")
    assert generation is not None

    assert await admit("gpu-0") is False
    assert await _depth_of(observer, depth) == 0
    await locks.release("gpu-0", generation)

    assert await admit("gpu-0") is True
    assert await _depth_of(observer, depth) == 1


async def test_a_host_admit_after_a_wipe_creates_the_epoch_and_refuses_until_lasts_have_passed(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
    depth: str,
):
    client = make_client(redis_url)
    locks = new_locks(client, lasts=LASTS)
    admit = Admit(client, locks, depth)
    await wipe(observer, key_prefix)

    refused = await admit("gpu-0")

    assert refused is False
    assert await observer.exists(locks.keys.epoch)

    async def admitted() -> bool:
        return await admit("gpu-0")

    await until(admitted)
    assert await _depth_of(observer, depth) == 1


BOUND_SECONDS = 10


class Reserve:
    """A host's reserve: read the depth, acquire, read the depth again and give the lock back when
    it rose. An admit waits at one barrier of it and runs there, while the reserve waits."""

    def __init__(self, locks: Locks, observer: Redis, depth: str, admit_at: int) -> None:
        self._locks = locks
        self._observer = observer
        self._depth = depth
        self._admit_at = admit_at
        self._reached = asyncio.Event()
        self._admitted = asyncio.Event()

    async def _barrier(self, position: int) -> None:
        if position == self._admit_at:
            self._reached.set()
            await self._admitted.wait()

    async def reserve(self, name: str) -> None:
        assert await _depth_of(self._observer, self._depth) == 0
        await self._barrier(0)
        generation = await self._locks.acquire(name)
        assert generation is not None
        await self._barrier(1)
        if await _depth_of(self._observer, self._depth) > 0:
            await self._locks.release(name, generation)
        await self._barrier(2)

    async def admit_at_the_barrier(self, admit: Admit, name: str) -> bool:
        await self._reached.wait()
        admitted = await admit(name)
        self._admitted.set()
        return admitted


@pytest.mark.parametrize(
    ("admit_at", "admitted"),
    [
        pytest.param(0, True, id="before-the-acquire"),
        pytest.param(1, False, id="after-the-acquire"),
        pytest.param(2, False, id="after-the-second-depth-read"),
    ],
)
async def test_an_admit_racing_a_reserve_never_leaves_a_lock_beside_a_depth(
    admit_at: int,
    admitted: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    depth: str,
):
    client = make_client(redis_url)
    locks = new_locks(client)
    race = Reserve(locks, observer, depth, admit_at)

    async with asyncio.timeout(BOUND_SECONDS):
        _, admit_reply = await asyncio.gather(
            race.reserve("gpu-0"), race.admit_at_the_barrier(Admit(client, locks, depth), "gpu-0")
        )

    assert admit_reply is admitted
    held = (await locks.state("gpu-0")).generation is not None
    assert not (held and await _depth_of(observer, depth) > 0)
