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


@pytest.mark.parametrize("admit_before_acquire", [True, False])
async def test_a_reserve_that_reads_the_depth_around_its_acquire_never_leaves_a_lock_beside_a_depth(
    admit_before_acquire: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    observer: Redis,
    depth: str,
):
    client = make_client(redis_url)
    locks = new_locks(client)
    admit = Admit(client, locks, depth)

    assert await _depth_of(observer, depth) == 0
    if admit_before_acquire:
        await admit("gpu-0")
    generation = await locks.acquire("gpu-0")
    assert generation is not None
    if not admit_before_acquire:
        await admit("gpu-0")
    if await _depth_of(observer, depth) > 0:
        await locks.release("gpu-0", generation)

    held = (await locks.state("gpu-0")).generation is not None
    assert not (held and await _depth_of(observer, depth) > 0)
