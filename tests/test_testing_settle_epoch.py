from collections.abc import Callable
from typing import Literal

import pytest
from redis.asyncio import Redis

from tandem import Leases, Liveness, Locks
from tandem.testing import settle_epoch
from tests.support.leases import liveness_of
from tests.support.leases import wipe as wipe_leases
from tests.support.locks import wipe as wipe_locks


@pytest.mark.parametrize("settled", [False, True])
@pytest.mark.parametrize("family", ["leases", "locks"])
async def test_an_absent_holder_reads_gone_at_once_only_after_settle_epoch(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
    family: Literal["leases", "locks"],
    settled: bool,
):
    client = make_client(redis_url)
    leases = new_leases(client)
    locks = new_locks(client)
    await wipe_leases(observer, key_prefix, "owner")
    await wipe_locks(observer, key_prefix)
    if settled:
        await settle_epoch(observer, key_prefix=key_prefix)

    if family == "leases":
        liveness = await liveness_of(leases, "pod-a")
    else:
        liveness = (await locks.state("gpu-0")).liveness

    assert liveness is (Liveness.GONE if settled else Liveness.UNKNOWN)
