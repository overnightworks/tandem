from collections.abc import Callable
from datetime import timedelta
from typing import Literal

import pytest
from redis import Redis as SyncRedis
from redis.asyncio import Redis

import tandem
from tandem import Leases, Liveness, Locks
from tandem.testing import EpochSet, aged_epoch, settle_epoch
from tests.support.leases import liveness_of
from tests.support.leases import wipe as wipe_leases
from tests.support.locks import wipe as wipe_locks


@pytest.mark.parametrize("lasts", [timedelta(seconds=30), timedelta(days=36500), timedelta.max])
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
    lasts: timedelta,
):
    """Spec: TESTING-1."""
    client = make_client(redis_url)
    leases = new_leases(client, lasts=lasts)
    locks = new_locks(client, lasts=lasts)
    await wipe_leases(observer, key_prefix, "owner")
    await wipe_locks(observer, key_prefix)
    if settled:
        await settle_epoch(observer, key_prefix=key_prefix)

    if family == "leases":
        liveness = await liveness_of(leases, "pod-a")
    else:
        liveness = (await locks.state("gpu-0")).liveness

    assert liveness is (Liveness.GONE if settled else Liveness.UNKNOWN)


@pytest.mark.parametrize(
    ("ran_for", "lasts", "liveness"),
    [
        pytest.param(
            timedelta(hours=1), timedelta(seconds=30), Liveness.GONE, id="lasts-below-ran-for"
        ),
        pytest.param(timedelta(hours=1), timedelta(hours=1), Liveness.GONE, id="lasts-equal"),
        pytest.param(
            timedelta(minutes=1), timedelta(days=1), Liveness.UNKNOWN, id="lasts-far-above-ran-for"
        ),
        pytest.param(None, timedelta(seconds=30), Liveness.GONE, id="none-short-lasts"),
        pytest.param(None, timedelta(days=36500), Liveness.GONE, id="none-century-lasts"),
        pytest.param(None, timedelta.max, Liveness.GONE, id="none-max-lasts"),
    ],
)
@pytest.mark.parametrize("family", ["leases", "locks"])
async def test_an_absent_holder_reads_as_the_aged_epoch_dates_it_through_a_sync_client(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    new_locks: Callable[..., Locks],
    observer: Redis,
    key_prefix: str,
    family: Literal["leases", "locks"],
    ran_for: timedelta | None,
    lasts: timedelta,
    liveness: Liveness,
):
    """Spec: TESTING-7."""
    client = make_client(redis_url)
    leases = new_leases(client, lasts=lasts)
    locks = new_locks(client, lasts=lasts)
    await wipe_leases(observer, key_prefix, "owner")
    await wipe_locks(observer, key_prefix)
    with SyncRedis.from_url(redis_url) as sync_client:
        sync_client.set(*aged_epoch(key_prefix=key_prefix, ran_for=ran_for))

    if family == "leases":
        actual = await liveness_of(leases, "pod-a")
    else:
        actual = (await locks.state("gpu-0")).liveness

    assert actual is liveness


def test_aged_epoch_unpacks_into_the_key_and_value_of_a_set():
    epoch = aged_epoch(key_prefix="app", ran_for=timedelta(seconds=1))

    key, value = epoch

    assert isinstance(epoch, EpochSet)
    assert (epoch.key, epoch.value) == (key, value)


@pytest.mark.parametrize(
    "ran_for",
    [
        pytest.param(timedelta(0), id="zero"),
        pytest.param(timedelta(seconds=-1), id="negative"),
        pytest.param(1.5, id="not-a-timedelta"),
    ],
)
def test_a_ran_for_that_is_no_positive_timedelta_raises_value_error_naming_it(ran_for: object):
    with pytest.raises(ValueError, match="ran_for"):
        aged_epoch(key_prefix="app", ran_for=ran_for)  # type: ignore[arg-type]


def test_the_root_export_contract_lists_the_testing_helpers():
    assert "testing" in tandem.__all__
    assert tandem.testing.settle_epoch is settle_epoch
    assert tandem.testing.aged_epoch is aged_epoch
    assert tandem.testing.EpochSet is EpochSet
