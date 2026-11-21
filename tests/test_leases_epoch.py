from collections.abc import Callable
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Leases, LeaseState, Liveness, Roster
from tandem.keys import lease_keys
from tests.support.deadlines import redis_ms
from tests.support.leases import Handled, liveness_of, serving, wipe
from tests.support.restart import restart
from tests.support.waiting import until

LASTS_MS = 400
LASTS = timedelta(milliseconds=LASTS_MS)


async def test_a_renewal_after_a_redis_restart_starts_a_lease_with_another_generation(
    restartable_redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    """Spec: LEASE-5."""
    client = make_client(restartable_redis_url)
    leases = new_leases(client)
    before = await leases.renew("pod-a")

    await restart(client)
    after = await leases.renew("pod-a")

    assert after.started is True
    assert after.generation != before.generation


async def test_after_a_redis_restart_an_absent_lease_reads_unknown_for_lasts_then_gone(
    restartable_redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    """Spec: LEASE-8."""
    client = make_client(restartable_redis_url)
    leases = new_leases(client, lasts=LASTS)
    await leases.renew("pod-a")

    await restart(client)
    read_at = await redis_ms(client)
    first_read = await leases.state(["pod-a"])

    async def settled() -> bool:
        return await liveness_of(leases, "pod-a") is Liveness.GONE

    assert first_read == {"pod-a": LeaseState(Liveness.UNKNOWN, None, None)}
    await until(settled)
    assert await redis_ms(client) >= read_at + LASTS_MS


async def test_only_state_reads_after_a_wipe_read_unknown_until_lasts_passed_since_the_first(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    """Spec: LEASE-8."""
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS)
    await wipe(observer, key_prefix, "owner")

    read_at = await redis_ms(client)
    first_read = await leases.state(["pod-a", "pod-b"])

    async def settled() -> bool:
        return await liveness_of(leases, "pod-a") is Liveness.GONE

    assert first_read == {
        "pod-a": LeaseState(Liveness.UNKNOWN, None, None),
        "pod-b": LeaseState(Liveness.UNKNOWN, None, None),
    }
    await until(settled)
    assert await redis_ms(client) >= read_at + LASTS_MS


async def test_state_renew_and_the_sweep_each_create_the_epoch_in_their_own_script(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    """Spec: LUA-2."""
    leases = new_leases(make_client(redis_url))
    epoch = lease_keys(key_prefix, "owner").epoch

    async def has_epoch() -> bool:
        return bool(await observer.exists(epoch))

    await wipe(observer, key_prefix, "owner")
    await leases.state(["pod-a"])
    assert await has_epoch()

    await wipe(observer, key_prefix, "owner")
    await leases.renew("pod-a")
    assert await has_epoch()

    await wipe(observer, key_prefix, "owner")
    async with serving(leases, Handled()):
        await until(has_epoch)


async def test_live_reads_unsettled_right_after_a_wipe_and_settled_with_the_holders_after_lasts(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    """Spec: LEASE-10."""
    leases = new_leases(make_client(redis_url), lasts=LASTS)
    await wipe(observer, key_prefix, "owner")

    right_after = await leases.live()
    await leases.renew("pod-a")
    await leases.renew("pod-b")

    async def settled() -> bool:
        await leases.renew("pod-a")
        await leases.renew("pod-b")
        return (await leases.live()).settled

    await until(settled)
    assert right_after == Roster(holders=frozenset(), settled=False)
    assert await leases.live() == Roster(holders=frozenset({"pod-a", "pod-b"}), settled=True)
