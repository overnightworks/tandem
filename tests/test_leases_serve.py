import asyncio
import signal
import sys
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import Leases, Liveness, StoreError
from tandem.errors import BACKOFF_FIRST_SECONDS
from tandem.keys import lease_keys
from tests.support.backoffs import BackoffRecordingLeases, Backoffs
from tests.support.deadlines import redis_ms, until_redis_ms
from tests.support.leases import (
    SERVING_EVERY,
    Handled,
    ended,
    liveness_of,
    serving,
    until_gone,
)
from tests.support.proxy import TcpProxy
from tests.support.waiting import until, within

LASTS = timedelta(milliseconds=300)
CLAIM_FOR = timedelta(milliseconds=300)


async def _process_line(process: asyncio.subprocess.Process) -> str:
    assert process.stdout is not None
    return (await within(process.stdout.readline())).decode().strip()


async def _start_process(*arguments: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.support.lease_process",
        *arguments,
        stdout=asyncio.subprocess.PIPE,
    )


async def test_the_lapse_of_a_killed_pod_reaches_one_handler_and_another_if_that_claimer_dies(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    client = make_client(redis_url)
    leases = new_leases(client, lasts=timedelta(seconds=2), claim_for=CLAIM_FOR)
    holder = await _start_process("renew", redis_url, key_prefix, "pod-a", "2000")
    claimer: asyncio.subprocess.Process | None = None
    try:
        assert await _process_line(holder) == "RENEWED"
        holder.send_signal(signal.SIGKILL)
        assert await liveness_of(leases, "pod-a") is Liveness.ALIVE

        claimer = await _start_process("serve", redis_url, key_prefix, "300")
        assert await _process_line(claimer) == "HANDLING pod-a"
        handled = Handled()
        async with serving(leases, handled):
            await until_redis_ms(client, await redis_ms(client) + 1000)
            assert handled.lapses == []
            claimer.send_signal(signal.SIGKILL)
            (lapse,) = await handled.received()
        assert lapse.holder == "pod-a"
    finally:
        for process in (holder, claimer):
            if process is not None:
                if process.returncode is None:
                    process.kill()
                await process.wait()


async def test_a_handler_that_runs_past_claim_for_keeps_its_lapse_from_others_and_done_follows(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    client = make_client(redis_url)
    first = new_leases(client, lasts=LASTS, claim_for=CLAIM_FOR)
    second = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await first.renew("pod-a")
    running, other = Handled(hold=True), Handled()

    async with serving(first, running):
        await running.received()
        async with serving(second, other, claimer="the other"):
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)
            assert other.lapses == []
            running.release()
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)

    assert len(running.lapses) == 1
    assert other.lapses == []


async def test_a_connection_drop_is_ridden_out_with_back_off_and_a_raising_handler_leaves_the_loop(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    key_prefix: str,
    backoffs: Backoffs,
):
    through_proxy = make_client(proxy.url, retry=Retry(NoBackoff(), 0))
    serving_leases = BackoffRecordingLeases(
        through_proxy,
        key_prefix=key_prefix,
        family="owner",
        lasts=LASTS,
        claim_for=CLAIM_FOR,
        backoffs=backoffs,
    )
    renewer = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    attempts: list[str] = []

    async def failing_once(lapse: object) -> None:
        attempts.append("attempt")
        if len(attempts) == 1:
            raise RuntimeError("the handler fails")

    task = asyncio.create_task(
        serving_leases.serve_lapses(failing_once, claimer="the test", every=SERVING_EVERY)
    )
    try:
        proxy.cut()
        await backoffs.reached(2)
        assert not task.done()
        proxy.restore()
        await renewer.renew("pod-a")
        await renewer.renew("pod-b")

        async def twice() -> bool:
            return len(attempts) >= 2

        await until(twice)
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert backoffs.delays[0] == BACKOFF_FIRST_SECONDS
    assert backoffs.delays[1] > backoffs.delays[0]


async def test_a_refusal_ends_serve_lapses_with_store_error(
    url_denying: Callable[[str], Awaitable[str]],
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    leases = new_leases(make_client(await url_denying("evalsha")))
    task = asyncio.create_task(
        leases.serve_lapses(Handled(), claimer="the test", every=SERVING_EVERY)
    )

    assert isinstance(await ended(task), StoreError)


@pytest.mark.parametrize("lapses", [1, 25])
async def test_every_lapse_of_a_mass_lapse_reaches_a_handler(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    lapses: int,
):
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    holders = [f"pod-{number}" for number in range(lapses)]
    for holder in holders:
        await leases.renew(holder)
    handled = Handled()

    async with serving(leases, handled):
        await handled.received(lapses)

    assert sorted(lapse.holder for lapse in handled.lapses) == sorted(holders)


async def test_the_claims_of_a_held_batch_do_not_expire_and_each_lapse_is_handled_once(
    redis_url: str, make_client: Callable[..., Redis], new_leases: Callable[..., Leases]
):
    client = make_client(redis_url)
    first = new_leases(client, lasts=LASTS, claim_for=CLAIM_FOR)
    second = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    holders = ["pod-a", "pod-b", "pod-c"]
    for holder in holders:
        await first.renew(holder)
    for holder in holders:
        await until_gone(first, holder)
    running, other = Handled(hold=True), Handled()

    async with serving(first, running):
        await running.received()
        async with serving(second, other, claimer="the other"):
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)
            assert other.lapses == []
            running.release()
            await running.received(3)
            await until_redis_ms(client, await redis_ms(client) + 3 * 300)

    assert sorted(lapse.holder for lapse in running.lapses) == holders
    assert other.lapses == []


async def test_a_short_outage_is_ridden_out_with_real_waits_and_the_lapse_is_handled_after_it(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    through_proxy = new_leases(
        make_client(proxy.url, retry=Retry(NoBackoff(), 0)), lasts=LASTS, claim_for=CLAIM_FOR
    )
    renewer = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    handled = Handled()

    async with serving(through_proxy, handled):
        proxy.cut()
        await proxy.attempts_reached(1)
        proxy.restore()
        await renewer.renew("pod-a")
        await handled.received()


async def test_a_claim_that_runs_out_leaves_the_handler_running(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
):
    client = make_client(redis_url)
    leases = new_leases(client, lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")
    running = Handled(hold=True)

    async with serving(leases, running) as task:
        await running.received()
        await observer.delete(lease_keys(key_prefix, "owner").lapse.claim)
        await until_redis_ms(client, await redis_ms(client) + 300)
        running.release()
        await until_redis_ms(client, await redis_ms(client) + 300)
        assert not task.done()


async def test_a_claim_that_cannot_be_extended_for_an_outage_leaves_the_handler_running(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
):
    client = make_client(redis_url)
    through_proxy = new_leases(
        make_client(proxy.url, retry=Retry(NoBackoff(), 0)), lasts=LASTS, claim_for=CLAIM_FOR
    )
    await new_leases(client, lasts=LASTS).renew("pod-a")
    running = Handled(hold=True)

    async with serving(through_proxy, running) as task:
        await running.received()
        proxy.cut()
        await until_redis_ms(client, await redis_ms(client) + 300)
        proxy.restore()
        running.release()
        await until_redis_ms(client, await redis_ms(client) + 300)
        assert not task.done()
