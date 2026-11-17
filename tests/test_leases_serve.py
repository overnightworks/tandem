import asyncio
import signal
import sys
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from logline import Outcome
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from tandem import Lapse, Leases, Liveness, StoreError
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
from tests.support.logs import LogLines
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
        async with serving(second, other):
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

    task = asyncio.create_task(serving_leases.serve_lapses(failing_once, every=SERVING_EVERY))
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
    task = asyncio.create_task(leases.serve_lapses(Handled(), every=SERVING_EVERY))

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
        async with serving(second, other):
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


_LOGGER = "tandem.leases"


@pytest.mark.parametrize(
    ("raised", "level", "outcome"),
    [(None, "info", Outcome.OK), (ValueError, "error", Outcome.FAILED)],
)
async def test_a_lapse_handling_writes_one_line_with_its_outcome(
    raised: type[Exception] | None,
    level: str,
    outcome: Outcome,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")

    async def handle(lapse: object) -> None:
        if raised is not None:
            raise raised("the handler fails")

    async with serving(leases, handle):
        (line, *_) = await log_lines.await_of("lapse_handling.ended", _LOGGER)

    assert line["level"] == level
    assert line["outcome"] == outcome
    assert line["lease_family"] == "owner"
    assert line["holder_id"] == "pod-a"
    assert line["duration_ms"] >= 0
    assert line.get("error_type") == (raised.__name__ if raised else None)
    assert ("Traceback" in line.get("exception", "")) == (raised is not None)


def _store_error_raised_directly() -> Exception:
    return StoreError("a refusal naming the store")


def _store_error_behind_a_host_error() -> Exception:
    try:
        raise StoreError("a refusal naming the store")
    except StoreError as refusal:
        wrapped = ValueError("the host gave up")
        wrapped.__cause__ = refusal
        return wrapped


def _store_error_inside_a_group() -> Exception:
    return ExceptionGroup("the host's tasks failed", [StoreError("a refusal naming the store")])


def _host_error_raised_from_another() -> Exception:
    try:
        try:
            raise KeyError("the host's own failure")
        except KeyError as first:
            raise ValueError("the host gave up") from first
    except ValueError as wrapped:
        return wrapped


@pytest.mark.parametrize(
    ("failure", "error_type", "writes_traceback"),
    [
        (_store_error_raised_directly, StoreError, False),
        (_store_error_behind_a_host_error, ValueError, False),
        (_store_error_inside_a_group, ExceptionGroup, False),
        (_host_error_raised_from_another, ValueError, True),
    ],
)
async def test_a_failed_lapse_handler_writes_a_traceback_unless_a_store_error_is_in_it(
    failure: Callable[[], Exception],
    error_type: type[Exception],
    writes_traceback: bool,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")

    async def handle(lapse: object) -> None:
        raise failure()

    async with serving(leases, handle):
        (line, *_) = await log_lines.await_of("lapse_handling.ended", _LOGGER)

    assert line["level"] == "error"
    assert line["outcome"] == Outcome.FAILED
    assert line["error_type"] == error_type.__name__
    assert ("exception" in line) is writes_traceback
    log_lines.assert_none_holds("a refusal naming the store")


async def test_each_lapse_handling_runs_under_its_own_trace_that_ends_with_it(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")
    await leases.renew("pod-b")
    handled = Handled()

    async with serving(leases, handled):
        handling = await log_lines.await_of("lapse_handling.ended", _LOGGER, count=2)
        rounds = await log_lines.await_of("lapse_round.ended", _LOGGER, count=2)

    assert len({line["trace_id"] for line in handling}) == len(handling)
    assert all(line["trace_id"] is not None for line in handling)
    assert all(line["trace_id"] is None for line in rounds)


async def test_a_round_writes_its_counts_at_info_with_work_and_at_debug_without(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")
    await leases.renew("pod-b")

    async def handle(lapse: Lapse) -> None:
        if lapse.holder == "pod-a":
            raise ValueError("the handler fails")

    async def a_round_worked() -> bool:
        return any(line["lapse_count"] for line in log_lines.of("lapse_round.ended", _LOGGER))

    async with serving(leases, handle):
        await until(a_round_worked)

    rounds = log_lines.of("lapse_round.ended", _LOGGER)
    worked = [line for line in rounds if line["lapse_count"] > 0]
    idle = [line for line in rounds if line["lapse_count"] == 0]
    assert idle
    assert {(line["level"], line["failed_count"]) for line in idle} == {("debug", 0)}
    assert worked[0]["level"] == "info"
    assert (worked[0]["lapse_count"], worked[0]["failed_count"]) == (2, 1)
    assert all(line["outcome"] == Outcome.OK and line["duration_ms"] >= 0 for line in rounds)


async def test_an_extension_the_acl_refuses_is_logged_with_its_reason(
    url_denying: Callable[[str], Awaitable[str]],
    redis_url: str,
    observer: Redis,
    key_prefix: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    await new_leases(make_client(redis_url), lasts=LASTS).renew("pod-a")
    leases = new_leases(
        make_client(await url_denying("flushall")), lasts=LASTS, claim_for=CLAIM_FOR
    )
    running = Handled(hold=True)

    async with serving(leases, running):
        await running.received()
        await observer.acl_setuser(key_prefix, enabled=True, commands=["-evalsha"])
        (line,) = await log_lines.await_of("lapse_claim_extension.failed", _LOGGER)
        await log_lines.await_of("lapse_claim.expired", _LOGGER)
        running.release()

    assert line["level"] == "warning"
    assert line["reason"] == "refused"
    assert line["error_type"] == StoreError.__name__
    assert line["lease_family"] == "owner"
    assert line["holder_id"] == "pod-a"
    claim_events = [
        written["event"]
        for written in log_lines.of_logger(_LOGGER)
        if written["event"].startswith("lapse_claim")
    ]
    assert claim_events == ["lapse_claim_extension.failed", "lapse_claim.expired"]


async def test_an_extension_lost_to_an_outage_is_logged_but_never_declared_expired(
    proxy: TcpProxy,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    await new_leases(make_client(redis_url), lasts=LASTS).renew("pod-a")
    leases = new_leases(
        make_client(proxy.url, retry=Retry(NoBackoff(), 0)), lasts=LASTS, claim_for=CLAIM_FOR
    )
    running = Handled(hold=True)

    async with serving(leases, running):
        await running.received()
        proxy.cut()
        (line,) = await log_lines.await_of("lapse_claim_extension.failed", _LOGGER)
        loop = asyncio.get_running_loop()
        claim_over_at = loop.time() + 2 * CLAIM_FOR.total_seconds()

        async def claim_is_over() -> bool:
            return loop.time() >= claim_over_at

        await until(claim_is_over)
        running.release()

    assert line["reason"] == "outage"
    assert not log_lines.of("lapse_claim.expired", _LOGGER)


async def test_a_claim_that_runs_out_is_logged_without_its_generation(
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    new_leases: Callable[..., Leases],
    key_prefix: str,
    log_lines: LogLines,
):
    leases = new_leases(make_client(redis_url), lasts=LASTS, claim_for=CLAIM_FOR)
    await leases.renew("pod-a")
    running = Handled(hold=True)

    async with serving(leases, running):
        (lapse,) = await running.received()
        await observer.delete(lease_keys(key_prefix, "owner").lapse.claim)
        (line,) = await log_lines.await_of("lapse_claim.expired", _LOGGER)
        running.release()

    assert line["level"] == "warning"
    assert line["lease_family"] == "owner"
    assert line["holder_id"] == "pod-a"
    log_lines.assert_none_holds(lapse.generation)


async def test_an_outage_is_logged_per_failed_round_and_a_refusal_writes_no_line(
    proxy: TcpProxy,
    url_denying: Callable[[str], Awaitable[str]],
    observer: Redis,
    make_client: Callable[..., Redis],
    key_prefix: str,
    backoffs: Backoffs,
    log_lines: LogLines,
):
    await url_denying("flushall")
    through_proxy = make_client(
        proxy.url.replace("redis://", f"redis://{key_prefix}@"), retry=Retry(NoBackoff(), 0)
    )
    leases = BackoffRecordingLeases(
        through_proxy,
        key_prefix=key_prefix,
        family="owner",
        lasts=LASTS,
        claim_for=CLAIM_FOR,
        backoffs=backoffs,
    )
    task = asyncio.create_task(leases.serve_lapses(Handled(), every=SERVING_EVERY))
    try:
        proxy.cut()
        await backoffs.reached(2)
        proxy.restore()
        await log_lines.await_of("lapse_round.ended", _LOGGER, count=1)

        async def recovered() -> bool:
            rounds = log_lines.of("lapse_round.ended", _LOGGER)
            return bool(rounds) and rounds[-1]["outcome"] == Outcome.OK

        await until(recovered)
        written = len(log_lines.of_logger(_LOGGER))
        await observer.acl_setuser(key_prefix, enabled=True, commands=["-evalsha"])
        assert isinstance(await ended(task), StoreError)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    failed = [
        line
        for line in log_lines.of("lapse_round.ended", _LOGGER)
        if line["outcome"] == Outcome.FAILED
    ]
    assert len(failed) >= 2
    assert {(line["level"], line["lapse_count"]) for line in failed} == {("warning", 0)}
    refusal_period = log_lines.of_logger(_LOGGER)[written:]
    assert not [line for line in refusal_period if line["level"] in ("warning", "error")]
