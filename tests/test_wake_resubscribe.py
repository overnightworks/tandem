import asyncio
import contextlib
import sys
from collections.abc import Callable

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff, NoBackoff
from redis.exceptions import ConnectionError as RedisConnectionError

from tandem import Wake, WakeChannel
from tandem.errors import BACKOFF_CAP_SECONDS, BACKOFF_FIRST_SECONDS
from tests.support.backoffs import BackoffRecordingChannel, Backoffs
from tests.support.host import ScenarioHost
from tests.support.logs import LogLines
from tests.support.proxy import TcpProxy
from tests.support.reader import Reader
from tests.support.waiting import within


async def _reader_connection_ids(observer: Redis, key_prefix: str) -> list[int]:
    """The ids of the subscription connections that belong to this test."""
    connections = await observer.client_list(_type="pubsub")
    return [
        int(connection["id"])
        for connection in connections
        if connection["name"].startswith(key_prefix)
    ]


async def test_a_killed_subscription_is_resubscribed_once_and_wakes_continue(
    redis_url: str, make_client: Callable[..., Redis], observer: Redis, key_prefix: str
):
    """Spec: WAKE-11, WAKE-26."""
    channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    publisher = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    async with Reader(channel) as reader:
        await reader.next_resynced()
        (connection_id,) = await _reader_connection_ids(observer, key_prefix)
        await observer.client_kill_filter(_id=str(connection_id))

        await reader.next_resynced()
        await publisher.publish("next")

        assert await reader.next_wake() == Wake("next")
        assert reader.drain() == []


async def test_a_drop_inside_the_retry_budget_yields_one_resubscribed(
    proxy: TcpProxy, make_client: Callable[..., Redis], key_prefix: str, redis_url: str
):
    """redis-py's own re-subscription confirms through the reader, so tandem adds no copy of it.

    Spec: WAKE-26.
    """
    retry = Retry(ConstantBackoff(0.05), 100)
    client = make_client(proxy.url, retry=retry)
    pool, connection_settings = (
        client.connection_pool,
        dict(client.connection_pool.connection_kwargs),
    )
    publisher = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    async with Reader(WakeChannel(client, key_prefix=key_prefix)) as reader:
        await reader.next_resynced()
        # A pooled connection makes the plain command fail inside the Retry; redis-py 5.x does not
        # retry a handshake that fails on a fresh connection.
        await client.ping()
        proxy.cut()
        plain_command = asyncio.ensure_future(client.ping())
        await proxy.attempts_reached(3)
        proxy.restore()

        assert await within(plain_command) is True
        await reader.next_resynced()
        await publisher.publish("next")
        assert await reader.next_wake() == Wake("next")
        assert reader.drain() == []
    assert client.get_retry() is retry
    assert client.connection_pool is pool
    assert client.connection_pool.connection_kwargs == connection_settings


async def test_an_outage_beyond_the_retry_budget_ends_in_one_resubscribed_with_capped_back_offs(
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    key_prefix: str,
    redis_url: str,
    backoffs: Backoffs,
    log_lines: LogLines,
):
    """Spec: WAKE-26, LOG-14, LOG-15."""
    client = make_client(proxy.url, retry=Retry(NoBackoff(), 1))
    publisher = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    channel = BackoffRecordingChannel(client, key_prefix=key_prefix, backoffs=backoffs)
    async with Reader(channel) as reader:
        await reader.next_resynced()
        proxy.cut()
        await backoffs.reached(6)
        assert reader.running
        proxy.restore()

        await reader.next_resynced()
        await publisher.publish("next")
        assert await reader.next_wake() == Wake("next")
        assert reader.drain() == []
    subscription = [
        line
        for line in log_lines.of_logger("tandem.wake")
        if line["event"].startswith("wake_subscription.")
    ]
    lost = [line for line in subscription if line["event"] == "wake_subscription.lost"]
    assert [line["event"] for line in (subscription[0], subscription[-1])] == [
        "wake_subscription.confirmed"
    ] * 2
    assert len(lost) >= 6
    assert [line["level"] for line in lost].count("warning") == 1
    assert lost[0]["level"] == "warning"
    assert all(line["level"] in ("warning", "debug") for line in lost)
    first, cap = BACKOFF_FIRST_SECONDS, BACKOFF_CAP_SECONDS
    assert backoffs.delays[0] == first
    assert max(backoffs.delays) == cap
    assert all(
        later == min(earlier * 2, cap)
        for earlier, later in zip(backoffs.delays, backoffs.delays[1:], strict=False)
    )


async def test_a_redis_restart_is_resubscribed_once_and_wakes_continue(
    restartable_redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    """Spec: WAKE-26."""
    client = make_client(restartable_redis_url)
    channel = WakeChannel(client, key_prefix=key_prefix)
    async with Reader(channel) as reader:
        await reader.next_resynced()
        with contextlib.suppress(RedisConnectionError):
            await client.execute_command("SHUTDOWN", "NOSAVE")

        await reader.next_resynced()
        await channel.publish("later")
        assert await reader.next_wake() == Wake("later")
        assert reader.drain() == []


async def test_state_changed_while_disconnected_is_read_again_at_the_next_resynced(
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    key_prefix: str,
    redis_url: str,
    observer: Redis,
):
    """Spec: WAKE-11."""
    writer_client = make_client(redis_url)
    reader_client = make_client(proxy.url, retry=Retry(ConstantBackoff(0.05), 100))
    channel = WakeChannel(reader_client, key_prefix=key_prefix)
    host = ScenarioHost(writer_client, key_prefix)
    async with Reader(channel) as reader:
        await host.handle(await reader.next_resynced())
        proxy.cut()
        await proxy.attempts_reached(1)
        await host.commit("orders/7", "shipped")
        await WakeChannel(writer_client, key_prefix=key_prefix).publish("orders/7")  # lost
        assert host.served == {}
        proxy.restore()

        await host.handle(await reader.next_resynced())

        assert host.served == {"orders/7": "shipped"}
        assert reader.drain() == []


async def test_two_reader_processes_both_get_the_wake_of_a_third(redis_url: str, key_prefix: str):
    """Spec: WAKE-20."""
    started: list[asyncio.subprocess.Process] = []

    async def start(command: str, *address: str) -> asyncio.subprocess.Process:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "tests.support.wake_process",
            command,
            redis_url,
            key_prefix,
            *address,
            stdout=asyncio.subprocess.PIPE,
        )
        started.append(process)
        return process

    try:
        readers = [await start("read") for _ in range(2)]
        for process in readers:
            assert process.stdout is not None
            assert (await within(process.stdout.readline())).strip() == b"RESYNCED"
        writer = await start("write", "shared")
        await within(writer.wait())
        for process in readers:
            assert process.stdout is not None
            assert (await within(process.stdout.readline())).strip() == b"WAKE shared"
    finally:
        for process in started:
            if process.returncode is None:
                process.kill()
            await process.wait()
