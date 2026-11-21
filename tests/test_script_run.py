from collections.abc import Awaitable, Callable

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from tandem import LUA_KIT, Deadlines, Script, StoreError, Wake, WakeChannel, wake_channel
from tests.support.deadlines import KEYS_OF_THE_SCRIPT, key_names, redis_ms, state_of
from tests.support.proxy import TcpProxy
from tests.support.reader import Reader

_DEADLINE_AND_WAKE = (
    KEYS_OF_THE_SCRIPT
    + "tandem.deadline_set(keys, ARGV[1], ARGV[2])\nreturn tandem.wake(ARGV[3], ARGV[4])"
)


async def test_a_script_writes_a_deadline_and_a_wake_that_are_visible_together(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    key_prefix: str,
):
    """Spec: DEADLINE-10, LUA-7."""
    client = make_client(redis_url)
    deadlines = new_deadlines(client)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resynced()

        reached = await Script(client, _DEADLINE_AND_WAKE).run(
            key_names(deadlines), ["room-1", 60_000, wake_channel(key_prefix), "room-1"]
        )

        assert reached == 1
        assert await reader.next_wake() == Wake("room-1")
        state = await state_of(client, deadlines, "room-1")
        assert state is not None
        assert state[0] == "waiting"


@pytest.mark.parametrize(
    "traceparent",
    [None, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "not-a-trace"],
)
@pytest.mark.parametrize("address", ["orders/7", "12:orders", "caf\u00e9"])
async def test_a_script_wakes_with_the_trace_it_is_given_or_none(
    traceparent: str | None,
    address: str,
    redis_url: str,
    make_client: Callable[..., Redis],
    key_prefix: str,
):
    """Spec: WAKE-24, LUA-7."""
    client = make_client(redis_url)
    script = Script(client, "return tandem.wake(ARGV[1], ARGV[2], ARGV[3])")
    arguments: list[str | int | bytes] = [wake_channel(key_prefix), address]
    if traceparent is not None:
        arguments.append(traceparent)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resynced()

        reached = await script.run([], arguments)

        assert reached == 1
        assert await reader.next_wake() == Wake(address, traceparent)


async def test_a_script_that_fails_before_its_first_write_leaves_neither_deadline_nor_wake(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_deadlines: Callable[..., Deadlines],
    key_prefix: str,
):
    client = make_client(redis_url)
    deadlines = new_deadlines(client)
    async with Reader(WakeChannel(make_client(redis_url), key_prefix=key_prefix)) as reader:
        await reader.next_resynced()

        script = Script(client, _DEADLINE_AND_WAKE)
        names = key_names(deadlines)
        arguments: list[str | int | bytes] = [
            "room-1",
            "not a number",
            wake_channel(key_prefix),
            "room-1",
        ]

        with pytest.raises(StoreError):
            await script.run(names, arguments)

        await WakeChannel(client, key_prefix=key_prefix).publish("barrier")
        assert await reader.next_wake() == Wake("barrier")
        assert reader.drain() == []
    assert await state_of(client, deadlines, "room-1") is None


async def test_a_script_runs_again_after_the_servers_script_cache_is_flushed(
    restartable_redis_url: str, make_client: Callable[..., Redis]
):
    """Spec: SCRIPT-1."""
    client = make_client(restartable_redis_url)
    script = Script(client, "return 7")
    assert await script.run([], []) == 7

    await client.script_flush()

    assert await script.run([], []) == 7


async def test_a_script_that_redis_refuses_raises_store_error(
    url_denying: Callable[[str], Awaitable[str]], make_client: Callable[..., Redis]
):
    """Spec: SCRIPT-2, STORE-2."""
    script = Script(make_client(await url_denying("evalsha")), "return 7")

    with pytest.raises(StoreError) as refusal:
        await script.run([], [])

    assert isinstance(refusal.value.__cause__, RedisError)


async def test_a_script_whose_reply_never_comes_raises_store_error(
    proxy: TcpProxy, make_client: Callable[..., Redis]
):
    """Spec: SCRIPT-2."""
    client = make_client(proxy.url, retry=Retry(NoBackoff(), 0), socket_timeout=0.3)
    await client.ping()
    script = Script(client, "return 7")
    proxy.drop_replies()

    with pytest.raises(StoreError):
        await script.run([], [])


async def test_now_ms_is_the_redis_clock_in_milliseconds(
    redis_url: str, make_client: Callable[..., Redis]
):
    """Spec: LUA-1."""
    client = make_client(redis_url)
    before = await redis_ms(client)

    reported = await Script(client, "return tandem.now_ms()").run([], [])

    assert isinstance(reported, int)
    assert before <= reported <= await redis_ms(client)


async def test_a_script_that_fails_after_a_write_keeps_the_write(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    """Spec: SCRIPT-3."""
    client = make_client(redis_url)
    script = Script(client, "redis.call('SET', KEYS[1], 'written')\nerror('fails after the write')")

    with pytest.raises(StoreError):
        await script.run([f"{key_prefix}:kept"], [])

    assert await client.get(f"{key_prefix}:kept") == b"written"


_COUNT_RUNS = "if ARGV[1] == 'warm-up' then return 0 end\nreturn redis.call('INCR', KEYS[1])"


@pytest.mark.parametrize(("retries", "runs"), [(0, 1), (3, 2)])
async def test_the_clients_retry_alone_runs_a_script_again_after_a_lost_reply(
    retries: int,
    runs: int,
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
):
    """Spec: SCRIPT-3."""
    counter = f"{key_prefix}:runs"
    client = make_client(proxy.url, retry=Retry(NoBackoff(), retries), socket_timeout=0.3)
    script = Script(client, _COUNT_RUNS)
    await script.run([counter], ["warm-up"])  # loads the script, so no reply is lost as NOSCRIPT
    proxy.drop_replies_of_open_connections()

    if retries == 0:
        with pytest.raises(StoreError):
            await script.run([counter], ["run"])
    else:
        await script.run([counter], ["run"])

    assert int(await observer.get(counter) or 0) == runs


async def test_an_error_in_a_script_is_reported_at_a_line_offset_by_the_kit(
    redis_url: str, make_client: Callable[..., Redis]
):
    """Spec: SCRIPT-4."""
    script = Script(make_client(redis_url), "error('fails on the first line')")

    with pytest.raises(StoreError) as failure:
        await script.run([], [])

    assert f"user_script:{LUA_KIT.count(chr(10)) + 1}:" in str(failure.value)


async def test_a_script_that_starts_with_a_shebang_is_refused(
    redis_url: str, make_client: Callable[..., Redis]
):
    """Spec: SCRIPT-4."""
    script = Script(make_client(redis_url), "#!lua\nreturn 1")

    with pytest.raises(StoreError):
        await script.run([], [])
