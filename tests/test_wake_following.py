import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import aclosing
from datetime import timedelta

import pytest
from logline import current_traceparent, unit_trace
from redis.asyncio import Redis

from tandem import StoreError, Wake, WakeChannel
from tandem.wake import _Resubscribed
from tests.support.logs import LogLines
from tests.support.proxy import TcpProxy
from tests.support.waiting import BOUND, until

_TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
_INCOMING_TRACEPARENT = f"00-{_TRACE_ID}-b7ad6b7169203331-01"


class Handlers:
    """Host handlers that record what they were called with, and can be told to raise."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.traces: list[str | None] = []
        self.failures: list[Exception] = []
        self.raise_in_resync: Exception | None = None
        self.raise_in_wake: Exception | None = None
        self.raise_in_failed: Exception | None = None

    def on_resync(self) -> None:
        self.calls.append("resync")
        if self.raise_in_resync is not None:
            raise self.raise_in_resync

    def on_wake(self, wake: Wake) -> None:
        self.calls.append(wake.address)
        self.traces.append(current_traceparent())
        if self.raise_in_wake is not None:
            raise self.raise_in_wake

    def on_failed(self, error: Exception) -> None:
        self.failures.append(error)
        if self.raise_in_failed is not None:
            raise self.raise_in_failed

    def following(self, channel: WakeChannel, *, ready_within: timedelta = BOUND):
        return channel.following(
            on_resync=self.on_resync,
            on_wake=self.on_wake,
            on_failed=self.on_failed,
            ready_within=ready_within,
        )


async def test_entry_returns_after_the_first_resync_was_handled(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    handlers = Handlers()
    channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)

    async with handlers.following(channel):
        assert handlers.calls == ["resync"]


async def test_entry_raises_an_outage_when_the_subscription_is_not_confirmed_in_time(
    proxy: TcpProxy, make_client: Callable[..., Redis], key_prefix: str
):
    handlers = Handlers()
    channel = WakeChannel(make_client(proxy.url), key_prefix=key_prefix)
    proxy.cut()
    tasks_before = asyncio.all_tasks()
    following = handlers.following(channel, ready_within=timedelta(seconds=0.2))

    with pytest.raises(StoreError) as raised:
        async with following:
            pass

    assert raised.value.outage
    assert handlers.calls == []
    assert asyncio.all_tasks() <= tasks_before | proxy.tasks


async def test_a_handler_exception_before_the_entry_returns_raises_out_of_the_entry(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    handlers = Handlers()
    handlers.raise_in_resync = RuntimeError("boom")
    channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)

    following = handlers.following(channel)

    with pytest.raises(RuntimeError, match="boom"):
        async with following:
            pass

    assert handlers.failures == []


async def test_a_wake_is_handled_inside_the_trace_of_its_sender(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    handlers = Handlers()
    channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    async with handlers.following(channel):
        with unit_trace(_INCOMING_TRACEPARENT):
            await channel.publish("orders/7")
        await until(_has_handled(handlers, 1))

    (handling_trace,) = handlers.traces
    assert handling_trace is not None
    assert handling_trace.split("-")[1] == _TRACE_ID
    assert current_traceparent() != handling_trace


async def test_wakes_are_handled_one_at_a_time_in_arrival_order(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    handlers = Handlers()
    channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    addresses = [str(number) for number in range(20)]
    async with handlers.following(channel):
        for address in addresses:
            await channel.publish(address)
        await until(_has_handled(handlers, len(addresses)))

    assert handlers.calls == ["resync", *addresses]


@pytest.mark.parametrize("failing_handler", ["on_wake", "on_resync"])
async def test_a_handler_exception_after_the_entry_ends_the_follow_with_one_line_and_one_on_failed(
    failing_handler: str,
    redis_url: str,
    make_client: Callable[..., Redis],
    observer: Redis,
    key_prefix: str,
    log_lines: LogLines,
):
    handlers = Handlers()
    witness = Handlers()
    secret = "address-the-handler-choked-on"
    followed_client = make_client(redis_url)
    channel = WakeChannel(followed_client, key_prefix=key_prefix)
    witness_channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    async with witness.following(witness_channel), handlers.following(channel):
        if failing_handler == "on_wake":
            handlers.raise_in_wake = RuntimeError(secret)
            await channel.publish("orders/7")
        else:
            handlers.raise_in_resync = RuntimeError(secret)
            await _kill_subscription_of(observer, followed_client)
        await until(_has_failed(handlers))
        await channel.publish("after the failure")
        await until(_has_called(witness, "after the failure"))

    (error,) = handlers.failures
    assert isinstance(error, RuntimeError)
    assert "after the failure" not in handlers.calls
    (failed,) = log_lines.of("wake_follow.failed", "tandem.wake")
    assert failed["error_type"] == "RuntimeError"
    assert failed["level"] == "error"
    log_lines.assert_none_holds(secret)


async def _kill_subscription_of(observer: Redis, client: Redis) -> None:
    name = await client.client_getname()
    for connection in await observer.client_list(_type="pubsub"):
        if connection["name"] == name:
            await observer.client_kill_filter(_id=connection["id"])


def _has_called(handlers: Handlers, call: str) -> Callable[[], Awaitable[bool]]:
    async def called() -> bool:
        return call in handlers.calls

    return called


def _has_handled(handlers: Handlers, wake_count: int) -> Callable[[], Awaitable[bool]]:
    async def handled() -> bool:
        return len(handlers.traces) >= wake_count

    return handled


def _has_failed(handlers: Handlers) -> Callable[[], Awaitable[bool]]:
    async def failed() -> bool:
        return bool(handlers.failures)

    return failed


class _WakeBufferedWithTheFirstConfirmation(WakeChannel):
    """A channel whose first confirmation is followed at once by a wake, as one read can bring."""

    async def _received(self) -> AsyncGenerator[Wake | _Resubscribed]:
        buffered = True
        async with aclosing(super()._received()) as events:
            async for event in events:
                yield event
                if buffered:
                    buffered = False
                    yield Wake("orders/7")


async def test_a_wake_buffered_with_the_first_confirmation_is_handled_after_the_entry_returned(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    handlers = Handlers()
    handlers.raise_in_wake = RuntimeError("boom")
    channel = _WakeBufferedWithTheFirstConfirmation(make_client(redis_url), key_prefix=key_prefix)

    async with handlers.following(channel):
        assert handlers.calls == ["resync"]
        assert handlers.failures == []
        await until(_has_failed(handlers))

    assert handlers.calls == ["resync", "orders/7"]


async def test_an_on_failed_that_raises_is_raised_out_of_the_exit(
    redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    handlers = Handlers()
    handlers.raise_in_wake = RuntimeError("the handler")
    handlers.raise_in_failed = ValueError("the host's own error")
    channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    following = handlers.following(channel)

    with pytest.raises(ValueError, match="the host's own error"):
        async with following:
            await channel.publish("orders/7")
            await until(_has_failed(handlers))


@pytest.mark.parametrize("ready_within", [timedelta(0), timedelta(seconds=-1), 10, 10.0])
async def test_a_ready_within_that_is_no_positive_timedelta_is_refused_naming_the_argument(
    ready_within: object, redis_url: str, make_client: Callable[..., Redis], key_prefix: str
):
    handlers = Handlers()
    channel = WakeChannel(make_client(redis_url), key_prefix=key_prefix)
    following = handlers.following(channel, ready_within=ready_within)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="ready_within"):
        async with following:
            pytest.fail("the entry returned although ready_within was refused")

    assert handlers.calls == []
