import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff, NoBackoff

from tandem import Resubscribed, Wake, WakeChannel
from tests.conftest import Backoffs
from tests.support.proxy import TcpProxy
from tests.support.reader import Reader
from tests.support.waiting import BOUND_SECONDS, until, within

REPLACEMENT_MESSAGES = 20


@dataclass
class Disruption:
    """One way to lose the subscription, and how the replacement is let proceed."""

    retry: Retry
    begin: Callable[[TcpProxy, Redis, list[int]], Awaitable[None]]
    release_when: Callable[[Backoffs], Awaitable[None]]


async def _kill_and_hold(proxy: TcpProxy, observer: Redis, connection_ids: list[int]) -> None:
    proxy.hold()
    await observer.client_kill_filter(_id=str(connection_ids[0]))


async def _cut(proxy: TcpProxy, observer: Redis, connection_ids: list[int]) -> None:
    proxy.cut()


async def _at_once(backoffs: Backoffs) -> None:
    return None


async def _after_the_retry_budget(backoffs: Backoffs) -> None:
    await backoffs.reached(1)


DISRUPTIONS = {
    "killed": Disruption(Retry(ConstantBackoff(0.05), 100), _kill_and_hold, _at_once),
    "dropped inside the retry budget": Disruption(
        Retry(ConstantBackoff(0.05), 100), _cut, _at_once
    ),
    "dropped beyond the retry budget": Disruption(
        Retry(NoBackoff(), 1), _cut, _after_the_retry_budget
    ),
}


@pytest.mark.parametrize("disruption", DISRUPTIONS.values(), ids=DISRUPTIONS.keys())
async def test_every_message_of_the_replacement_subscription_follows_its_resubscribed(
    disruption: Disruption,
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
    observer: Redis,
    redis_url: str,
    key_prefix: str,
    backoffs: Backoffs,
):
    reader_client = make_client(f"redis://127.0.0.1:{proxy.port}", retry=disruption.retry)
    publisher = make_client(redis_url)
    sent: list[tuple[int, int]] = []  # the number and the receivers Redis counted for it
    zero_seen = asyncio.Event()
    stop = asyncio.Event()

    async def publish_without_pause(channel: str) -> None:
        number = 0
        while not stop.is_set():
            receivers = await publisher.publish(channel, str(number))
            sent.append((number, receivers))
            if receivers == 0:
                zero_seen.set()
            number += 1
            await asyncio.sleep(0)

    async with Reader(WakeChannel(reader_client, key_prefix=key_prefix)) as reader:
        await reader.next_resubscribed()
        (channel,) = await observer.pubsub_channels(f"{key_prefix}*")
        connection_ids = [
            int(connection["id"])
            for connection in await observer.client_list(_type="pubsub")
            if connection["name"].startswith(key_prefix)
        ]
        publishing = asyncio.create_task(publish_without_pause(str(channel)))
        try:
            await until(_published_with_a_receiver(sent))
            await disruption.begin(proxy, observer, connection_ids)
            await within(zero_seen.wait())
            await disruption.release_when(backoffs)
            proxy.restore()
            await until(_replacement_got(sent, REPLACEMENT_MESSAGES))
        finally:
            stop.set()
            await publishing

        first_zero = next(index for index, (_, receivers) in enumerate(sent) if receivers == 0)
        replacement = [number for number, receivers in sent[first_zero:] if receivers == 1]
        zeros = {number for number, receivers in sent if receivers == 0}
        assert replacement, "the publisher saw no receiver after the run of zero"

        yielded = await _read_until(reader, replacement[-1])

    assert sum(isinstance(event, Resubscribed) for event in yielded) == 1
    resubscribed_at = max(
        index for index, event in enumerate(yielded) if isinstance(event, Resubscribed)
    )
    after_resubscribed = [
        int(event.address) for event in yielded[resubscribed_at:] if isinstance(event, Wake)
    ]
    assert after_resubscribed == replacement
    assert zeros.isdisjoint(int(event.address) for event in yielded if isinstance(event, Wake))


def _published_with_a_receiver(sent: list[tuple[int, int]]) -> Callable[[], Awaitable[bool]]:
    async def condition() -> bool:
        return any(receivers == 1 for _, receivers in sent)

    return condition


def _replacement_got(sent: list[tuple[int, int]], count: int) -> Callable[[], Awaitable[bool]]:
    async def condition() -> bool:
        zeros = [index for index, (_, receivers) in enumerate(sent) if receivers == 0]
        return bool(zeros) and sum(receivers for _, receivers in sent[zeros[-1] :]) >= count

    return condition


async def _read_until(reader: Reader, last_number: int) -> list[Wake | Resubscribed]:
    yielded: list[Wake | Resubscribed] = []
    async with asyncio.timeout(BOUND_SECONDS):
        while True:
            event = await reader.next()
            yielded.append(event)
            if event == Wake(str(last_number)):
                return yielded
