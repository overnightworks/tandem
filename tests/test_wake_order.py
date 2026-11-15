import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff, NoBackoff

from tandem import Resubscribed, Wake
from tests.support.backoffs import BackoffRecordingChannel, Backoffs
from tests.support.numbered_publisher import NumberedPublisher
from tests.support.proxy import TcpProxy
from tests.support.reader import Reader
from tests.support.waiting import BOUND_SECONDS, until, within

REPLACEMENT_MESSAGES = 20


@dataclass
class Disruption:
    """One way to lose the subscription, and when its replacement is let proceed."""

    retry: Retry
    begin: Callable[[TcpProxy, Redis, str], Awaitable[None]]
    release_when: Callable[[Backoffs], Awaitable[None]]


async def _kill_and_hold(proxy: TcpProxy, observer: Redis, key_prefix: str) -> None:
    proxy.hold()
    for connection in await observer.client_list(_type="pubsub"):
        if connection["name"].startswith(key_prefix):
            await observer.client_kill_filter(_id=connection["id"])


async def _cut(proxy: TcpProxy, observer: Redis, key_prefix: str) -> None:
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
    reader_client = make_client(proxy.url, retry=disruption.retry)
    reader_channel = BackoffRecordingChannel(
        reader_client, key_prefix=key_prefix, backoffs=backoffs
    )
    async with Reader(reader_channel) as reader:
        await reader.next_resubscribed()
        (channel,) = await observer.pubsub_channels(f"{key_prefix}*")
        publisher = NumberedPublisher(make_client(redis_url), str(channel))
        publisher.start()
        try:
            await until(publisher.has_had_a_receiver)
            await disruption.begin(proxy, observer, key_prefix)
            await within(publisher.zero_seen.wait())
            await disruption.release_when(backoffs)
            proxy.restore()
            await until(lambda: publisher.replacement_has_got(REPLACEMENT_MESSAGES))
        finally:
            await publisher.stop()

        replacement = publisher.numbers_after_the_zeros()
        assert replacement, "the publisher saw no receiver after the run of zero"
        yielded = await _read_until(reader, Wake(str(replacement[-1])))

    resubscribed = [index for index, event in enumerate(yielded) if isinstance(event, Resubscribed)]
    assert len(resubscribed) == 1
    wakes = [int(event.address) for event in yielded if isinstance(event, Wake)]
    assert yielded[resubscribed[0] + 1 :] == [Wake(str(number)) for number in replacement]
    assert publisher.numbers_with_no_receiver().isdisjoint(wakes)


async def _read_until(reader: Reader, last: Wake) -> list[Wake | Resubscribed]:
    yielded: list[Wake | Resubscribed] = []
    async with asyncio.timeout(BOUND_SECONDS):
        while last not in yielded:
            yielded.append(await reader.next())
    return yielded
