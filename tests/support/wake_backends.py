"""The two wake channels the shared `following` cells run against, and the controls each offers.

The cells prove one contract: `WakeChannel` on a real Redis and the in-memory fake of
`tandem.testing` must pass them alike. Each backend forces on its own side what Redis would do.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from redis.asyncio import Redis

from tandem import WakeChannel
from tandem.testing import FakeWakeBus, FakeWakeChannel
from tests.support.proxy import TcpProxy

type Channel = WakeChannel | FakeWakeChannel


class WakeBackend(Protocol):
    def new_channel(self) -> Channel:
        """Another pod of the same app."""
        ...

    def unconfirmable_channel(self) -> Channel:
        """A pod whose subscription Redis never confirms."""
        ...

    async def refusable_channel(self) -> Channel:
        """A pod whose subscription `refuse` can make Redis refuse, before or after its entry."""
        ...

    async def resync(self, channel: Channel) -> None:
        """Make the following of `channel` confirm again, as after a lost connection."""
        ...

    async def refuse(self, channel: Channel) -> None: ...

    @property
    def background_tasks(self) -> set[object]:
        """The tasks of the backend that outlive a following by design."""
        ...


class FakeBackend:
    def __init__(self) -> None:
        self._bus = FakeWakeBus()

    def new_channel(self) -> FakeWakeChannel:
        return self._bus.pod()

    def unconfirmable_channel(self) -> FakeWakeChannel:
        channel = self._bus.pod()
        channel.withhold_confirmation()
        return channel

    async def refusable_channel(self) -> FakeWakeChannel:
        return self._bus.pod()

    async def resync(self, channel: Channel) -> None:
        assert isinstance(channel, FakeWakeChannel)
        channel.resync()

    async def refuse(self, channel: Channel) -> None:
        assert isinstance(channel, FakeWakeChannel)
        channel.refuse()

    @property
    def background_tasks(self) -> set[object]:
        return set()


@dataclass(frozen=True)
class RedisFixtures:
    """What a test on the shared Redis is given; the `wake` fixture takes them only for it."""

    redis_url: str
    key_prefix: str
    make_client: Callable[..., Redis]
    observer: Redis
    proxy: TcpProxy
    url_denying: Callable[[str], Awaitable[str]]


class RedisBackend:
    def __init__(self, fixtures: RedisFixtures) -> None:
        self._fixtures = fixtures
        self._clients: dict[WakeChannel, Redis] = {}
        self._proxy_used = False

    def _channel(self, url: str) -> WakeChannel:
        client = self._fixtures.make_client(url)
        channel = WakeChannel(client, key_prefix=self._fixtures.key_prefix)
        self._clients[channel] = client
        return channel

    def new_channel(self) -> WakeChannel:
        return self._channel(self._fixtures.redis_url)

    def unconfirmable_channel(self) -> WakeChannel:
        channel = self._channel(self._fixtures.proxy.url)
        self._fixtures.proxy.cut()
        self._proxy_used = True
        return channel

    async def refusable_channel(self) -> WakeChannel:
        return self._channel(await self._fixtures.url_denying("flushall"))

    async def resync(self, channel: Channel) -> None:
        assert isinstance(channel, WakeChannel)
        observer = self._fixtures.observer
        name = await self._clients[channel].client_getname()
        for connection in await observer.client_list(_type="pubsub"):
            if connection["name"] == name:
                await observer.client_kill_filter(_id=connection["id"])

    async def refuse(self, channel: Channel) -> None:
        await self._fixtures.observer.acl_setuser(
            self._fixtures.key_prefix, enabled=True, reset_channels=True
        )

    @property
    def background_tasks(self) -> set[object]:
        return set(self._fixtures.proxy.tasks) if self._proxy_used else set()
