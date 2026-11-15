import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from fakeredis import FakeAsyncRedis, FakeServer
from redis.asyncio import Redis
from redis.asyncio.retry import Retry

from tests.support.backends import Backend
from tests.support.backoffs import Backoffs
from tests.support.proxy import TcpProxy


def _required_url(variable: str) -> str:
    url = os.environ.get(variable)
    if url is None:
        pytest.fail(f"{variable} is unset: run the tests through scripts/run_redis_tests.sh")
    return url


@pytest.fixture
def redis_url() -> str:
    return _required_url("TANDEM_TEST_REDIS_URL")


@pytest.fixture
def key_prefix() -> str:
    """A prefix of its own, so tests share one server without sharing keys or channels."""
    return f"t-{uuid.uuid4().hex[:12]}"


@pytest.fixture
async def proxy(redis_url: str) -> AsyncIterator[TcpProxy]:
    host_and_port = redis_url.removeprefix("redis://")
    host, port = host_and_port.split(":")
    running = TcpProxy(host, int(port))
    await running.start()
    yield running
    await running.stop()


@pytest.fixture
async def make_client(key_prefix: str) -> AsyncIterator[Callable[..., Redis]]:
    """Clients named after the test, so CLIENT LIST finds exactly this test's connections."""
    made: list[Redis] = []

    def make(
        url: str,
        *,
        retry: Retry | None = None,
        socket_timeout: float | None = None,
        health_check_interval: float = 0,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        **client_options: Any,
    ) -> Redis:
        options: dict[str, Any] = {
            "socket_timeout": socket_timeout,
            "health_check_interval": health_check_interval,
            "encoding": encoding,
            "decode_responses": decode_responses,
            **client_options,
        }
        if retry is not None:
            options["retry"] = retry
        client = Redis.from_url(url, client_name=f"{key_prefix}-{len(made)}", **options)
        made.append(client)
        return client

    yield make
    for client in made:
        await client.aclose()


@pytest.fixture
async def observer(redis_url: str) -> AsyncIterator[Redis]:
    """A client for reading server state; its connection carries no test name."""
    client = Redis.from_url(redis_url, decode_responses=True)
    yield client
    await client.aclose()


class _ProxyOutage:
    def __init__(self, proxy: TcpProxy) -> None:
        self._proxy = proxy

    def begin(self) -> None:
        self._proxy.cut()

    def end(self) -> None:
        self._proxy.restore()


class _FakeServerOutage:
    def __init__(self, server: FakeServer) -> None:
        self._server = server

    def begin(self) -> None:
        self._server.connected = False

    def end(self) -> None:
        self._server.connected = True


@pytest.fixture(params=["redis", "fakeredis"])
async def backend(
    request: pytest.FixtureRequest,
    redis_url: str,
    proxy: TcpProxy,
    make_client: Callable[..., Redis],
) -> AsyncIterator[Backend]:
    """The same scenario on the real server and on fakeredis."""
    if request.param == "redis":
        yield Backend(
            new_client=lambda decode_responses=False: make_client(
                redis_url, decode_responses=decode_responses
            ),
            publisher_with_outage=lambda: (
                make_client(proxy.url),
                _ProxyOutage(proxy),
            ),
        )
        return
    server = FakeServer()
    fakes: list[FakeAsyncRedis] = []

    def new_fake(decode_responses: bool = False) -> FakeAsyncRedis:
        fakes.append(FakeAsyncRedis(server=server, decode_responses=decode_responses))
        return fakes[-1]

    yield Backend(
        new_client=new_fake,
        publisher_with_outage=lambda: (new_fake(), _FakeServerOutage(server)),
    )
    for fake in fakes:
        await fake.aclose()


@pytest.fixture
def backoffs() -> Backoffs:
    """Records the back-offs of a `BackoffRecordingChannel` and lets each pass at once."""
    return Backoffs(hold=False)


@pytest.fixture
def held_backoffs() -> Backoffs:
    """Records the back-offs of a `BackoffRecordingChannel` and keeps it in its first one."""
    return Backoffs(hold=True)


@pytest.fixture
def restartable_redis_url() -> str:
    """The URL of the server a test may shut down; Docker's restart policy brings it back."""
    return _required_url("TANDEM_TEST_RESTARTABLE_REDIS_URL")


@pytest.fixture
async def url_denying(
    redis_url: str, observer: Redis, key_prefix: str
) -> AsyncIterator[Callable[[str], Awaitable[str]]]:
    """Builds the URL of a user of the shared server that may do everything but one command."""

    async def build(command: str) -> str:
        await observer.acl_setuser(
            key_prefix,
            enabled=True,
            nopass=True,
            commands=["+@all", f"-{command}"],
            keys=["*"],
            channels=["*"],
        )
        return redis_url.replace("redis://", f"redis://{key_prefix}@")

    yield build
    await observer.acl_deluser(key_prefix)


@pytest.fixture
async def url_with_wrong_password(
    redis_url: str, observer: Redis, key_prefix: str
) -> AsyncIterator[str]:
    """The URL of a user of the shared server that presents a password Redis rejects."""
    await observer.acl_setuser(
        key_prefix,
        enabled=True,
        passwords=["+the-right-one"],
        commands=["+@all"],
        keys=["*"],
        channels=["*"],
    )
    yield redis_url.replace("redis://", f"redis://{key_prefix}:the-wrong-one@")
    await observer.acl_deluser(key_prefix)
