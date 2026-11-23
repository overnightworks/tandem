import fcntl
import io
import os
import sys
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import timedelta
from typing import Any

import pytest
from fakeredis import FakeAsyncRedis, FakeServer
from logline.testing import configured
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError, TimeoutError

from tandem import Client, Deadlines, Leases, Locks, Slots
from tandem.errors import FailureReason
from tandem.testing import settle_epoch
from tests.support.attempts import Attempts
from tests.support.backends import Backend
from tests.support.backoffs import Backoffs
from tests.support.deadlines import TwoPods
from tests.support.locks import FAMILY
from tests.support.logs import LogLines
from tests.support.proxy import TcpProxy
from tests.support.slots import FAMILY as SLOT_FAMILY
from tests.support.store_failures import StoreFailure
from tests.support.wake_backends import FakeBackend, RedisBackend, RedisFixtures, WakeBackend


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
def log_lines(monkeypatch: pytest.MonkeyPatch) -> Iterator[LogLines]:
    """The host's logging as the fleet runs it: logline's JSON lines at the debug level."""
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setenv("LOG_LEVEL", "debug")
    with configured("tandem-tests"):
        yield LogLines(stdout)


@pytest.fixture(params=["redis", "fake"])
def wake(request: pytest.FixtureRequest) -> WakeBackend:
    """The same following cell against a real Redis and against `tandem.testing`'s fake, which
    needs no Redis."""
    if request.param == "fake":
        return FakeBackend()
    return RedisBackend(
        RedisFixtures(
            redis_url=request.getfixturevalue("redis_url"),
            key_prefix=request.getfixturevalue("key_prefix"),
            make_client=request.getfixturevalue("make_client"),
            observer=request.getfixturevalue("observer"),
            proxy=request.getfixturevalue("proxy"),
            url_denying=request.getfixturevalue("url_denying"),
        )
    )


@pytest.fixture
def new_deadlines(key_prefix: str) -> Callable[..., Deadlines]:
    """Deadlines of one kind under the test's prefix; every call with the same kind shares them."""

    def build(
        client: Client, *, kind: str = "work", claim_for: timedelta = timedelta(seconds=30)
    ) -> Deadlines:
        return Deadlines(client, key_prefix=key_prefix, kind=kind, claim_for=claim_for)

    return build


@pytest.fixture
def two_pods(
    redis_url: str, make_client: Callable[..., Redis], new_deadlines: Callable[..., Deadlines]
) -> TwoPods:
    """One client with its deadlines, and the same kind's deadlines on a second client."""
    client = make_client(redis_url)
    return client, new_deadlines(client), new_deadlines(make_client(redis_url))


@pytest.fixture
def new_leases(key_prefix: str) -> Callable[..., Leases]:
    """Leases of one family under the test's prefix; every call with the same family shares them."""

    def build(
        client: Client,
        *,
        family: str = "owner",
        lasts: timedelta = timedelta(seconds=30),
        claim_for: timedelta = timedelta(seconds=30),
    ) -> Leases:
        return Leases(
            client, key_prefix=key_prefix, family=family, lasts=lasts, claim_for=claim_for
        )

    return build


@pytest.fixture
async def settled_epoch(observer: Redis, key_prefix: str) -> None:
    """The app's epoch set far in the past, so a lock reads free at once until a test wipes it."""
    await settle_epoch(observer, key_prefix=key_prefix)


@pytest.fixture
def new_locks(key_prefix: str, settled_epoch: None) -> Callable[..., Locks]:
    """Locks of one family under the test's prefix; every call with the same family shares them."""

    def build(
        client: Client, *, family: str = FAMILY, lasts: timedelta = timedelta(seconds=30)
    ) -> Locks:
        return Locks(client, key_prefix=key_prefix, family=family, lasts=lasts)

    return build


@pytest.fixture
def new_slots(key_prefix: str) -> Callable[..., Slots]:
    """Slots of one family under the test's prefix; every call with the same family shares them."""

    def build(
        client: Client, *, family: str = SLOT_FAMILY, lasts: timedelta = timedelta(seconds=30)
    ) -> Slots:
        return Slots(client, key_prefix=key_prefix, family=family, lasts=lasts)

    return build


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
            # redis-py 5.x retries only these; 8.x's Retry retries them anyway.
            options["retry_on_error"] = [ConnectionError, TimeoutError]
        client = Redis.from_url(url, client_name=f"{key_prefix}-{len(made)}", **options)
        made.append(client)
        return client

    yield make
    for client in made:
        await client.aclose()


@pytest.fixture
def proxied_client(proxy: TcpProxy, make_client: Callable[..., Redis]) -> Redis:
    """A client through the proxy that never retries, so an outage reaches the caller at once."""
    return make_client(proxy.url, retry=Retry(NoBackoff(), 0))


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


@pytest.fixture(params=list(FailureReason))
async def store_failure(
    request: pytest.FixtureRequest,
    proxy: TcpProxy,
    proxied_client: Redis,
    url_denying: Callable[[str], Awaitable[str]],
    observer: Redis,
    key_prefix: str,
    make_client: Callable[..., Redis],
) -> StoreFailure:
    """The same scenario with an outage of the store and with a refusal of its commands."""
    client = (
        proxied_client
        if request.param is FailureReason.OUTAGE
        else make_client(await url_denying("flushall"))
    )
    return StoreFailure(request.param, client, proxy, observer, key_prefix)


@pytest.fixture
def attempts() -> Attempts:
    """Records the results of the attempts of a gated call."""
    return Attempts()


@pytest.fixture
def restartable_redis_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """The URL of the server a test may shut down or flush; a loop in its container restarts it.

    Docker's restart policy is deliberately not used: its delay doubles with every restart.

    One test at a time holds the server, across xdist workers, because a shutdown or a script flush
    is state every other user of it would observe.
    """
    url = _required_url("TANDEM_TEST_RESTARTABLE_REDIS_URL")
    lock_path = tmp_path_factory.getbasetemp().parent / "restartable-redis.lock"
    with lock_path.open("w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        yield url


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
