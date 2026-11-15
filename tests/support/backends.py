"""The Redis a test runs against: the real server through a proxy, or fakeredis."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from redis.asyncio import Redis


class Outage(Protocol):
    """Takes the publisher's way to Redis down and brings it back."""

    def begin(self) -> None: ...

    def end(self) -> None: ...


@dataclass(frozen=True)
class Backend:
    new_client: Callable[[], Redis]
    publisher_with_outage: Callable[[], tuple[Redis, Outage]]
