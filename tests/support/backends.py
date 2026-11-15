"""The Redis a test runs against: the real server through a proxy, or fakeredis."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from redis.asyncio import Redis


class Outage(Protocol):
    """Takes the publisher's way to Redis down and brings it back."""

    def begin(self) -> None: ...

    def end(self) -> None: ...


class NewClient(Protocol):
    def __call__(self, *, decode_responses: bool = False) -> Redis: ...


@dataclass(frozen=True)
class Backend:
    new_client: NewClient
    publisher_with_outage: Callable[[], tuple[Redis, Outage]]
