"""The attempts of a gated call, observed: a test waits for each result before it acts."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis

from tandem import Deadlines, Due, Leases, Locks, Renewal, StoreError
from tandem.deadlines import DeadlineQueue
from tests.support.waiting import BOUND_SECONDS


class Attempts:
    """The results of a gated call's attempts, in order: what it returned or the `StoreError`."""

    def __init__(self) -> None:
        self.results: list[object] = []
        self._changed = asyncio.Condition()

    async def observed[T](self, attempt: Awaitable[T]) -> T:
        """Await `attempt`, note its result and hand it on unchanged."""
        try:
            result = await attempt
        except StoreError as error:
            await self._note(error)
            raise
        await self._note(result)
        return result

    @property
    def unsuccessful_count(self) -> int:
        """How many attempts raised a `StoreError` or answered False."""
        return sum(result is False or isinstance(result, StoreError) for result in self.results)

    async def reached(self, count: int) -> None:
        """Wait until `count` attempts have a result."""
        await self._until(lambda: len(self.results) >= count)

    async def failed(self, count: int) -> None:
        """Wait until `count` attempts raised a `StoreError`."""
        await self._until(
            lambda: sum(isinstance(result, StoreError) for result in self.results) >= count
        )

    async def until_unsuccessful(self) -> None:
        """Wait until an attempt raised a `StoreError` or answered False."""
        await self._until(lambda: self.unsuccessful_count >= 1)

    async def _note(self, result: object) -> None:
        async with self._changed:
            self.results.append(result)
            self._changed.notify_all()

    async def _until(self, condition: Callable[[], bool]) -> None:
        async with asyncio.timeout(BOUND_SECONDS):
            async with self._changed:
                await self._changed.wait_for(condition)


class _ObservedLapseQueue(DeadlineQueue):
    def __init__(self, *args: Any, attempts: Attempts) -> None:
        super().__init__(*args)
        self._attempts = attempts

    async def extend(self, due: Due) -> bool:
        return await self._attempts.observed(super().extend(due))


class ObservedLeases(Leases):
    """`Leases` whose lapse claims' extensions go to `attempts`."""

    def __init__(self, redis: Redis, *, attempts: Attempts, **options: Any) -> None:
        super().__init__(redis, **options)
        self._lapses = _ObservedLapseQueue(
            redis, self._lapse_keys, self._lapses.claim_for, attempts=attempts
        )


class RenewalObservedLeases(Leases):
    """`Leases` whose renewals go to `attempts`."""

    def __init__(self, redis: Redis, *, attempts: Attempts, **options: Any) -> None:
        super().__init__(redis, **options)
        self._attempts = attempts

    async def _renew_untimed(self, holder: str) -> Renewal:
        return await self._attempts.observed(super()._renew_untimed(holder))


class ExtendObservedLocks(Locks):
    """`Locks` whose extensions go to `attempts`."""

    def __init__(self, redis: Redis, *, attempts: Attempts, **options: Any) -> None:
        super().__init__(redis, **options)
        self._attempts = attempts

    async def _extend_untimed(self, name: str, generation: str) -> bool:
        return await self._attempts.observed(super()._extend_untimed(name, generation))


class ObservedDeadlines(Deadlines):
    """`Deadlines` whose claims' extensions go to `attempts`."""

    def __init__(self, redis: Redis, *, attempts: Attempts, **options: Any) -> None:
        super().__init__(redis, **options)
        self._attempts = attempts

    async def extend(self, due: Due) -> bool:
        return await self._attempts.observed(super().extend(due))
