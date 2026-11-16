"""A publisher that sends numbered addresses without pause, as frames with no trace, and keeps the
receiver count of each."""

import asyncio

from redis.asyncio import Redis


class NumberedPublisher:
    def __init__(self, client: Redis, channel: str) -> None:
        self._client = client
        self._channel = channel
        self._stopped = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.sent: list[tuple[int, int]] = []  # the number and the receivers Redis counted for it
        self.zero_seen = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._publish())

    async def stop(self) -> None:
        assert self._task is not None
        self._stopped.set()
        await self._task

    async def _publish(self) -> None:
        number = 0
        while not self._stopped.is_set():
            receivers = await self._client.publish(self._channel, f"0:{number}")
            self.sent.append((number, receivers))
            if receivers == 0:
                self.zero_seen.set()
            number += 1
            await asyncio.sleep(0)

    async def has_had_a_receiver(self) -> bool:
        return any(receivers > 0 for _, receivers in self.sent)

    async def replacement_has_got(self, count: int) -> bool:
        return len(self.numbers_after_the_zeros()) >= count

    def numbers_with_no_receiver(self) -> set[int]:
        return {number for number, receivers in self.sent if receivers == 0}

    def numbers_after_the_zeros(self) -> list[int]:
        """The numbers a receiver got once the count was back at one after its last run of zero."""
        last_zero = max(
            (index for index, (_, receivers) in enumerate(self.sent) if receivers == 0),
            default=None,
        )
        if last_zero is None:
            return []
        return [number for number, receivers in self.sent[last_zero:] if receivers > 0]
