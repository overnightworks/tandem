# ruff: noqa: T201 - this process reports to its parent test on stdout
"""A WakeChannel reader or writer as a process: `read URL PREFIX` or `write URL PREFIX ADDRESS`."""

import asyncio
import sys

from redis.asyncio import Redis

from tandem import Wake, WakeChannel
from tests.support.waiting import BOUND


def _report(line: str) -> None:
    print(line, flush=True)


async def read(channel: WakeChannel) -> None:
    def report_wake(wake: Wake) -> None:
        _report(f"WAKE {wake.address}")

    def report_failure(error: Exception) -> None:
        _report(f"FAILED {type(error).__name__}")

    async with channel.following(
        on_resync=lambda: _report("RESYNCED"),
        on_wake=report_wake,
        on_failed=report_failure,
        ready_within=BOUND,
    ):
        await asyncio.Event().wait()


async def main(command: str, url: str, key_prefix: str, *address: str) -> None:
    client = Redis.from_url(url)
    channel = WakeChannel(client, key_prefix=key_prefix)
    try:
        if command == "read":
            await read(channel)
        else:
            await channel.publish(address[0])
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
