# ruff: noqa: T201 - this process reports to its parent test on stdout
"""A WakeChannel reader or writer as a process: `read URL PREFIX` or `write URL PREFIX ADDRESS`."""

import asyncio
import sys

from redis.asyncio import Redis

from tandem import Resubscribed, WakeChannel


async def read(channel: WakeChannel) -> None:
    async for event in channel.received():
        print(
            "RESUBSCRIBED" if isinstance(event, Resubscribed) else f"WAKE {event.address}",
            flush=True,
        )


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
