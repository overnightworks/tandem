# ruff: noqa: T201 - this process reports to its parent test on stdout
"""A pod as a process: `URL PREFIX NAME LASTS_MS` acquires the lock on NAME and never lets go."""

import asyncio
import sys
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Locks
from tests.support.locks import FAMILY


async def main(url: str, key_prefix: str, name: str, lasts_milliseconds: str) -> None:
    client = Redis.from_url(url)
    lasts = timedelta(milliseconds=int(lasts_milliseconds))
    locks = Locks(client, key_prefix=key_prefix, family=FAMILY, lasts=lasts)
    try:
        generation = await locks.acquire(name)
        print("ACQUIRED" if generation is not None else "REFUSED", flush=True)
        await asyncio.Event().wait()
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
