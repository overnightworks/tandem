# ruff: noqa: T201 - this process reports to its parent test on stdout
"""A pod as a process: `renew URL PREFIX HOLDER LASTS_MS` or `serve URL PREFIX CLAIM_FOR_MS`."""

import asyncio
import sys
from datetime import timedelta

from redis.asyncio import Redis

from tandem import Lapse, Leases

FAMILY = "owner"


async def main(command: str, url: str, key_prefix: str, argument: str, *rest: str) -> None:
    client = Redis.from_url(url)
    span = timedelta(milliseconds=int(rest[0] if rest else argument))
    leases = Leases(client, key_prefix=key_prefix, family=FAMILY, lasts=span, claim_for=span)
    try:
        if command == "renew":
            await leases.renew(argument)
            print("RENEWED", flush=True)
            await asyncio.Event().wait()
        else:
            await leases.serve_lapses(report, claimer="the process", every=timedelta(seconds=0.05))
    finally:
        await client.aclose()


async def report(lapse: Lapse) -> None:
    print(f"HANDLING {lapse.holder}", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
