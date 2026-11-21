# tandem

The fleet's shared Redis coordination for apps that run in several pods. Today it holds five tools. A wake
channel with resync: a pod that changed something tells the others which address changed, and every
reader learns when it must re-read everything it serves. Deadlines on Redis time: a key comes due on
Redis's own clock and reaches one worker at a time. Leases: a holder's claim to be alive, with a
generation per start and a claim on each lapse. Locks: a hold on a name that only its generation extends
or releases, started only when none is held. Slots: a counted hold, at most N live holders per pool
across pods, taken in several pools at once or in none, each hold expiring on Redis's clock. It builds on
[redis-py](https://redis.readthedocs.io/)'s asyncio client and writes its log lines through
[logline](https://github.com/overnightworks/logline).
Distribution `overnightworks-tandem`, import package `tandem`.

## Install

`overnightworks-tandem` is not on PyPI. A host pins a tag by git. Put the tag in place of `vX.Y.Z`:

```toml
dependencies = [
    "overnightworks-tandem @ git+https://github.com/overnightworks/tandem@vX.Y.Z",
    "overnightworks-logline @ git+https://github.com/overnightworks/logline@vA.B.C",
]
```

tandem declares `overnightworks-logline>=0.3.0,<2` by name, so the host pins the logline tag (`vA.B.C`
inside that range) beside tandem's, and one logline runs in the process. A host without that pin asks the
package index, where the name is not registered. A host's Dependabot entry for uv ignores `overnightworks-*`,
because Dependabot looks the name up on PyPI. Tags are never moved; a broken release gets the next patch tag.

## Quick start

```python
from datetime import timedelta

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError

from tandem import Deadlines, Leases, Locks, Slots, StoreError, Wake, WakeChannel

# Retry and timeouts: duty 1 of the reference.
redis = Redis.from_url(
    "redis://redis:6379",
    retry=Retry(ExponentialBackoff(), 3),
    retry_on_error=[ConnectionError, TimeoutError],
    socket_timeout=5,
    socket_connect_timeout=5,
)
channel = WakeChannel(redis, key_prefix="skat")


async def follow_changes(served: Served) -> None:
    def on_wake(wake: Wake) -> None:
        served.reread(wake.address)  # runs in the sender's trace

    # Entering returns once the first re-read is done; with no confirmation in time it raises StoreError.
    async with channel.following(
        on_resync=served.reread_everything,
        on_wake=on_wake,
        on_failed=served.stop_serving,  # called once when the following ends on its own
        ready_within=timedelta(seconds=10),
    ):
        await served.until_shutdown()


async def save(served: Served, address: str, value: str) -> None:
    await served.commit(address, value)
    try:
        await channel.publish(address)
    except StoreError:
        # The client's Retry gave up and the delivery is uncertain; this runs the Retry again, and a
        # duplicate wake is harmless.
        await channel.publish(address)


deadlines = Deadlines(redis, key_prefix="skat", kind="seats", claim_for=timedelta(seconds=30))


async def expire_seats(served: Served) -> None:
    for due in await deadlines.claim_due(limit=20):
        await served.expire(seat=due.key, fence=due.claim)  # the host fences its own effect
        await deadlines.done(due)


leases = Leases(
    redis, key_prefix="skat", family="agents", lasts=timedelta(seconds=30), claim_for=timedelta(seconds=60)
)


async def run_agent(agent: Agent) -> None:
    async with leases.kept(agent.id, every=timedelta(seconds=10)) as hold:
        await agent.run(fence=hold.generation)  # the host fences with the generation it stored


locks = Locks(redis, key_prefix="skat", family="gpu", lasts=timedelta(seconds=15))


async def train_on_gpu(job: Job) -> None:
    generation = await locks.acquire("gpu-0")
    if generation is None:
        return  # held by someone else, or Redis restarted less than `lasts` ago
    try:
        while not job.done() and await locks.extend("gpu-0", generation):
            await job.step(fence=generation)  # a stale generation is refused by the job's own fence
    finally:
        await locks.release("gpu-0", generation)


slots = Slots(redis, key_prefix="skat", family="streams", lasts=timedelta(seconds=60))


async def open_stream(person: str) -> None:
    token = await slots.take({"global": 200, f"person-{person}": 3})
    if token is None:
        return  # a pool is full: refuse now, nothing waits
    try:
        await serve_stream(person)
    finally:
        await slots.release(token)  # a pod that dies frees its slot after `lasts`
```

[docs/reference.md](docs/reference.md) lists every public name, the Redis layout, the log events and the
duties a host takes on: what its client carries, what it fences and what it re-derives after a Redis restart.

A host's tests wake without Redis through `tandem.testing`, which is for tests only: `FakeWakeBus().pod()`
returns a channel with `publish` and `following` as `WakeChannel` has them, and tandem's own tests hold it
to the same cells as the real one.

## Fleet standard: cross-pod coordination in Redis

Covers what pods share through Redis: waking one another across pods, deadlines kept in Redis, leases
kept in Redis, locks kept in Redis and counted holds kept in Redis. It excludes deadlines and holds an app
keeps in its own database under its own transaction, such as skat's chair holds, and rate limits (events
per time window, such as webauth's).

New code coordinates through tandem at once. An existing copy moves to tandem now, as soon as the tandem
release that ships its primitive exists. The move runs as one fleet-program item with a sub-issue in each
covered repository. Today tandem ships the wake channel, deadlines, leases, locks and the counted hold.

A deviation is a request naming its enduring harm. tandem's head reviews it, only the operator rules it,
and the ruling is recorded in this section; the process is marketplace's
[How a standard works](https://github.com/overnightworks/marketplace/blob/main/docs/standards.md#how-a-standard-works).

## Limits

- Delivery is at most once per subscription; the re-read at `on_resync` and the host's handling of a failed
  `publish` make its state converge.
- A subscription that dies without closing, such as a half-open TCP connection, is noticed only when the
  operating system reports it; tandem sends no heartbeat.
- Deadlines, leases, lapses, claims and locks are as durable as the Redis behind them. A restart takes them
  all and the host reconciles; a lost lease or lock reads UNKNOWN, not GONE or free, for `lasts`, and a first
  `acquire` of an app with no epoch yet returns None for `lasts`. What this needs of Redis is stated in the
  [architecture](docs/ARCHITECTURE.md#6-constraints-and-reasons). A step of Redis's clock moves every expiry.
- A lapse whose handler always raises returns to a worker every `claim_for`; a family without
  `serve_lapses` keeps its lapse queue until the next restart.
- A paused holder lapses after `lasts`; its stale effects are refused only by the host's own fence.
- A lock has no forced release and no holder label: its host keeps who holds it in its own store.
- A slot has no renewal and no count to read: a hold that must outlast `lasts` is a lease or a lock.
- A generation is 128 random bits and is never shown to a person or logged: it lets `end` its lease.

## Versioning

`tandem.__version__` is the one version. A change of a public name, its type or its meaning is stated in
the pull request as **Breaking** with the hosts' migration. The wake channel's version segment is part of
the contract between pods of different releases: raising it is **Breaking**. The names and fields of the log
events are public too: renaming one is **Breaking**.

## Development and licence

`AGENTS.md` names the commands; the tests run against a real Redis in a throwaway container. MIT, see `LICENSE`.
