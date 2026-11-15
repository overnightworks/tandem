# tandem

The fleet's shared Redis coordination for apps that run in several pods. Today it holds two tools. A wake
channel with resync: a pod that changed something tells the others which address changed, and every
reader learns when it must re-read everything it serves. Deadlines on Redis time: a key comes due on
Redis's own clock and reaches one claimer at a time. It builds on [redis-py](https://redis.readthedocs.io/)'s
asyncio client. The reader is the developer who wires a host in.

Distribution `overnightworks-tandem`, import package `tandem`.

## Install

`overnightworks-tandem` is not on PyPI. A host pins a tag by git. Put the tag in place of `vX.Y.Z`:

```toml
dependencies = [
    "overnightworks-tandem @ git+https://github.com/overnightworks/tandem@vX.Y.Z",
]
```

A host's Dependabot entry for uv ignores `overnightworks-*`, because Dependabot looks the name up on PyPI.
Tags are never moved; a broken release gets the next patch tag.

## Quick start

```python
from datetime import timedelta

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from tandem import Deadlines, Resubscribed, StoreError, Wake, WakeChannel

# The Retry lets the host's own re-read at a Resubscribed survive a Redis restart (see duty 1).
# The timeouts let `publish` fail on a Redis that stays silent (see the publish row).
redis = Redis.from_url(
    "redis://redis:6379",
    retry=Retry(ExponentialBackoff(), 3),
    socket_timeout=5,
    socket_connect_timeout=5,
)
channel = WakeChannel(redis, key_prefix="skat")


async def follow_changes(served: Served) -> None:
    async for event in channel.received():
        match event:
            case Resubscribed():
                await served.reread_everything()
            case Wake(address):
                await served.reread(address)


async def save(served: Served, address: str, value: str) -> None:
    await served.commit(address, value)
    try:
        await channel.publish(address)
    except StoreError:
        await channel.publish(address)  # delivery was uncertain; a duplicate wake is harmless


deadlines = Deadlines(redis, key_prefix="skat", kind="seats", claim_for=timedelta(seconds=30))


async def hold_seat(seat: str) -> None:
    await deadlines.set(seat, timedelta(minutes=2))  # due on Redis's clock, not the pod's


async def expire_seats(served: Served) -> None:
    for due in await deadlines.claim_due(claimer="pod-3", limit=20):
        await served.expire(seat=due.key, fence=due.claim)  # the host fences its own effect
        await deadlines.done(due)
```

## The API

| Name | What it is |
|---|---|
| `WakeChannel(redis, *, key_prefix)` | The wake channel of one app on the host's `redis.asyncio.Redis` client. `key_prefix` is one segment matching `[a-z0-9][a-z0-9-]*`; anything else raises `ValueError` naming `key_prefix`. |
| `checked_segment(value, *, argument)` | The check `WakeChannel` applies to `key_prefix`: it returns `value` when it is one segment matching `[a-z0-9][a-z0-9-]*` and raises `ValueError` naming `argument` otherwise. A host that derives a prefix, such as `<prefix>-agents`, validates it with this before it builds a channel. |
| `await channel.publish(address)` | One `PUBLISH` of `address`, the name of what changed and never its content. It raises `StoreError` when Redis refuses or does not answer; a Redis that stays silent fails it only when the host's client sets `socket_timeout` and `socket_connect_timeout`, which default to none, so without them `publish` waits for ever. tandem never retries or queues it, and the client's own `Retry` never resends the `PUBLISH`; it governs only the connecting and health check before it, so with an unreachable Redis `publish` can block for about the `Retry`'s total plus each attempt's connect timeout before it raises. |
| `channel.received()` | An async iterator of `Wake` and `Resubscribed` that runs until it is cancelled. Each call opens a pub/sub connection of its own. |
| `Wake(address)` | A pod published `address`. |
| `Resubscribed()` | Redis acknowledged the subscription, the first time and after every reconnect. |
| `Deadlines(redis, *, key_prefix, kind, claim_for)` | The deadlines of one `kind` of one app. `key_prefix` and `kind` are each one segment matching `[a-z0-9][a-z0-9-]*`, and `claim_for` is a positive `timedelta`, the length of a claim; anything else (a `claim_for` that is no `timedelta` included) raises `ValueError` naming the argument. A deadline key is any string. |
| `await deadlines.set(key, due_in)` | `key` comes due `due_in` after now on Redis `TIME` (a pod's clock is never read). A `key` that already waits is re-armed; a `key` that a claimer holds is re-armed too and the claim is void, so the claimer's later `done` returns False and leaves the new deadline. |
| `await deadlines.cancel(key)` | Removes `key`, waiting or claimed, and voids its claim. True when there was something to remove. |
| `await deadlines.claim_due(*, claimer, limit)` | Claims at most `limit` keys that are due, and then keys whose claim ran out, each under a fresh random 128-bit token, and returns them as `Due`s. `limit` is at least 1, or it raises `ValueError` naming the argument. tandem keeps no record of `claimer`. |
| `await deadlines.extend(due)` | Holds the claim for another `claim_for`. False once its token is gone or the claim ran out. |
| `await deadlines.done(due)` | Finishes the claim and removes the key. False once its token is gone. |
| `Due(key, claim)` | One claim: the deadline's key and its token. |
| `deadlines.keys` | The `DeadlineKeys` (`due`, `claimed`, `claim`), the three key names of the kind, for a caller's script that runs `LUA_KIT`'s transitions on them. A script gets them as `KEYS` in the order `due`, `claimed`, `claim` (`[keys.due, keys.claimed, keys.claim]`) and builds the `keys` table of `LUA_KIT` with `local keys = {due = KEYS[1], claimed = KEYS[2], claim = KEYS[3]}`. |
| `Script(redis, source)` | A caller's Lua, run after `LUA_KIT`. `await script.run(keys, args)` runs it by its SHA, loads it again after `NOSCRIPT` and returns Redis's raw reply; every Redis failure raises `StoreError`. The host client's `Retry` governs a lost connection and can run the script again after a lost reply; tandem adds no retry. Redis does not roll a script back: a script that fails after a write keeps the write, so it checks before it writes. `LUA_KIT` is prepended to `source`, so an error's line number is offset by the kit's lines, and a `#!lua` shebang cannot be used. |
| `Client` | The type of the Redis client tandem takes, for a host's own parameter. |
| `LUA_KIT` | Lua that defines the table `tandem`; `Script` prepends it. `keys` below is a table with the fields `due`, `claimed` and `claim`, built from the script's `KEYS` as above. |
| `tandem.now_ms()` | Redis `TIME` in milliseconds. |
| `tandem.deadline_set(keys, key, due_in_ms)` | The transition of `Deadlines.set`: the key waits for `due_in_ms` from now and its claim is void. Returns the due in ms. |
| `tandem.deadline_cancel(keys, key)` | The transition of `Deadlines.cancel`. Returns 1 or 0. |
| `tandem.deadline_state(keys, key)` | Reads without changing: `{'waiting', due_ms}` while the key waits, `{'claimed', claim_until_ms}` while a claim holds it, nil otherwise. A claim past its expiry that nobody took over still reads as claimed. |
| `tandem.wake(channel, address)` | One `PUBLISH` of `address` inside the script, atomic with its writes; `channel` is `wake_channel(key_prefix)`. Returns the number of subscribers reached. |
| `wake_channel(key_prefix)` | The name of the wake channel, for `tandem.wake`. |
| `DeadlineKeys` | The record `deadlines.keys` returns. |
| `StoreError` | Redis refused or timed out; the cause is chained. |

The channel is `<key_prefix>:tandem:v1:wake`: one channel per key prefix, so every pod that follows the
same channel uses the same `key_prefix`. An app may run several `WakeChannel`s on one Redis under distinct
prefixes, for example `skat` and `skat-agents`.

`received()` hides no reconnect. When the connection breaks, redis-py reconnects inside the retry budget of
the host's client, or tandem backs off (0.5 s doubling to 5 s, with no attempt limit), subscribes again and
carries on; either way the next event is a `Resubscribed`, before any wake it covers. Only a command Redis
refuses outright, such as a subscribe the ACL denies or credentials Redis rejects, ends `received()` with
`StoreError`.

The deadlines of a kind live in `<key_prefix>:tandem:v1:d:<kind>:due`, `...:claimed` and `...:claim`, disjoint
from the channel and from every other kind. `claim_for` is the same for every pod of one kind. A claim whose
reply is lost to a retry of the host's client expires after `claim_for` and then reaches a claimer again. A `set` or `cancel` that the client's `Retry` runs again cannot tell a replay from a new operation: if another pod re-armed the key in between, a replayed `cancel` removes the new deadline and a replayed `set` overwrites its due time or invalidates its claim.

## The host's duties

Redis pub/sub delivers a wake to the readers subscribed at that moment and keeps nothing. Three duties
close that gap, and two more cover deadlines:

1. **Re-read everything the pod serves at each `Resubscribed`.** A wake sent while the pod was
   disconnected is gone for good; the `Resubscribed` after the reconnect is the signal that anything may
   have changed. After a Redis restart the host's own client can still hold stale pooled connections, so
   the first command of that re-read fails with `ConnectionError` on a default `redis.asyncio` client. The
   host's client therefore carries a `Retry`, as in the quick start, or the re-read retries itself.
2. **After a failed `publish`, publish again or bound the readers' staleness.** `publish` fails on a silent
   Redis only under the client's `socket_timeout`, so the host sets it. A failure means the
   delivery is uncertain: the `PUBLISH` may never have reached Redis, or Redis delivered it and only the
   reply was lost. A second `publish` of the same address only causes a duplicate wake. Alternatively every
   reader re-reads on a period, which bounds how stale it can get.
3. **A pod turns Ready at its first `Resubscribed`.** Before it, the pod is deaf to wakes and a state it
   served earlier may be stale.
4. **Fence the effect of a claimed deadline with your own version.** A due key reaches a handler at least
   once and each key has at most one valid claim at a time, yet a handler that pauses past its claim runs
   beside the next claimer after a takeover. The host checks and writes its effect against a version or
   generation of its own, in one atomic step of its own store, so the older handler's effect is refused.
5. **Reconcile the deadlines after a Redis restart.** A restart takes every pending deadline and every claim
   with it. The host derives what its durable state needs (the deadlines it guards) and `set`s them again
   at start and after it notices a restart, for example at a `Resubscribed`.

## Fleet standard: cross-pod coordination in Redis

Covers what pods share through Redis: waking one another across pods, deadlines kept in Redis and leases
kept in Redis. It excludes deadlines and holds an app keeps in its own database under its own
transaction, such as skat's chair holds.

New code coordinates through tandem at once; an existing copy moves to tandem when its app touches it
anyway. Today tandem ships the wake channel and deadlines; leases join with their release.

A deviation is a request naming its enduring harm. tandem's head reviews it, only the operator rules it,
and the ruling is recorded in this section; the process is marketplace's
[How a standard works](https://github.com/overnightworks/marketplace/blob/main/docs/standards.md#how-a-standard-works).

## Limits

- A subscription that dies without closing, such as a half-open TCP connection, is noticed only when the
  operating system reports it: tandem sends no heartbeat of its own, and redis-py's health check does not
  fail a silent pub/sub read.
- Delivery is at most once per subscription; duty 1 and duty 2 make the host's state converge anyway.
- A deadline is as durable as the Redis behind it; duty 5 covers a restart.

## Versioning

`tandem.__version__` is the one version. A change of a public name, its type or its meaning is stated in
the pull request as **Breaking** with the hosts' migration.

## Development

`AGENTS.md` names the commands. The tests run against a real Redis in a throwaway container started by
`scripts/run_redis_tests.sh`.

## Licence

MIT, see `LICENSE`.
