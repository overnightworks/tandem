# tandem

The fleet's shared Redis coordination for apps that run in several pods. Today it holds four tools. A wake
channel with resync: a pod that changed something tells the others which address changed, and every
reader learns when it must re-read everything it serves. Deadlines on Redis time: a key comes due on
Redis's own clock and reaches one claimer at a time. Leases: a holder's claim to be alive, with a
generation per start and a claim on each lapse. Locks: a hold on a name that only its generation extends
or releases, started only when none is held. It builds on [redis-py](https://redis.readthedocs.io/)'s
asyncio client and writes its log lines through [logline](https://github.com/overnightworks/logline). The
reader is the developer who wires a host in.

Distribution `overnightworks-tandem`, import package `tandem`.

## Install

`overnightworks-tandem` is not on PyPI. A host pins a tag by git. Put the tag in place of `vX.Y.Z`:

```toml
dependencies = [
    "overnightworks-tandem @ git+https://github.com/overnightworks/tandem@vX.Y.Z",
    "overnightworks-logline @ git+https://github.com/overnightworks/logline@vA.B.C",
]
```

tandem declares `overnightworks-logline>=0.3.0,<0.6` by name, so the host pins the logline tag (`vA.B.C`
inside that range) beside tandem's, and one logline runs in the process. A host without that pin asks the
package index, where the name is not registered. A host's Dependabot entry for uv ignores `overnightworks-*`,
because Dependabot looks the name up on PyPI. Tags are never moved; a broken release gets the next patch tag.

## Quick start

```python
from datetime import timedelta

from logline import unit_trace
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError

from tandem import (
    Deadlines,
    Lapse,
    Leases,
    Liveness,
    Locks,
    Resubscribed,
    StoreError,
    Wake,
    WakeChannel,
)

# The Retry lets the host's own re-read at a Resubscribed survive a Redis restart (see duty 1);
# on redis-py 5.x the client also needs retry_on_error=[ConnectionError, TimeoutError] for it.
# The timeouts let `publish` fail on a Redis that stays silent (see the publish row).
redis = Redis.from_url(
    "redis://redis:6379",
    retry=Retry(ExponentialBackoff(), 3),
    retry_on_error=[ConnectionError, TimeoutError],
    socket_timeout=5,
    socket_connect_timeout=5,
)
channel = WakeChannel(redis, key_prefix="skat")


async def follow_changes(served: Served) -> None:
    async for event in channel.received():
        match event:
            case Resubscribed():
                await served.reread_everything()
            case Wake(address, traceparent):
                with unit_trace(traceparent):  # the handling joins the sender's trace (see duty 15)
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


leases = Leases(
    redis,
    key_prefix="skat",
    family="agents",
    lasts=timedelta(seconds=30),
    claim_for=timedelta(seconds=60),
)


async def run_agent(agent: Agent) -> None:
    async with leases.kept(agent.id, every=timedelta(seconds=10)) as hold:
        await agent.run(fence=hold.generation)  # the host fences with the generation it stored


async def free_lapsed_agents(served: Served) -> None:
    async def free(lapse: Lapse) -> None:
        await served.free(agent=lapse.holder, fence=lapse.generation)

    await leases.serve_lapses(free, claimer="pod-3", every=timedelta(seconds=5))  # runs until cancelled


async def agents_gone(served: Served) -> list[str]:
    states = await leases.state(served.known_agents())
    return [agent for agent, state in states.items() if state.liveness is Liveness.GONE]


locks = Locks(redis, key_prefix="skat", family="gpu", lasts=timedelta(seconds=15))


async def train_on_gpu(job: Job) -> None:
    generation = await locks.acquire("gpu-0")
    if generation is None:
        return  # held by someone else, or Redis restarted less than `lasts` ago
    try:
        while not job.done():
            await job.step(fence=generation)  # the job's writes carry the generation, so a stale one is refused
            if not await locks.extend("gpu-0", generation):
                return  # the lock is lost; a later step would be refused by its fence
    finally:
        await locks.release("gpu-0", generation)
```

## The API

| Name | What it is |
|---|---|
| `WakeChannel(redis, *, key_prefix)` | The wake channel of one app on the host's `redis.asyncio.Redis` client. `key_prefix` is one segment matching `[a-z0-9][a-z0-9-]*`; anything else raises `ValueError` naming `key_prefix`. |
| `checked_segment(value, *, argument)` | The check `WakeChannel` applies to `key_prefix`: it returns `value` when it is one segment matching `[a-z0-9][a-z0-9-]*` and raises `ValueError` naming `argument` otherwise. A host that derives a prefix, such as `<prefix>-agents`, validates it with this before it builds a channel. |
| `await channel.publish(address)` | One `PUBLISH` of `address`, the name of what changed and never its content, with the caller's current traceparent (logline's `current_traceparent()`; none outside a trace). It raises `StoreError` when Redis refuses or does not answer; a Redis that stays silent fails it only when the host's client sets `socket_timeout` and `socket_connect_timeout`, which default to none, so without them `publish` waits for ever. tandem never retries or queues it, and the client's own `Retry` never resends the `PUBLISH`; it governs only the connecting and health check before it, so with an unreachable Redis `publish` can block for about the `Retry`'s total plus each attempt's connect timeout before it raises. |
| `channel.received()` | An async iterator of `Wake` and `Resubscribed` that runs until it is cancelled. Each call opens a pub/sub connection of its own. |
| `Wake(address, traceparent=None)` | A pod published `address` while it was in the trace `traceparent`; None when its sender had none. It is the sender's value as received: tandem validates nothing, and logline's `unit_trace` starts a new trace for a malformed one. |
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
| `Leases(redis, *, key_prefix, family, lasts, claim_for)` | The leases of one `family` of one app. A lease lasts `lasts` after its latest renewal; a claim on a lapse holds for `claim_for`. `key_prefix` and `family` are each one segment matching `[a-z0-9][a-z0-9-]*`, and `lasts` and `claim_for` are positive `timedelta`s; anything else raises `ValueError` naming the argument. A holder is any string. |
| `await leases.renew(holder)` | Keeps the lease of `holder` for another `lasts` on Redis `TIME` and returns a `Renewal`. A lease that is not live (never started, ended, or expired) starts anew under a fresh random 128-bit generation with `started=True`; exactly one of several concurrent renewals sees it. |
| `await leases.end(holder, generation)` | Ends the live lease of `generation` at once and queues its lapse as due now. False, and nothing changes, when `generation` is not the holder's current one or the lease is no longer live. |
| `await leases.state(holders)` | A `dict` from each holder to a `LeaseState(liveness, generation, remaining)`, read in one script that renews and starts nothing. ALIVE while its lease is live, with its `generation` and the `remaining` time; otherwise UNKNOWN until `lasts` have passed since the app's epoch, then GONE, and `generation` and `remaining` are None. The epoch is the Redis time of the first command of any pod after Redis lost its data, so a restart reads UNKNOWN, not GONE, for `lasts`; a host reconciles after the restart. A host that did not start the lease reads its generation here, for example to accept an add under the agent's current generation and later end only what that generation made on a `Lapse`. An empty collection returns `{}` without a Redis call. |
| `await leases.live()` | A `Roster(holders, settled)`, read in one script: every live holder, and whether `lasts` have passed since the epoch. A roster that is not `settled` may miss holders that a restart took. |
| `leases.kept(holder, *, every)` | An async context manager that renews `holder` once before the block runs, then every `every`, and ends the lease at exit. It yields a `Hold` whose `generation` follows each renewal, so a holder that lapsed and started again shows the new one. `every` is a positive `timedelta` below `lasts`, or it raises `ValueError` naming `every`. A `StoreError` of the first renewal raises out of the entry; a failed later renewal is logged and retried, never ends the block. At exit an in-flight renewal completes, then the current generation is ended, shielded from cancellation. |
| `await leases.serve_lapses(handle, *, claimer, every)` | Runs until cancelled. Each `every` it sweeps expired leases into the lapse queue and claims the due lapses in small batches until none is due, and calls `await handle(lapse)` for each, at least once. While `handle` runs its claim is extended every `claim_for / 3`; a failed extension is logged and stops, and `handle` is never cancelled. `done` follows a `handle` that returns; one that raises is logged and its lapse returns to a claimer when its claim runs out. Each `handle` runs in a trace of its own, which ends with it. An outage of Redis is ridden out with the back-off of `received()`; a refusal raises `StoreError`. |
| `Locks(redis, *, key_prefix, family, lasts)` | The locks of one `family` of one app. A lock lasts `lasts` after its latest start or extension. `key_prefix` and `family` are each one segment matching `[a-z0-9][a-z0-9-]*`, and `lasts` is a positive `timedelta`; anything else raises `ValueError` naming the argument. A name is any string. |
| `await locks.acquire(name)` | Starts the lock on `name` under a fresh random 128-bit generation and returns it, or returns None when a live lock holds `name`, the holder asking again included: a lock is not re-entrant, and `acquire` never extends one. After a Redis restart it returns None until `lasts` have passed since the app's epoch. |
| `await locks.extend(name, generation)` | Keeps the live lock of `generation` for another `lasts` on Redis `TIME`. False, and nothing changes, for an absent, foreign or expired generation, even when nobody took the name since. |
| `await locks.release(name, generation)` | Ends the live lock of `generation` at once; the next `acquire` gets a new generation. False, and nothing changes, for any other generation. tandem has no release without the generation: a crashed holder's lock runs out after `lasts`. |
| `await locks.state(name)` | A `LockState(liveness, generation, remaining)`, read in one script. ALIVE while held, with the holder's `generation` and the `remaining` time; GONE when free; UNKNOWN until `lasts` have passed since the epoch when Redis may have lost the lock. |
| `locks.keys`, `locks.lasts` | The `LockKeys` (`epoch`, `expiry`, `generation`) and the `lasts` of the family, for a host's script that calls `tandem.lock_state`. A script gets the keys as `KEYS` in the order `epoch`, `expiry`, `generation` and builds the table with `local keys = {epoch = KEYS[1], expiry = KEYS[2], generation = KEYS[3]}`. |
| `Renewal(started, generation)` | The result of `renew`. |
| `Lapse(holder, generation, due)` | A lease that ended or expired, with the generation it had. |
| `Roster(holders, settled)` | The result of `live`. |
| `Hold(generation)` | What `kept` yields. |
| `LeaseState(liveness, generation, remaining)` | The result of `leases.state` for one holder: `generation` and `remaining` are None unless ALIVE. |
| `LockState(liveness, generation, remaining)` | The result of `locks.state`: `generation` and `remaining` are None unless ALIVE. |
| `LockKeys` | The record `locks.keys` returns. |
| `Liveness` | `ALIVE`, `GONE` or `UNKNOWN`, of a lease holder and of a lock name. |
| `Client` | The type of the Redis client tandem takes, for a host's own parameter. |
| `LUA_KIT` | Lua that defines the table `tandem`; `Script` prepends it. `keys` of the deadline functions below is a table with the fields `due`, `claimed` and `claim`, built from the script's `KEYS` as above. |
| `tandem.now_ms()` | Redis `TIME` in milliseconds. |
| `tandem.epoch_settled(epoch_key, now_ms, lasts_ms)` | Creates the app's epoch at `now_ms` when absent and returns true once `lasts_ms` have passed since it. |
| `tandem.deadline_set(keys, key, due_in_ms)` | The transition of `Deadlines.set`: the key waits for `due_in_ms` from now and its claim is void. Returns the due in ms. |
| `tandem.deadline_cancel(keys, key)` | The transition of `Deadlines.cancel`. Returns 1 or 0. |
| `tandem.deadline_state(keys, key)` | Reads without changing: `{'waiting', due_ms}` while the key waits, `{'claimed', claim_until_ms}` while a claim holds it, nil otherwise. A claim past its expiry that nobody took over still reads as claimed. |
| `tandem.lock_state(keys, name, lasts_ms)` | Reads a lock without changing it, apart from creating the epoch when absent. `keys` has the fields `epoch`, `expiry` and `generation` (`locks.keys`); `lasts_ms` is `locks.lasts` in ms. Returns `{'held', generation, expires_ms}` while a live lock holds `name`, `{'unknown'}` while it is absent and `lasts_ms` have not passed since the epoch, and nil when it is free. `if tandem.lock_state(...) then` therefore refuses unless the lock is free and settled, so a host's admit script reads the lock and writes its own state in one atomic step. |
| `tandem.wake(channel, address, traceparent)` | One `PUBLISH` of `address` inside the script, atomic with its writes; `channel` is `wake_channel(key_prefix)`. `traceparent` is optional: a Lua script cannot read the trace, so a host passes `current_traceparent()` from Python as an argument, and without it the wake carries none. Returns the number of subscribers reached. |
| `wake_channel(key_prefix)` | The name of the wake channel, for `tandem.wake`. |
| `DeadlineKeys` | The record `deadlines.keys` returns. |
| `StoreError` | Redis refused or timed out; the cause is chained. |
| `tandem.testing.settle_epoch(redis, *, key_prefix)` | Test-only helper for a host's tests (`await`, with a `redis.asyncio.Redis`): sets the app's epoch so far in the past that every lease and lock family of `key_prefix` reads settled at once, with no knowledge of tandem's key layout. Production code that settles the epoch breaks the restart grace: after a Redis restart it reads GONE or free for what Redis lost, where it must read UNKNOWN until `lasts` have passed. |

The leases of a family live in `<key_prefix>:tandem:v1:l:<family>:expiry` and `...:generation`, its lapse
queue in `...:l:<family>:lapse:{due,claimed,claim}` and the app's epoch in `<key_prefix>:tandem:v1:epoch`,
disjoint from the channel and the deadlines. Only the expiry decides ALIVE: a lapse that a handler claimed
reads GONE, or UNKNOWN until `lasts` have passed since the epoch, and a `renew` after it starts a new
generation.

The locks of a family live in `<key_prefix>:tandem:v1:lock:<family>:expiry` (a ZSET scored by the expiry in
ms) and `...:generation` (a HASH), sharing the app's epoch with the leases and disjoint from every other
structure. A name is a ZSET member and a HASH field, never a key segment. Each `acquire` removes a small
batch of expired entries of its family, so names that are never acquired again leave over time. A lock has
no lapse queue: a crashed holder's lock expires after `lasts`, and what follows is the host's reapers'.

The channel is `<key_prefix>:tandem:v2:wake`: one channel per key prefix, so every pod that follows the
same channel uses the same `key_prefix`. An app may run several `WakeChannel`s on one Redis under distinct
prefixes, for example `skat` and `skat-agents`. A pod on a tandem before 0.8.0 publishes and listens on
`<key_prefix>:tandem:v1:wake`, so it and a pod on 0.8.0 or later never hear each other: the host restarts
all its pods at once when it moves to 0.8.0, and a rollback goes back for all pods at once as well.

`received()` hides no reconnect. When the connection breaks, redis-py reconnects inside the retry budget of
the host's client, or tandem backs off (0.5 s doubling to 5 s, with no attempt limit), subscribes again and
carries on; either way the next event is a `Resubscribed`, before any wake it covers. Only a command Redis
refuses outright, such as a subscribe the ACL denies or credentials Redis rejects, ends `received()` with
`StoreError`. A payload that is no valid UTF-8, or no frame of tandem's, is skipped with a
`wake_payload.skipped` warning that names its length, never its content; the next valid wake arrives as before. A client that decodes responses
(`decode_responses=True`) fails inside redis-py on a payload that is no valid UTF-8, so tandem subscribes
again and a `Resubscribed` precedes the next wake; a readable payload that is no frame is skipped on the
existing subscription, without a `Resubscribed`.

The deadlines of a kind live in `<key_prefix>:tandem:v1:d:<kind>:due`, `...:claimed` and `...:claim`, disjoint
from the channel and from every other kind. `claim_for` is the same for every pod of one kind. A claim whose
reply is lost to a retry of the host's client expires after `claim_for` and then reaches a claimer again. A `set` or `cancel` that the client's `Retry` runs again cannot tell a replay from a new operation: if another pod re-armed the key in between, a replayed `cancel` removes the new deadline and a replayed `set` overwrites its due time or invalidates its claim.

## What tandem logs

Every line goes through the standard-library logger `tandem.<module>` as logline's event contract defines
it (README of logline, "Fleet standard"): the message is the literal event, the fields are `extra=`. A host
that called `logline.configure` gets the JSON line with its `trace_id`; a host with only a standard-library
handler gets an ordinary record at its own level and handler, the fields kept. tandem calls neither
`configure` nor structlog. The event names and fields below are public: a rename or a removed field is
**Breaking**, an added field is not.

| Event | Logger | Level | Fields |
|---|---|---|---|
| `lease_renewal.ended`, `lease_end.ended` | `tandem.leases` | info; error when it failed | `outcome`, `duration_ms`, `lease_family`, `holder_id`; `error_type` when failed |
| `lease_state_read.ended`, `lease_roster_read.ended` | `tandem.leases` | info; error when it failed | `outcome`, `duration_ms`, `lease_family`; `error_type` when failed |
| `lock_acquisition.ended`, `lock_extension.ended`, `lock_release.ended`, `lock_state_read.ended` | `tandem.locks` | info; error when it failed | `outcome`, `duration_ms`, `lock_family`; `error_type` when failed |
| `wake_publication.ended` | `tandem.wake` | info; error when it failed | `outcome`, `duration_ms`; `error_type` when failed |
| `lease_renewal.failed`, a background renewal of `kept` | `tandem.leases` | warning | `reason` (`outage` or `refused`), `error_type`, `lease_family`, `holder_id` |
| `lease_end.failed`, the end at the exit of `kept` failed after a cancellation | `tandem.leases` | error | `error_type`, `lease_family`, `holder_id` |
| `lapse_handling.ended`, one `handle` of `serve_lapses` | `tandem.leases` | info; error when it failed, with a traceback unless the failure holds a `StoreError`, which names Redis | `outcome`, `duration_ms`, `lease_family`, `holder_id`; `error_type` when failed |
| `lapse_round.ended`, one round of `serve_lapses` | `tandem.leases` | info with lapses; debug without; warning when an outage failed the round | `outcome`, `duration_ms`, `lease_family`, `lapse_count`, `failed_count` |
| `lapse_claim_extension.failed` | `tandem.leases` | warning | `reason`, `error_type`, `lease_family`, `holder_id` |
| `lapse_claim.expired`, a claim that ran out after its extension was refused or answered false; never after an outage, whose result is unknown | `tandem.leases` | warning | `lease_family`, `holder_id` |
| `wake_subscription.lost`, once per outage; its further attempts | `tandem.wake` | warning; debug | `reason` (`outage`), `error_type`, `failed_attempt_count` |
| `wake_subscription.confirmed`, each `Resubscribed` | `tandem.wake` | info | `failed_attempt_count` |
| `wake_payload.skipped` | `tandem.wake` | warning | `reason` (`invalid_utf8` or `malformed_frame`), `payload_bytes` |

- `outcome` is `ok`, `failed` or `cancelled`; a failed call raises `StoreError` after its line, and a
  `False` or `None` answer is `ok`. A `kept` writes `lease_renewal.ended` for its first renewal only: its
  background renewals write a line only when one fails, and its end at the exit writes `lease_end.ended`
  like a direct `end`; when a cancellation hides a failed end, `lease_end.failed` is its one error line.
- A refusal that reaches the host as `StoreError` out of `serve_lapses` or `received()` writes no line:
  the host's layer logs it.
- A holder is an id (see duty 14), so `holder_id` is the holder string. No line carries a generation, a lapse's
  key, a wake payload, a traceparent or the text of a `StoreError`.
- A lapse is delivered at least once, so a replayed lapse writes a second `lapse_handling.ended`.
- The wake carries its sender's traceparent: a host that handles a wake under `unit_trace(wake.traceparent)`
  writes those lines in the sender's trace (duty 15); without it they are in the pod's own context.

## The host's duties

Redis pub/sub delivers a wake to the readers subscribed at that moment and keeps nothing. Three duties
close that gap, two more cover deadlines, four cover leases, four cover locks, one covers logs and one
covers traces:

1. **Re-read everything the pod serves at each `Resubscribed`.** A wake sent while the pod was
   disconnected is gone for good; the `Resubscribed` after the reconnect is the signal that anything may
   have changed. After a Redis restart the host's own client can still hold stale pooled connections, so
   the first command of that re-read fails with `ConnectionError` on a default `redis.asyncio` client. The
   host's client therefore carries a `Retry`, as in the quick start, or the re-read retries itself. On
   redis-py 5.x the client also passes `retry_on_error` with `ConnectionError` and `TimeoutError`, or its
   `Retry` replays nothing, a lost reply included; redis-py 8 retries those two without it.
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
6. **One `kept` per holder.** Several pods may renew one holder when "any of them is alive" is the
   intended meaning: `renew` is atomic, and the lease then cannot tell them apart. A `kept` ends the shared
   generation at its exit, so only one pod runs it.
7. **The same `lasts` and `claim_for` for every pod of a family.** The lease and the claim are measured by
   the pod that reads them; pods that differ read each other's leases and claims by their own span.
8. **Treat a generation that differs from the stored one as the start.** A reply lost under the client's
   `Retry` can replay `renew` as `started=False`, so a host compares the generation with the one it stored
   instead of trusting `started`. It fences its effects with that generation, checked in the same atomic
   step as the write (a generation is random and compared only by equality).
9. **Reconcile after a Redis restart.** A restart takes every lease, lapse and claim with it: a lapse in
   flight is never delivered, and an absent lease reads UNKNOWN for `lasts` and then GONE. The host
   re-derives what it must free from its durable state, at its reconcile period, and reads GONE as the
   signal that nobody renews.

10. **Treat a `None` from `acquire` as "not yours yet".** It answers a live holder and the `lasts` after a
    Redis restart alike, so the host retries later or gives up, and never reads None as a fault.
11. **Keep the generation and stop at the first `extend` or `release` that returns False.** The lock is
    then lost or ended, and whoever kept working under it fences its effects with the generation, as in
    duty 8. A reply lost under the client's `Retry` replays `acquire` to the same generation, and a
    `release` that succeeded to False.
12. **Extend before `lasts` run out, with the same `lasts` for every pod of a family.** A holder that
    pauses past `lasts` loses its lock to the next `acquire`. The span is measured by the pod that reads it.
13. **Read a lock the host's own script depends on with `tandem.lock_state`, never with `locks.state`.** `locks.state`
    and the script that acts on it are two steps; a host that must refuse or write atomically with the
    lock calls `tandem.lock_state` inside that script. When it reserves by `acquire` and then reads its
    own state again, it releases the lock if that state says another claim came first.
14. **A holder is an id, never a name, an e-mail address or content.** tandem writes the holder as
    `holder_id` in its lines, so a holder that names a person puts that person in the log.
15. **Handle each `Wake` inside `with unit_trace(wake.traceparent):`.** `received()` sets no trace, since a
    context set across its `yield` would leak into the host's loop. The block starts a unit in the sender's
    trace, or in a new one when `traceparent` is None or malformed, and ends it at the block's exit:

    ```python
    from logline import unit_trace

    case Wake(address, traceparent):
        with unit_trace(traceparent):
            await served.reread(address)
    ```

## Fleet standard: cross-pod coordination in Redis

Covers what pods share through Redis: waking one another across pods, deadlines kept in Redis, leases
kept in Redis and locks kept in Redis. It excludes deadlines and holds an app keeps in its own database under its own
transaction, such as skat's chair holds.

New code coordinates through tandem at once. An existing copy moves to tandem now, as soon as the tandem
release that ships its primitive exists. The move runs as one fleet-program item with a sub-issue in each
covered repository. Today tandem ships the wake channel, deadlines, leases and locks.

A deviation is a request naming its enduring harm. tandem's head reviews it, only the operator rules it,
and the ruling is recorded in this section; the process is marketplace's
[How a standard works](https://github.com/overnightworks/marketplace/blob/main/docs/standards.md#how-a-standard-works).

## Limits

- A subscription that dies without closing, such as a half-open TCP connection, is noticed only when the
  operating system reports it: tandem sends no heartbeat of its own, and redis-py's health check does not
  fail a silent pub/sub read.
- Delivery is at most once per subscription; duty 1 and duty 2 make the host's state converge anyway.
- A deadline is as durable as the Redis behind it; duty 5 covers a restart.
- A lease, its lapse and a claim are as durable as the Redis behind them; duty 9 covers a restart.
  The clock is Redis's: a step of it moves every expiry with it.
- A lapse whose handler always raises returns to a claimer every `claim_for`.
- A family without `serve_lapses` keeps its lapse queue until the next Redis restart.
- A paused holder lapses after `lasts` and its next `renew` starts a new generation; its stale effects are
  refused only by the host's own fence.
- A lock is as durable as the Redis behind it. After a restart `acquire` returns None for `lasts`, a lost
  generation's `extend` returns False and `tandem.lock_state` returns `{'unknown'}`, so two holders are
  never live together; a training that held the lock stops, and the host reconciles.
- A first `acquire` of an app whose epoch does not exist yet returns None for `lasts`: Redis cannot tell a
  new app from a restart.
- A lock has no forced release and no holder label: its host keeps who holds it in its own store.
- A generation is 128 random bits, so two starts collide with a negligible probability.
- A generation lets `end` its lease, and `leases.state` gives it to every pod of the app that reaches the
  Redis. That widens no trust boundary, as each pod already can end any lease there, so a generation is an
  app-internal value, never shown to a person or logged.

## Versioning

`tandem.__version__` is the one version. A change of a public name, its type or its meaning is stated in
the pull request as **Breaking** with the hosts' migration. The wake channel's version segment is part of
the contract between pods of different releases: raising it, as 0.8.0 did to `v2`, is **Breaking**, and
pods on either side of it do not hear each other. The names and fields of the log events are
public too: renaming one is **Breaking** on the release page.

## Development

`AGENTS.md` names the commands. The tests run against a real Redis in a throwaway container started by
`scripts/run_redis_tests.sh`.

## Licence

MIT, see `LICENSE`.
