# tandem reference

The exact contract of tandem's public names, its Redis layout, its log events and the duties a host takes on.
The reader is the developer who wires a host in and has the [README](../README.md) at hand; the reasons
behind the design are in [ARCHITECTURE](ARCHITECTURE.md).

## API

| Name | What it is |
|---|---|
| `WakeChannel(redis, *, key_prefix)` | The wake channel of one app on the host's `redis.asyncio.Redis` client. `key_prefix` is one segment matching `[a-z0-9][a-z0-9-]*`; anything else raises `ValueError` naming `key_prefix`. |
| `checked_segment(value, *, argument)` | The check `WakeChannel` applies to `key_prefix`: it returns `value` when it is one segment matching `[a-z0-9][a-z0-9-]*` and raises `ValueError` naming `argument` otherwise. A host that derives a prefix, such as `<prefix>-agents`, validates it with this before it builds a channel. |
| `await channel.publish(address)` | One `PUBLISH` of `address`, the name of what changed and never its content, with the caller's current traceparent (logline's `current_traceparent()`; none outside a trace), sent under the host client's `Retry` like every other command on that client, so a publish whose reply was lost can reach Redis more than once and a wake can arrive twice. A refusal such as `NOPERM` raises `StoreError` at once; a lost request or reply raises it once the client's `Retry` gives up; it is never queued. That is a bounded time only with `retries >= 0` in the client's `Retry` and with `socket_timeout` and `socket_connect_timeout` set to a number: redis-py's default for each is none on 5.3 and 5 seconds on 8.1. A client that `Redis(host=..., port=...)` builds without `retry` has redis-py's own default, which on redis-py 8 sends up to 11 times; a client built with `Redis.from_url(...)` or from a connection pool without `retry` has the connection class's own `Retry(NoBackoff(), 0)`, one attempt, so a lost reply raises at once. tandem checks none of this and states no formula. |
| `channel.following(*, on_resync, on_wake, on_failed, ready_within)` | An async context manager that follows the channel for the host on a pub/sub connection of its own. Entering returns once Redis confirmed the subscription and `on_resync()` returned; with no confirmation within `ready_within` (a positive `timedelta`, required, no default; a `ValueError` otherwise) it raises `StoreError(outage=True)`. `on_resync()` is called at every confirmation, the first and after every reconnect: re-read everything the pod serves. `on_wake(wake)` is called for every wake inside `unit_trace(wake.traceparent)`. The handlers are synchronous and run one at a time, in the order Redis delivered. A refusal or a handler exception before entry returns is raised out of the entry. After it, either ends the following: tandem writes one `wake_follow.failed` line and calls `on_failed(error)` once; an exception of `on_failed` is the host's own error and is raised out of the exit. Leaving the context cancels the following and closes the subscription. |
| `Wake(address, traceparent=None)` | A pod published `address` while it was in the trace `traceparent`; None when its sender had none. It is the sender's value as received: tandem validates nothing, and logline's `unit_trace`, which `following` wraps the handler in, starts a new trace for a malformed one. |
| `Deadlines(redis, *, key_prefix, kind, claim_for)` | The deadlines of one `kind` of one app. `key_prefix` and `kind` are each one segment matching `[a-z0-9][a-z0-9-]*`, and `claim_for` is a positive `timedelta`, the length of a claim; anything else (a `claim_for` that is no `timedelta` included) raises `ValueError` naming the argument. A deadline key is any string. |
| `await deadlines.set(key, due_in)` | `key` comes due `due_in` after now on Redis `TIME` (a pod's clock is never read). A `key` that already waits is re-armed; a `key` that a worker holds is re-armed too and the claim is void, so that worker's later `done` returns False and leaves the new deadline. |
| `await deadlines.cancel(key)` | Removes `key`, waiting or claimed, and voids its claim. True when there was something to remove. |
| `await deadlines.claim_due(*, limit)` | Claims at most `limit` keys that are due, and then keys whose claim ran out, each under a fresh random 128-bit token, and returns them as `Due`s. `limit` is at least 1, or it raises `ValueError` naming the argument. |
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
| `await leases.serve_lapses(handle, *, every)` | Runs until cancelled. Each `every` it sweeps expired leases into the lapse queue and claims the due lapses in small batches until none is due, and calls `await handle(lapse)` for each, at least once. While `handle` runs its claim is extended every `claim_for / 3`; a failed extension is logged and stops, and `handle` is never cancelled. `done` follows a `handle` that returns; one that raises is logged and its lapse returns to a worker when its claim runs out. Each `handle` runs in a trace of its own, which ends with it. An outage of Redis is ridden out with the back-off of the wake channel's following; a refusal raises `StoreError`. |
| `Locks(redis, *, key_prefix, family, lasts)` | The locks of one `family` of one app. A lock lasts `lasts` after its latest start or extension. `key_prefix` and `family` are each one segment matching `[a-z0-9][a-z0-9-]*`, and `lasts` is a positive `timedelta`; anything else raises `ValueError` naming the argument. A name is any string. |
| `await locks.acquire(name)` | Starts the lock on `name` under a fresh random 128-bit generation and returns it, or returns None when a live lock holds `name`, the holder asking again included: a lock is not re-entrant, and `acquire` never extends one. After a Redis restart it returns None until `lasts` have passed since the app's epoch. |
| `await locks.extend(name, generation)` | Keeps the live lock of `generation` for another `lasts` on Redis `TIME`. False, and nothing changes, for an absent, foreign or expired generation, even when nobody took the name since. |
| `await locks.release(name, generation)` | Ends the live lock of `generation` at once; the next `acquire` gets a new generation. False, and nothing changes, for any other generation. tandem has no release without the generation: a crashed holder's lock runs out after `lasts`. |
| `await locks.state(name)` | A `LockState(liveness, generation, remaining)`, read in one script. ALIVE while held, with the holder's `generation` and the `remaining` time; GONE when free; UNKNOWN until `lasts` have passed since the epoch when Redis may have lost the lock. |
| `locks.keys`, `locks.lasts` | The `LockKeys` (`epoch`, `expiry`, `generation`) and the `lasts` of the family, for a host's script that calls `tandem.lock_state`. A script gets the keys as `KEYS` in the order `epoch`, `expiry`, `generation` and builds the table with `local keys = {epoch = KEYS[1], expiry = KEYS[2], generation = KEYS[3]}`. |
| `Slots(redis, *, key_prefix, family, lasts)` | The counted holds of one `family` of one app. A hold lasts `lasts` after its take. `key_prefix` and `family` are each one segment matching `[a-z0-9][a-z0-9-]*`, and `lasts` is a positive `timedelta`; anything else raises `ValueError` naming the argument. A pool is any string. |
| `await slots.take(limits)` | `limits` maps a pool to its cap. Holds one slot in every pool of `limits`, or in none, in one script, and returns the token; returns None when any pool already has its cap of live holds. It never waits. An empty mapping, or a cap that is not an `int`, is a `bool` or is negative, raises `ValueError` naming `limits`; a cap of 0 refuses every take. The token is opaque: the host keeps it and passes it back to `release` unchanged, and its form is no contract. It embeds the pools, so the host treats it like them and never logs it. A reply lost under the client's `Retry` replays the take with the same random part, which returns the same token, so the hold counts once. |
| `await slots.release(token)` | Ends the hold of `token` in all its pools and returns True only when the hold was live. False, and nothing changes, for an unknown token; an expired token returns False and its entry is removed on the way. A reply lost under the client's `Retry` replays a release that succeeded as False. There is no renewal and no count to read. |
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
| `tandem.testing.FakeWakeBus()` | Test-only: the pods of one app in one test, without Redis. `bus.pod()` returns the `FakeWakeChannel` of one more pod; what any pod publishes reaches every pod that follows, the publisher's own included. |
| `FakeWakeChannel` | Test-only: a pod's `publish(address)` and `following(*, on_resync, on_wake, on_failed, ready_within)` with the behaviour of `WakeChannel`'s (same code for `following`: the first confirmation, `ready_within`, the split of a failure before and after entry, the sender's trace, the order and the exit), proven by the same cells against real Redis. It logs no `wake_publication.ended`. It has no outage to ride out; the controls make it lose what Redis would. `resync()` confirms the following again, as a reconnect does; `withhold_confirmation()` leaves a following unconfirmed until `resync()` is called while it is subscribed (a `resync()` before any following keeps the withholding), so its entry raises `StoreError(outage=True)` after `ready_within` and it hears no wake before the confirmation; `refuse()` ends every following with a refusal `StoreError`, and the next entry raises it. Production code must not use it. |
| `FakeWakeChannel.fail_publishes(*errors)` | Test-only: each of the next publishes of that pod raises the next given `StoreError`, in order, and delivers nothing; a later call queues behind the errors not yet raised, and publishes deliver again once none is left. |
| `FakeWakeChannel.hold_publishes()` | Test-only: returns a `PublishHold`; publishes of that pod made while it holds stay pending and deliver nothing, and `release()` lets them finish and deliver in the order they were made. A publish whose caller is cancelled while held delivers nothing. No timing is involved. |

## Redis layout and retries

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

The slots of a family live in one ZSET, `<key_prefix>:tandem:v1:slot:<family>:holders`, scored by each hold's
expiry in ms; a hold's member is the JSON of its token's random part followed by its pools. A pool is never a
key segment. Every `take` removes the expired members of its family, and each granted take sets the key's expiry to
`lasts`, so the key is gone once every hold has expired. Slots read and write no epoch.

The channel is `<key_prefix>:tandem:v2:wake`: one channel per key prefix, so every pod that follows the
same channel uses the same `key_prefix`. An app may run several `WakeChannel`s on one Redis under distinct
prefixes, for example `skat` and `skat-agents`. Pods whose channels carry different version segments never
hear each other, so a host restarts all its pods at once when a release raises the segment, and a rollback
across it goes back for all pods at once as well.

The wake payload is one frame: the decimal byte length of the traceparent, a colon, the traceparent's bytes
and the address's bytes, with no end marker, so any address survives byte for byte, `12:orders` and a colon
included; a length of 0 is a wake with no trace. `WakeChannel.publish` and the Lua `tandem.wake` write the
same bytes. The reader accepts a length of 1 to 9 digits that fits the payload and whose two parts are
UTF-8 and skips anything else as `malformed_frame`; the traceparent reaches the host as received.

`following` hides no reconnect. When the connection breaks, redis-py reconnects inside the retry budget of
the host's client, or tandem backs off (0.5 s doubling to 5 s, with no attempt limit), subscribes again and
carries on; either way the next call is `on_resync`, before any wake it covers. Only a command Redis
refuses outright, such as a subscribe the ACL denies or credentials Redis rejects, ends the following with
`StoreError`, raised out of the entry or, once entered, handed to `on_failed`. A payload that is no valid UTF-8, or no frame of tandem's, is skipped with a
`wake_payload.skipped` warning that names its length, never its content; the next valid wake arrives as before. A client that decodes responses
(`decode_responses=True`) fails inside redis-py on a payload that is no valid UTF-8, so tandem subscribes
again and an `on_resync` call precedes the next wake; a readable payload that is no frame is skipped on the
existing subscription, without one.

The deadlines of a kind live in `<key_prefix>:tandem:v1:d:<kind>:due`, `...:claimed` and `...:claim`, disjoint
from the channel and from every other kind. `claim_for` is the same for every pod of one kind. A claim whose
reply is lost to a retry of the host's client expires after `claim_for` and then reaches a worker again. A `set` or `cancel` that the client's `Retry` runs again cannot tell a replay from a new operation: if another pod re-armed the key in between, a replayed `cancel` removes the new deadline and a replayed `set` overwrites its due time or invalidates its claim.

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
| `slot_take.ended`, `slot_release.ended` | `tandem.slots` | info; error when it failed | `outcome`, `duration_ms`, `slot_family`; `error_type` when failed. A None or False answer is `ok`; no line carries a pool or a token |
| `wake_publication.ended` | `tandem.wake` | info; error when it failed | `outcome`, `duration_ms`; `error_type` when failed |
| `lease_renewal.failed`, a background renewal of `kept` | `tandem.leases` | warning | `reason` (`outage` or `refused`), `error_type`, `lease_family`, `holder_id` |
| `lease_end.failed`, the end at the exit of `kept` failed after a cancellation | `tandem.leases` | error | `error_type`, `lease_family`, `holder_id` |
| `lapse_handling.ended`, one `handle` of `serve_lapses` | `tandem.leases` | info; error when it failed, with a traceback unless the failure holds a `StoreError`, which names Redis | `outcome`, `duration_ms`, `lease_family`, `holder_id`; `error_type` when failed |
| `lapse_round.ended`, one round of `serve_lapses` | `tandem.leases` | info with lapses; debug without; warning when an outage failed the round | `outcome`, `duration_ms`, `lease_family`, `lapse_count`, `failed_count` |
| `lapse_claim_extension.failed` | `tandem.leases` | warning | `reason`, `error_type`, `lease_family`, `holder_id` |
| `lapse_claim.expired`, a claim that ran out after its extension was refused or answered false; never after an outage, whose result is unknown | `tandem.leases` | warning | `lease_family`, `holder_id` |
| `wake_subscription.lost`, once per outage; its further attempts | `tandem.wake` | warning; debug | `reason` (`outage`), `error_type`, `failed_attempt_count` |
| `wake_subscription.confirmed`, each `on_resync` call | `tandem.wake` | info | `failed_attempt_count` |
| `wake_follow.failed`, the following ended after its entry because a handler raised or Redis refused | `tandem.wake` | error | `error_type` |
| `wake_payload.skipped` | `tandem.wake` | warning | `reason` (`invalid_utf8` or `malformed_frame`), `payload_bytes` |

- `outcome` is `ok`, `failed` or `cancelled`; a failed call raises `StoreError` after its line, and a
  `False` or `None` answer is `ok`. A `kept` writes `lease_renewal.ended` for its first renewal only: its
  background renewals write a line only when one fails, and its end at the exit writes `lease_end.ended`
  like a direct `end`; when a cancellation hides a failed end, `lease_end.failed` is its one error line.
- A refusal that reaches the host as `StoreError` out of `serve_lapses` or the entry of `following` writes no line:
  the host's layer logs it.
- A holder is an id (see duty 13), so `holder_id` is the holder string. No line carries a generation, a lapse's
  key, a wake payload, a traceparent or the text of a `StoreError`.
- A lapse is delivered at least once, so a replayed lapse writes a second `lapse_handling.ended`.
- The wake carries its sender's traceparent: `following` calls `on_wake` inside `unit_trace(wake.traceparent)`,
  so the handler's lines are in the sender's trace.

## The host's duties

Redis pub/sub delivers a wake to the readers subscribed at that moment and keeps nothing. `following`
calls the host's re-read at each resync, holds the pod's start until the first one and carries each wake's trace. Two
duties remain for the wake, two more cover deadlines, four cover leases, four cover locks, four cover slots and one covers logs:

1. **Give the client a `Retry`, so the re-read at `on_resync` survives a Redis restart.** After a restart the
   host's own client can still hold stale pooled connections, so the first command of that re-read fails
   with `ConnectionError` on a default `redis.asyncio` client. The host's client therefore carries a
   `Retry`, as in the quick start, or the re-read retries itself. On redis-py 5.x the client also passes
   `retry_on_error` with `ConnectionError` and `TimeoutError`, or its `Retry` replays nothing, a lost
   reply included; redis-py 8 retries those two without it.
2. **After a failed `publish`, publish again or bound the readers' staleness.** `publish` ends when the
   client's `Retry` gives up, like `Script.run` and every command on that client; that is finite only with
   `retries >= 0` and with `socket_timeout` and `socket_connect_timeout` set, so the host sets them and
   builds its client with a `retry`. A failure means the delivery is uncertain: the `PUBLISH` may never
   have reached Redis, or Redis delivered it and only the reply was lost, and the `Retry` may already have
   sent it more than once. One `publish` of the same address after the failure runs the `Retry` again and
   only causes a duplicate wake. Alternatively every reader re-reads on a period, which bounds how stale it
   can get. A loop of the host's own around `publish` multiplies both the sends and the bound.
3. **Fence the effect of a claimed deadline with your own version.** A due key reaches a handler at least
   once and each key has at most one valid claim at a time, yet a handler that pauses past its claim runs
   beside the next worker after a takeover. The host checks and writes its effect against a version or
   generation of its own, in one atomic step of its own store, so the older handler's effect is refused.
4. **Reconcile the deadlines after a Redis restart.** A restart takes every pending deadline and every claim
   with it. The host derives what its durable state needs (the deadlines it guards) and `set`s them again
   at start and after it notices a restart, for example at an `on_resync` call.
5. **One `kept` per holder.** Several pods may renew one holder when "any of them is alive" is the
   intended meaning: `renew` is atomic, and the lease then cannot tell them apart. A `kept` ends the shared
   generation at its exit, so only one pod runs it.
6. **The same `lasts` and `claim_for` for every pod of a family.** The lease and the claim are measured by
   the pod that reads them; pods that differ read each other's leases and claims by their own span.
7. **Treat a generation that differs from the stored one as the start.** A reply lost under the client's
   `Retry` can replay `renew` as `started=False`, so a host compares the generation with the one it stored
   instead of trusting `started`. It fences its effects with that generation, checked in the same atomic
   step as the write (a generation is random and compared only by equality).
8. **Reconcile after a Redis restart.** A restart takes every lease, lapse and claim with it: a lapse in
   flight is never delivered, and an absent lease reads UNKNOWN for `lasts` and then GONE. The host
   re-derives what it must free from its durable state, at its reconcile period, and reads GONE as the
   signal that nobody renews.
9. **Treat a `None` from `acquire` as "not yours yet".** It answers a live holder and the `lasts` after a
    Redis restart alike, so the host retries later or gives up, and never reads None as a fault.
10. **Keep the generation and stop at the first `extend` or `release` that returns False.** The lock is
    then lost or ended, and whoever kept working under it fences its effects with the generation, as in
    duty 7. A reply lost under the client's `Retry` replays `acquire` to the same generation, and a
    `release` that succeeded to False.
11. **Extend before `lasts` run out, with the same `lasts` for every pod of a family.** A holder that
    pauses past `lasts` loses its lock to the next `acquire`. The span is measured by the pod that reads it.
12. **Read a lock the host's own script depends on with `tandem.lock_state`, never with `locks.state`.** `locks.state`
    and the script that acts on it are two steps; a host that must refuse or write atomically with the
    lock calls `tandem.lock_state` inside that script. When it reserves by `acquire` and then reads its
    own state again, it releases the lock if that state says another claim came first.
13. **A holder is an id, never a name, an e-mail address or content.** tandem writes the holder as
    `holder_id` in its lines, so a holder that names a person puts that person in the log.
14. **A `take` that raised `StoreError` may still have granted a hold.** The script may have run and only
    the reply been lost, and the hold then occupies its slots until `lasts` runs out. The host treats the
    take as refused and does not retry in a loop that would fill the pool.
15. **The same `lasts` for every pod of a family.** A hold's expiry is stamped with the taking pod's `lasts`,
    and each granted take sets the whole family key's expiry to that pod's `lasts`. A pod with a shorter
    `lasts` therefore drops every other pod's holds early, once the key runs out, and the cap is exceeded; a
    longer one only keeps the key alive past expired holds, which a take still prunes by score.
16. **After a Redis restart a cap can be exceeded by the lost holds for at most `lasts`.** Slots read no
    epoch, so takes are granted at once, and the host accepts that overshoot.
    A host that moves an existing limiter onto `Slots` counts in two places while old and new pods run
    side by side, so each cap can be reached twice over for that deploy window; a change of `family` or
    `key_prefix` does the same.
17. **A take costs O(live holds of the family).** A pool that every take includes is bounded by its cap, and
    nothing bounds a family whose takes name pools that other takes leave out.
