# tandem architecture

## 1. Context and Scope

tandem runs inside each pod of a host app, with no server or storage of its own; its one neighbour is the
Redis the host already uses, whose clock (`TIME`) stamps every deadline and lease. Host developers start at
the [README](../README.md) and the [reference](reference.md).

```mermaid
flowchart LR
    subgraph podA["Pod A of the host app"]
        hostA["Host code"] -- "publish(address)" --> tandemA["tandem<br/>WakeChannel"]
    end
    subgraph podB["Pod B of the host app"]
        tandemB["tandem<br/>WakeChannel"] -- "on_resync, on_wake" --> hostB["Host code"]
    end
    tandemA -- "PUBLISH" --> redis[("Redis<br/>one channel per key prefix")]
    redis -- "message, subscribe confirmation" --> tandemB
```

Outside: the host, which owns its state, its Redis client (pool, retry, timeouts) and what an address means,
and Redis, which keeps no wake. tandem imports only `redis` and `logline` (the fleet's logger: a version
range by name in tandem, whose tag the host pins); an import contract keeps test tools out of it.

## 2. Building Block View

Module owners in `tandem`:

- `wake` owns `WakeChannel` and `Wake`: the `PUBLISH`, the frame of a payload, the pub/sub connection of each
  `following`, the back-off loop around it and what a host would otherwise write around that loop:
  readiness, the bounded start, the serialised handling, the sender's trace and the split of a failure before
  and after the entry.
- `deadlines` owns `Deadlines` and `Due`: the claim, extend and done scripts and the claim tokens.
- `leases` owns `Leases` and its records: the renew, end, state, live and sweep scripts and the loops of
  `kept` and `serve_lapses`. Its lapse queue is a `DeadlineQueue` of `deadlines` (the transitions of
  `Deadlines` over any three keys), so a lapse is claimed, extended and finished like a deadline.
- `locks` owns `Locks` and `LockState`: the acquire, extend, release and state scripts and the generations.
  It shares the app's epoch and `Liveness` with `leases` and has no lapse queue.
- `lua` owns `LUA_KIT`, the one implementation of each transition that the classes and a host's script run;
  `script` owns `Script`, which prepends it, runs by SHA and reloads on `NOSCRIPT`; `client` owns `Client`.
- `keys` owns every name tandem gives in Redis and the check of a key prefix and a kind.
- `testing` owns `settle_epoch` and the in-memory wake channel (`FakeWakeBus`, `FakeWakeChannel`), the helpers
  for a host's tests; `__all__` lists the module, so they are public. The fake extends `wake`'s one base of
  `following`, so a host's tests run the real handler dispatch, and the `following` cells run against it and
  against Redis alike, so a drift fails tandem's CI.
- `errors` owns `StoreError`, the one mapping of a Redis failure onto it, its classification as an outage or
  a refusal, and the back-off both loops use.

## 3. Runtime View

```mermaid
sequenceDiagram
    participant H as Host
    participant W as following()
    participant R as Redis
    H->>W: async with, ready_within
    W->>R: SUBSCRIBE channel
    R-->>W: subscribe confirmation
    W-->>H: on_resync(), the host re-reads everything
    W-->>H: the entry returns
    R-->>W: message
    W-->>H: on_wake(Wake) in the sender's trace
    R--xW: connection lost
    alt inside the client's retry budget
        W->>R: redis-py reconnects and subscribes again
    else beyond it
        W->>W: back off 0.5 s doubling to 5 s
        W->>R: SUBSCRIBE channel on a fresh connection
    end
    R-->>W: subscribe confirmation
    W-->>H: on_resync()
```

A deadline is in one of three places; every arrow is one Lua script on Redis `TIME`:

```mermaid
stateDiagram-v2
    [*] --> Waiting: set (due ZSET)
    Waiting --> Waiting: set again
    Waiting --> Claimed: claim_due, due reached (new token)
    Claimed --> Claimed: extend, token current and unexpired
    Claimed --> Claimed: claim_due, claim expired (new token)
    Claimed --> Waiting: set (token void)
    Claimed --> [*]: done, token current
    Waiting --> [*]: cancel
    Claimed --> [*]: cancel (token void)
```

Per kind there are a due ZSET scored by the due time in ms, a claimed ZSET scored by the claim's expiry and
a HASH from key to the current token. A claim is valid exactly while its token is the current one, so `set`,
`cancel` and a takeover void the old claim in the script that moves the key.

A lease is in the expiry ZSET while it lives and its generation is in a HASH. Every lease script reads
`TIME`, sets the epoch when absent and reads the lease state, so these are one snapshot:

```mermaid
stateDiagram-v2
    [*] --> Live: renew, no live lease (new generation)
    Live --> Live: renew (same generation)
    Live --> Lapse: end, current generation
    Live --> Lapse: expiry passed, swept by serve_lapses or by the next renew
    Lapse --> Lapse: claimed by serve_lapses (claim_due, extend)
    Lapse --> [*]: done
```

`state` writes only the epoch: a lapse in any state is GONE, or UNKNOWN until `lasts` have passed since
the epoch.

A lock is one ZSET member scored by its expiry and one HASH field holding its generation; every lock
script starts with `tandem.lock_state`, so a host's admit script that calls it takes the same snapshot:

```mermaid
stateDiagram-v2
    [*] --> Held: acquire, free and settled (new generation)
    Held --> Held: extend, current generation
    Held --> [*]: release, current generation
    Held --> [*]: expiry passed
```

An absent lock reads free only once `lasts` have passed since the epoch; before that `acquire` returns None.

## 4. Deployment View

A library in the host's environment, installed by a pinned git tag; nothing of tandem is deployed or run.

## 5. Crosscutting Concepts

**Trust.** The host, its Redis and the other pods of the app are trusted; Redis's own ACL decides who may
publish and subscribe. The channel carries addresses, never contents: whoever can publish can make every
pod re-read, and nothing more.

**Failure.** Every failure of Redis is a `StoreError` that says whether it was an outage or a refusal;
`following` and `serve_lapses` ride out an outage and end on a refusal, and `publish` says nothing of
whether the wake arrived. The [reference](reference.md) says how a failure reaches the host.

**Logging.** Every log point writes through logline's event contract on a standard-library logger
`tandem.<module>`: a literal `<noun>.<verb_past>` event with its fields in `extra=`. logline's `StepTimer`
times `publish`, each lease call and each lock call; `Deadlines` and `Script.run` write no line. A lapse
handler and a `serve_lapses` round write their own lines, because their levels and traceback do not fit the
timer. tandem never calls `configure` or structlog: the host owns the process's logging. No line carries a
generation, a value that can end a lease. Each lapse handling runs in its own trace (`unit_trace`), so its
lines and the host handler's share a `trace_id`. The [reference](reference.md#what-tandem-logs) lists the
events; they are public.

**Proof strategy.** The tests run every cell against a real Redis, and the cells of the plain flow also run
on fakeredis, the `following` cells also on the in-memory wake channel of `tandem.testing`; the lowest direct
dependency floors run the same tests. Branch coverage is 100 %, measured over all tests. `AGENTS.md` names
the commands and the rules for waits and outages.

## 6. Constraints and reasons

- Each `following` opens one pub/sub connection of its own from the host's client: a subscribed connection
  cannot run other commands. On leaving the context it cancels and awaits its reader, which unsubscribes and
  closes the connection.
- `on_resync` is called for every `subscribe` confirmation Redis sends, including the one redis-py's own
  re-subscription triggers inside the client's retry budget; both redis-py versions CI runs deliver it through
  `get_message`, so tandem never changes the host's client settings, pool or `Retry`.
- `publish` is one `PUBLISH` that is never retried or queued: a queued wake would outlive the state it
  describes, and a retry cannot tell a lost request from a lost reply. It sends the one command itself on a
  pooled connection, so a `publish` is absent from redis-py's operation metrics; the [reference](reference.md)
  says what the client's `Retry` still governs.
- The wake payload is a frame that carries the sender's traceparent beside the address. The handlers are
  synchronous and `following` runs them on its one reader task, in `unit_trace`, so handling is one at a time
  in Redis's order and a trace ends with its call. The channel's version segment is a contract between pods
  of different releases; the [reference](reference.md#redis-layout-and-retries) states the frame and what a
  change costs a host.
- Every transition of a deadline, lease or lock is one Lua script that reads `TIME`, so no pod clock is
  involved and a transition cannot interleave with another.
- A deadline key or lock name is a ZSET member and a HASH field, never a key segment, so a free-form value
  cannot reach a fixed key. The segment after `v1` is one of the fixed tokens `epoch`, `d`, `l` and `lock`, and
  a caller's `kind` or `family` appears only after its token and before a fixed suffix.
- Generations and claim tokens are 128 random bits from `secrets`, generated in Python because Lua's random is
  not fit, compared by equality and never ordered, so no counter is needed that a Redis restart would reset;
  the collision bound is accepted.
- The epoch is created by `SET NX` inside every lease and lock script, so UNKNOWN ends at most `lasts` after
  the first command after a restart, with no separate initialisation and no race between pods. The grace
  works only when the epoch is lost together with the leases and locks it guards: a Redis that forgets some
  keys and keeps the epoch (an eviction, a partial restore) reads a lost lock as free, and two holders can
  then be live together. The guarantee holds on a Redis that forgets all or nothing: one instance, never a
  replica (it can lag behind the epoch or the locks) and never a partial snapshot.
- A lock is not a lease: `Leases.renew` extends a live lease for any caller, so a lock built on it would be
  held for ever by the losers that keep polling; `extend` needs the generation.
- `tandem.lock_state` returns nil for a free lock, so `if tandem.lock_state(...) then` fails closed: a held
  lock and an unsettled epoch both refuse, in one atomic step with the host's write.
- A lapse leaves the expiry ZSET for the lapse queue in the script that notices it, so a claim on a lapse
  never touches the expiry that decides ALIVE. `serve_lapses` extends every pending claim of its batch, so the
  claims of a mass lapse do not run out while earlier handlers run.
- Scripts run through the host's client, so its `Retry` can replay one after a lost reply; the
  [reference](reference.md#redis-layout-and-retries) says what a replay does.

## 7. Not chosen

- Redis Streams: they keep history and need consumer groups, offsets and trimming; a re-read at
  `on_resync` already restores any state a lost wake described.
- One channel per address: the channels grow with the data and every reconnect resubscribes them all.
- A heartbeat of tandem's: it reopens if a host meets a silent half-open connection in operation.

## 8. Risks and Technical Debt

- Redis pub/sub is at most once: a wake published while a subscription is down is lost, and only the host's
  re-read at `on_resync` and its handling of a failed `publish` bring the pods back in line.
- The reader blocks in a read with no timeout, as redis-py's pub/sub does, so a connection that dies
  without closing is noticed late. `on_resync` and `on_wake` run one at a time in Redis's order, so a slow
  synchronous handler holds every later wake and a long first re-read delays the entry's return.
