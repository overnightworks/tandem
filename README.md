# tandem

The fleet's shared Redis coordination for apps that run in several pods. Today it holds one tool, a wake
channel with resync: a pod that changed something tells the others which address changed, and every
reader learns when it must re-read everything it serves. It builds on [redis-py](https://redis.readthedocs.io/)'s
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
from redis.asyncio import Redis

from tandem import Resubscribed, StoreError, Wake, WakeChannel

redis = Redis.from_url("redis://redis:6379")
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
```

## The API

| Name | What it is |
|---|---|
| `WakeChannel(redis, *, key_prefix, sleep=asyncio.sleep)` | The wake channel of one app on the host's `redis.asyncio.Redis` client. `key_prefix` is one segment matching `[a-z0-9][a-z0-9-]*`; anything else raises `ValueError` naming `key_prefix`. `sleep` is the wait between reconnect attempts; only tests replace it. |
| `await channel.publish(address)` | One `PUBLISH` of `address`, the name of what changed and never its content. It raises `StoreError` when Redis refuses or does not answer. tandem never retries or queues it, and the client's own `Retry` does not apply to it. |
| `channel.received()` | An async iterator of `Wake` and `Resubscribed` that runs until it is cancelled. Each call opens a pub/sub connection of its own. |
| `Wake(address)` | A pod published `address`. |
| `Resubscribed()` | Redis acknowledged the subscription, the first time and after every reconnect. |
| `StoreError` | Redis refused or timed out; the cause is chained. |

The channel is `<key_prefix>:tandem:v1:wake`: one channel per app, so every pod of an app uses the same
`key_prefix`.

`received()` hides no reconnect. When the connection breaks, redis-py reconnects inside the retry budget of
the host's client, or tandem backs off (0.5 s doubling to 5 s, with no attempt limit), subscribes again and
carries on; either way the next event is a `Resubscribed`, before any wake it covers. Only a command Redis
refuses outright, such as a subscribe the ACL denies or credentials Redis rejects, ends `received()` with
`StoreError`.

## The host's duties

Redis pub/sub delivers a wake to the readers subscribed at that moment and keeps nothing. Three duties
close that gap:

1. **Re-read everything the pod serves at each `Resubscribed`.** A wake sent while the pod was
   disconnected is gone for good; the `Resubscribed` after the reconnect is the signal that anything may
   have changed.
2. **After a failed `publish`, publish again or bound the readers' staleness.** A failure means the
   delivery is uncertain: the `PUBLISH` may never have reached Redis, or Redis delivered it and only the
   reply was lost. A second `publish` of the same address only causes a duplicate wake. Alternatively every
   reader re-reads on a period, which bounds how stale it can get.
3. **A pod turns Ready at its first `Resubscribed`.** Before it, the pod is deaf to wakes and a state it
   served earlier may be stale.

## Limits

- A subscription that dies without closing, such as a half-open TCP connection, is noticed only when the
  operating system reports it: tandem sends no heartbeat of its own, and redis-py's health check does not
  fail a silent pub/sub read.
- Delivery is at most once per subscription; duty 1 and duty 2 make the host's state converge anyway.

## Versioning

`tandem.__version__` is the one version. A change of a public name, its type or its meaning is stated in
the pull request as **Breaking** with the hosts' migration.

## Development

`AGENTS.md` names the commands. The tests run against a real Redis in a throwaway container started by
`scripts/run_redis_tests.sh`.

## Licence

MIT, see `LICENSE`.
