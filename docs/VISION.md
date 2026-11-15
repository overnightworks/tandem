# tandem vision

## What it does

tandem is where the fleet's apps get their multi-pod coordination through Redis, once, so that each app does
not grow its own. An app that runs in several pods uses it to tell its other pods that something changed and
to know, after any break in the connection, that it must read its state again.

It carries addresses and never contents: the wake says what changed, and the truth stays in the host's own
store. A fix to how a wake is lost or doubled is made once, and every app gets it by taking the next release.

It is made by and for agents: every rule a machine can check is in the checks, and the documents say what
an agent cannot derive from the code.

## Who uses it today

The operator and the agents that build and run his apps. No app has adopted it yet.

## Deliberate non-goals

- No storage of the host's state: tandem moves a wake, the host keeps the truth.
- No delivery guarantee beyond "the host re-reads at each `Resubscribed`": pub/sub is at most once, and the
  duties in the README make up the rest.
- No client of its own: a host passes its `redis.asyncio.Redis` and keeps its pool, retry and timeouts.
- No compatibility layers: a change that breaks its callers breaks cleanly and says so.
