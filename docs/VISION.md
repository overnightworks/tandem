# tandem vision

## What it does

tandem is where the fleet's apps get their multi-pod coordination through Redis, once, so that each app does
not grow its own. An app that runs in several pods uses it for four things:

- the wake: tell its other pods that something changed, and know after any break in the connection that it
  must read its state again;
- deadlines: a key that comes due on Redis's clock and reaches one claimer at a time;
- leases: a holder's claim to be alive, with a generation per start and a claim on each lapse;
- locks: a hold on a name that only its generation extends or releases, started only when none is held.

It carries addresses and generations and never contents: the truth stays in the host's own store. A fix to
how a wake is lost or doubled, or how a lease lapses, is made once, and every app gets it by taking the next
release.

It is made by and for agents: every rule a machine can check is in the checks, and the documents say what
an agent cannot derive from the code.

## Who uses it today

The operator and the agents that build and run his apps: every app of his that pins tandem in its manifest.

## Deliberate non-goals

- No storage of the host's state: tandem moves a wake, the host keeps the truth.
- No delivery guarantee beyond "the host re-reads at each `on_resync`": pub/sub is at most once, and the
  duties in the reference make up the rest.
- No client of its own: a host passes its `redis.asyncio.Redis` and keeps its pool, retry and timeouts.
- No compatibility layers: a change that breaks its callers breaks cleanly and says so.
