"""The names tandem gives in Redis: the one place a key prefix is checked and a name is built."""

import re
from dataclasses import dataclass

_SEGMENT = re.compile(r"[a-z0-9][a-z0-9-]*")


def checked_segment(value: str, *, argument: str) -> str:
    """Return `value` when it is one key segment, else raise ValueError naming the argument.

    The check `WakeChannel` applies to its `key_prefix`, so a host validates a prefix it derives
    before it builds a channel.
    """
    if not isinstance(value, str) or _SEGMENT.fullmatch(value) is None:
        raise ValueError(
            f"{argument} must be one segment matching {_SEGMENT.pattern}, got {value!r}"
        )
    return value


def wake_channel(key_prefix: str) -> str:
    """The wake channel of the app that owns `key_prefix`."""
    return f"{checked_segment(key_prefix, argument='key_prefix')}:tandem:v2:wake"


@dataclass(frozen=True)
class DeadlineKeys:
    """The three keys of one deadline kind: the due ZSET, the claimed ZSET and the claim HASH."""

    due: str
    claimed: str
    claim: str


def deadline_keys(key_prefix: str, kind: str) -> DeadlineKeys:
    """The keys of the deadlines of `kind` in the app that owns `key_prefix`."""
    base = (
        f"{checked_segment(key_prefix, argument='key_prefix')}:tandem:v1:d:"
        f"{checked_segment(kind, argument='kind')}"
    )
    return DeadlineKeys(due=f"{base}:due", claimed=f"{base}:claimed", claim=f"{base}:claim")


@dataclass(frozen=True)
class LeaseKeys:
    """The keys of one lease family: the app's epoch, the expiry ZSET, the generation HASH and the
    lapse queue, which is a deadline kind of its own."""

    epoch: str
    expiry: str
    generation: str
    lapse: DeadlineKeys


def _app(key_prefix: str) -> str:
    return f"{checked_segment(key_prefix, argument='key_prefix')}:tandem:v1"


def epoch_key(key_prefix: str) -> str:
    """The app's epoch, which every lease and lock family of `key_prefix` shares."""
    return f"{_app(key_prefix)}:epoch"


def lease_keys(key_prefix: str, family: str) -> LeaseKeys:
    """The keys of the leases of `family` in the app that owns `key_prefix`."""
    app = _app(key_prefix)
    base = f"{app}:l:{checked_segment(family, argument='family')}"
    lapse = f"{base}:lapse"
    return LeaseKeys(
        epoch=epoch_key(key_prefix),
        expiry=f"{base}:expiry",
        generation=f"{base}:generation",
        lapse=DeadlineKeys(due=f"{lapse}:due", claimed=f"{lapse}:claimed", claim=f"{lapse}:claim"),
    )


@dataclass(frozen=True)
class LockKeys:
    """The keys of one lock family: the app's epoch, the expiry ZSET and the generation HASH.

    A host's script passes them from its own KEYS in this order and builds the `keys` table of
    `tandem.lock_state` with `{epoch = KEYS[1], expiry = KEYS[2], generation = KEYS[3]}`.
    """

    epoch: str
    expiry: str
    generation: str


def lock_keys(key_prefix: str, family: str) -> LockKeys:
    """The keys of the locks of `family` in the app that owns `key_prefix`."""
    app = _app(key_prefix)
    base = f"{app}:lock:{checked_segment(family, argument='family')}"
    return LockKeys(
        epoch=epoch_key(key_prefix), expiry=f"{base}:expiry", generation=f"{base}:generation"
    )


def slot_key(key_prefix: str, family: str) -> str:
    """The ZSET that holds every live hold of the slots of `family` in the app of `key_prefix`."""
    return f"{_app(key_prefix)}:slot:{checked_segment(family, argument='family')}:holders"
