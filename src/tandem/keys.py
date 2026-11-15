"""The names tandem gives in Redis: the one place a key prefix is checked and a channel is built."""

import re

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
    return f"{checked_segment(key_prefix, argument='key_prefix')}:tandem:v1:wake"
