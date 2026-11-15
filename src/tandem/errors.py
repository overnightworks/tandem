"""The one error tandem raises for a Redis that refused or timed out, and where it is built."""

from redis.exceptions import RedisError


class StoreError(Exception):
    """Redis refused a command or did not answer in time; the cause is chained."""


def store_error(operation: str, error: RedisError) -> StoreError:
    """The `StoreError` for `operation` failing with `error`; raise it `from error`."""
    return StoreError(f"{operation} failed: {error}")
