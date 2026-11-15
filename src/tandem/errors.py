"""The one error tandem raises for a Redis that refused or timed out, and its classification."""

from redis import exceptions as redis_errors
from redis.exceptions import RedisError

BACKOFF_FIRST_SECONDS = 0.5
BACKOFF_CAP_SECONDS = 5.0


class StoreError(Exception):
    """Redis refused a command or did not answer in time; the cause is chained.

    `outage` tells a lost connection or a timeout, which a later attempt can survive, from a
    refusal (rejected credentials, a missing right, an error reply), which it cannot.
    """

    def __init__(self, message: str, *, outage: bool = False) -> None:
        super().__init__(message)
        self.outage = outage


def is_outage(error: RedisError) -> bool:
    """True for a lost connection or a timeout; false for a refusal, rejected credentials included.

    redis-py derives its authentication errors from `ConnectionError`, so they are excluded first.
    """
    if isinstance(error, redis_errors.AuthenticationError | redis_errors.AuthorizationError):
        return False
    return isinstance(error, redis_errors.ConnectionError | redis_errors.TimeoutError)


def store_error(operation: str, error: RedisError) -> StoreError:
    """The `StoreError` for `operation` failing with `error`; raise it `from error`."""
    return StoreError(f"{operation} failed: {error}", outage=is_outage(error))


def next_backoff(seconds: float) -> float:
    """The wait after `seconds`: doubled, up to the cap."""
    return min(seconds * 2, BACKOFF_CAP_SECONDS)
