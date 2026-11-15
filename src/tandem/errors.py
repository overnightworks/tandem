"""The one error tandem raises for a Redis that refused or timed out."""


class StoreError(Exception):
    """Redis refused a command or did not answer in time; the cause is chained."""
