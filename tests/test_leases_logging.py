import logging
from collections.abc import Awaitable, Callable

import pytest
from logline import Outcome
from redis.asyncio import Redis

from tandem import Leases, StoreError
from tests.support.logs import LogLines

type LeaseCall = Callable[[Leases], Awaitable[object]]

_HOLDER = "pod-a"
_LEASE_CALLS: dict[str, tuple[LeaseCall, str | None]] = {
    "lease_renewal.ended": (lambda leases: leases.renew(_HOLDER), _HOLDER),
    "lease_end.ended": (lambda leases: leases.end(_HOLDER, "any generation"), _HOLDER),
    "lease_state_read.ended": (lambda leases: leases.state([_HOLDER]), None),
    "lease_roster_read.ended": (lambda leases: leases.live(), None),
}


@pytest.mark.parametrize("event", _LEASE_CALLS)
async def test_every_lease_call_the_host_awaits_writes_one_line_with_its_outcome(
    event: str,
    url_denying: Callable[[str], Awaitable[str]],
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    log_lines: LogLines,
):
    """Spec: LOG-3, LOG-4, LOG-18, LOG-21."""
    call, holder = _LEASE_CALLS[event]
    refused = new_leases(make_client(await url_denying("evalsha")))

    await call(new_leases(make_client(redis_url)))
    with pytest.raises(StoreError):
        await call(refused)

    done, failed = log_lines.of(event, "tandem.leases")
    assert done["level"] == "info"
    assert done["outcome"] == Outcome.OK
    assert failed["level"] == "error"
    assert failed["outcome"] == Outcome.FAILED
    assert failed["error_type"] == StoreError.__name__
    assert done["duration_ms"] >= 0
    assert failed["duration_ms"] >= 0
    for line in (done, failed):
        assert line["lease_family"] == "owner"
        assert line.get("holder_id") == holder


@pytest.mark.parametrize(("level", "lines"), [(logging.INFO, 1), (logging.WARNING, 0)])
async def test_a_host_with_only_a_standard_handler_gets_the_lines_at_its_level_with_their_fields(
    level: int,
    lines: int,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    caplog: pytest.LogCaptureFixture,
):
    """Spec: LOG-1."""
    with caplog.at_level(level, logger="tandem"):
        await new_leases(make_client(redis_url)).renew(_HOLDER)

    written = [record for record in caplog.records if record.name == "tandem.leases"]
    assert len(written) == lines
    for record in written:
        assert record.__dict__["lease_family"] == "owner"
        assert record.__dict__["outcome"] == Outcome.OK
