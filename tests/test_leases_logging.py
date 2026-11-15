import logging
from collections.abc import Awaitable, Callable

import pytest
from logline import Outcome
from redis.asyncio import Redis

from tandem import Leases, StoreError
from tests.support.logs import LogLines

type LeaseCall = Callable[[Leases], Awaitable[object]]

_LEASE_CALLS: dict[str, LeaseCall] = {
    "lease_renewal.ended": lambda leases: leases.renew("pod-a"),
    "lease_end.ended": lambda leases: leases.end("pod-a", "any generation"),
    "lease_state_read.ended": lambda leases: leases.state(["pod-a"]),
    "lease_roster_read.ended": lambda leases: leases.live(),
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
    call = _LEASE_CALLS[event]

    await call(new_leases(make_client(redis_url)))
    with pytest.raises(StoreError):
        await call(new_leases(make_client(await url_denying("evalsha"))))

    done, failed = log_lines.of(event, "tandem.leases")
    assert (done["level"], done["outcome"], done["lease_family"]) == ("info", Outcome.OK, "owner")
    assert (failed["level"], failed["outcome"]) == ("error", Outcome.FAILED)
    assert failed["error_type"] == StoreError.__name__
    assert done["duration_ms"] >= 0 and failed["duration_ms"] >= 0


@pytest.mark.parametrize(("level", "lines"), [(logging.INFO, 1), (logging.WARNING, 0)])
async def test_a_host_with_only_a_standard_handler_gets_the_lines_at_its_level_with_their_fields(
    level: int,
    lines: int,
    redis_url: str,
    make_client: Callable[..., Redis],
    new_leases: Callable[..., Leases],
    caplog: pytest.LogCaptureFixture,
):
    with caplog.at_level(level, logger="tandem"):
        await new_leases(make_client(redis_url)).renew("pod-a")

    written = [record for record in caplog.records if record.name == "tandem.leases"]
    assert len(written) == lines
    for record in written:
        assert record.__dict__["lease_family"] == "owner"
        assert record.__dict__["outcome"] == Outcome.OK
