from collections.abc import Awaitable, Callable

import pytest
from logline import Outcome
from redis.asyncio import Redis

from tandem import Locks, StoreError
from tests.support.logs import LogLines

type LockCall = Callable[[Locks], Awaitable[object]]

_LOCK_CALLS: dict[str, LockCall] = {
    "lock_acquisition.ended": lambda locks: locks.acquire("job"),
    "lock_extension.ended": lambda locks: locks.extend("job", "any generation"),
    "lock_release.ended": lambda locks: locks.release("job", "any generation"),
    "lock_state_read.ended": lambda locks: locks.state("job"),
}


@pytest.mark.parametrize("event", _LOCK_CALLS)
async def test_every_lock_call_writes_one_line_with_its_outcome(
    event: str,
    url_denying: Callable[[str], Awaitable[str]],
    redis_url: str,
    make_client: Callable[..., Redis],
    new_locks: Callable[..., Locks],
    log_lines: LogLines,
):
    call = _LOCK_CALLS[event]

    await call(new_locks(make_client(redis_url)))
    with pytest.raises(StoreError):
        await call(new_locks(make_client(await url_denying("evalsha"))))

    done, failed = log_lines.of(event, "tandem.locks")
    assert (done["level"], done["outcome"]) == ("info", Outcome.OK)
    assert (failed["level"], failed["outcome"]) == ("error", Outcome.FAILED)
    assert failed["error_type"] == StoreError.__name__
    assert done["lock_family"] == failed["lock_family"]
