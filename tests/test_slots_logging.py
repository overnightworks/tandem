from collections.abc import Awaitable, Callable

import pytest
from logline import Outcome
from redis.asyncio import Redis

from tandem import Slots, StoreError
from tests.support.logs import LogLines

type SlotCall = Callable[[Slots], Awaitable[object]]

_POOL = "person-4711"
_SLOT_CALLS: dict[str, SlotCall] = {
    "slot_take.ended": lambda slots: slots.take({_POOL: 1}),
    "slot_release.ended": lambda slots: slots.release("any token"),
}


@pytest.mark.parametrize("event", _SLOT_CALLS)
async def test_every_slot_call_writes_one_line_with_its_outcome(
    event: str,
    url_denying: Callable[[str], Awaitable[str]],
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
    log_lines: LogLines,
):
    call = _SLOT_CALLS[event]
    refused = new_slots(make_client(await url_denying("evalsha")))

    await call(new_slots(make_client(redis_url)))
    with pytest.raises(StoreError):
        await call(refused)

    done, failed = log_lines.of(event, "tandem.slots")
    assert done["level"] == "info"
    assert done["outcome"] == Outcome.OK
    assert failed["level"] == "error"
    assert failed["outcome"] == Outcome.FAILED
    assert failed["error_type"] == StoreError.__name__
    assert done["slot_family"] == failed["slot_family"]


async def test_a_full_pool_answers_ok_and_no_line_carries_a_pool_or_a_token(
    redis_url: str,
    make_client: Callable[..., Redis],
    new_slots: Callable[..., Slots],
    log_lines: LogLines,
):
    slots = new_slots(make_client(redis_url))
    token = await slots.take({_POOL: 1})
    assert token is not None
    assert await slots.take({_POOL: 1}) is None
    await slots.release(token)
    await slots.release(token)

    takes = log_lines.of("slot_take.ended", "tandem.slots")
    releases = log_lines.of("slot_release.ended", "tandem.slots")
    assert [line["outcome"] for line in takes + releases] == [Outcome.OK] * 4
    log_lines.assert_none_holds(_POOL)
    log_lines.assert_none_holds(token)
