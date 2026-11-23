"""The two ways the store fails a client that `StoreError` tells apart: an outage and a refusal."""

from dataclasses import dataclass

from redis.asyncio import Redis

from tandem.errors import FailureReason
from tests.support.proxy import TcpProxy


@dataclass(frozen=True)
class StoreFailure:
    """A failure of `client`'s store that a test begins and ends."""

    reason: FailureReason
    client: Redis
    proxy: TcpProxy
    observer: Redis
    user: str  # the ACL user of a refusing client

    async def begin(self) -> None:
        if self.reason is FailureReason.OUTAGE:
            self.proxy.cut()
        else:
            await self.observer.acl_setuser(self.user, enabled=True, commands=["-evalsha"])

    async def end(self) -> None:
        if self.reason is FailureReason.OUTAGE:
            self.proxy.restore()
        else:
            await self.observer.acl_setuser(self.user, enabled=True, commands=["+evalsha"])
