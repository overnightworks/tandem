import asyncio

from tests.support.proxy import TcpProxy
from tests.support.waiting import until, within


async def test_stop_ends_a_connection_still_held_by_the_proxy():
    proxy = TcpProxy("127.0.0.1", 1)
    await proxy.start()
    proxy.hold()
    _, held_writer = await asyncio.open_connection("127.0.0.1", proxy.port)
    try:

        async def handler_is_waiting() -> bool:
            return bool(proxy.tasks)

        await until(handler_is_waiting)

        await within(proxy.stop(), bound=2)
    finally:
        held_writer.close()
