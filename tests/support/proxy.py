"""A small TCP proxy in front of Redis that a test can cut, black-hole and restore."""

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from enum import Enum, auto
from typing import Any


class Mode(Enum):
    FORWARD = auto()
    CUT = auto()  # accepts and closes at once: the client sees a refused connection
    DROP_REQUESTS = auto()  # keeps the connection, discards what the client sends
    DROP_REPLIES = auto()  # forwards what the client sends, discards what the server answers


class TcpProxy:
    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        self._upstream = (upstream_host, upstream_port)
        self._mode = Mode.FORWARD
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._attempts = 0
        self._attempt_seen = asyncio.Condition()
        self.tasks: set[asyncio.Task[None]] = set()

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    @property
    def attempts_while_cut(self) -> int:
        return self._attempts

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)

    async def stop(self) -> None:
        assert self._server is not None
        self._server.close()
        self._drop_connections()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self._server.wait_closed()

    def cut(self) -> None:
        """Refuse new connections and close the open ones."""
        self._attempts = 0
        self._mode = Mode.CUT
        self._drop_connections()

    def drop_requests(self) -> None:
        self._mode = Mode.DROP_REQUESTS

    def drop_replies(self) -> None:
        self._mode = Mode.DROP_REPLIES

    def restore(self) -> None:
        self._mode = Mode.FORWARD

    async def attempts_reached(self, count: int) -> None:
        """Wait until `count` connection attempts were refused since the last cut."""
        async with self._attempt_seen:
            await self._attempt_seen.wait_for(lambda: self._attempts >= count)

    def _drop_connections(self) -> None:
        for writer in tuple(self._writers):
            writer.close()

    async def _accept(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        handler = asyncio.current_task()
        assert handler is not None
        self.tasks.add(handler)
        try:
            await self._relay(client_reader, client_writer)
        finally:
            self.tasks.discard(handler)

    async def _relay(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        if self._mode is Mode.CUT:
            client_writer.close()
            async with self._attempt_seen:
                self._attempts += 1
                self._attempt_seen.notify_all()
            return
        upstream_reader, upstream_writer = await asyncio.open_connection(*self._upstream)
        self._writers |= {client_writer, upstream_writer}
        pumps = [
            self._spawn(
                self._pump(
                    client_reader, upstream_writer, lambda: self._mode is not Mode.DROP_REQUESTS
                )
            ),
            self._spawn(
                self._pump(
                    upstream_reader, client_writer, lambda: self._mode is not Mode.DROP_REPLIES
                )
            ),
        ]
        await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        for writer in (client_writer, upstream_writer):
            writer.close()
            self._writers.discard(writer)
        for pump in pumps:
            pump.cancel()

    def _spawn(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def _pump(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        forwarding: Callable[[], bool],
    ) -> None:
        with contextlib.suppress(ConnectionError, asyncio.IncompleteReadError):
            while chunk := await reader.read(65536):
                if forwarding():
                    writer.write(chunk)
                    await writer.drain()
