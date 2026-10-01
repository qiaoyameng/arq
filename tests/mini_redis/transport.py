"""In-process connection between redis-py and :class:`MiniRedis` without sockets.

The sandbox disallows binding/listening sockets, so we wire redis-py's
``Connection._reader``/``Connection._writer`` straight to the mini server via a
pair of in-memory asyncio transports. The full RESP protocol is still spoken.
"""

import asyncio

from redis.asyncio.connection import Connection


class MemTransport(asyncio.Transport):
    def __init__(self, loop: asyncio.AbstractEventLoop, protocol: asyncio.Protocol, peer: 'MemTransport'):
        super().__init__(extra=None)
        self._loop = loop
        self._protocol = protocol
        self._peer = peer
        self._closing = False
        self._paused = False

    def write(self, data: bytes) -> None:
        if not self._closing and data:
            self._loop.call_soon(self._peer._protocol.data_received, data)

    def writelines(self, list_of_data) -> None:
        self.write(b''.join(list_of_data))

    def can_write_eof(self) -> bool:
        return True

    def write_eof(self) -> None:
        self.close()

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        peer = self._peer
        # defer the peer disconnect slightly so a connection pool closing while
        # a reply is still being delivered does not surface as a spurious error
        if peer is not None and not self._loop.is_closed():
            self._loop.call_later(0.01, peer._protocol.connection_lost, None)

    def is_closing(self) -> bool:
        return self._closing

    def pause_reading(self) -> None:
        self._paused = True

    def resume_reading(self) -> None:
        self._paused = False

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol  # type: ignore[assignment]

    def get_protocol(self) -> asyncio.BaseProtocol:
        return self._protocol


def patch_connection_for(server) -> None:
    """Monkeypatch redis-py connections to dial the in-process ``server``."""

    async def _connect(self: Connection) -> None:
        loop = asyncio.get_running_loop()

        client_reader: asyncio.StreamReader = asyncio.StreamReader(loop=loop)
        server_reader: asyncio.StreamReader = asyncio.StreamReader(loop=loop)

        client_proto = asyncio.StreamReaderProtocol(client_reader, loop=loop)

        async def on_client_connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await server._handle(reader, writer)
            except asyncio.CancelledError:
                pass

        server_proto = asyncio.StreamReaderProtocol(server_reader, on_client_connected, loop=loop)

        client_transport = MemTransport(loop, client_proto, None)  # type: ignore[arg-type]
        server_transport = MemTransport(loop, server_proto, client_transport)  # type: ignore[arg-type]
        client_transport._peer = server_transport

        client_proto.connection_made(client_transport)
        server_proto.connection_made(server_transport)

        self._reader = client_reader
        self._writer = asyncio.StreamWriter(client_transport, client_proto, client_reader, loop)

    Connection._connect = _connect  # type: ignore[method-assign]
