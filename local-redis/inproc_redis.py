"""
In-process, socket-free Redis for running arq tests inside sandboxes that block
the ``socket()`` syscall. It shares the exact command implementation
(``mini_redis``) with the standalone TCP server, so behaviour verified here
matches running ``python local-redis/mini_redis.py`` as an independent local
Redis process.

Each asyncio event loop gets one isolated store, shared by every connection in
that loop (i.e. one Redis instance for all workers/pools).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import mini_redis  # noqa: E402
from redis._parsers.base import AsyncBaseParser  # noqa: E402
from redis.asyncio.connection import AbstractConnection  # noqa: E402
from redis.exceptions import ResponseError  # noqa: E402


def get_store() -> mini_redis.Store:
    loop = asyncio.get_running_loop()
    store = getattr(loop, '_mini_redis_store', None)
    if store is None:
        store = mini_redis.Store()
        loop._mini_redis_store = store  # type: ignore[attr-defined]
    return store


class InProcessParser(AsyncBaseParser):
    """Parser backed by the command executor instead of a socket stream."""

    __slots__ = AsyncBaseParser.__slots__ + ('connection_ref', '_connected')

    def __init__(self, socket_read_size: int):
        super().__init__(socket_read_size)
        self.connection_ref: Any = None
        self._connected = False

    def on_connect(self, connection: Any) -> None:
        self.connection_ref = connection
        self._connected = True

    def on_disconnect(self) -> None:
        self._connected = False

    async def can_read_destructive(self) -> bool:
        if not self._connected or self.connection_ref is None:
            return False
        return bool(self.connection_ref._pending)

    async def read_response(self, disable_decoding: bool = False) -> Any:
        if not self._connected:
            raise ConnectionError('in-process connection closed')
        conn = self.connection_ref
        if not conn._pending:
            raise ConnectionError('in-process protocol desync: no pending response')
        result = conn._pending.pop(0)
        if isinstance(result, BaseException):
            raise ResponseError(str(result))
        return result

    async def read_from_socket(self) -> bool:  # pragma: no cover - unused
        return False


class InProcessConnection(AbstractConnection):
    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault('parser_class', InProcessParser)
        super().__init__(**kwargs)
        self._inproc_connected = False
        self._pending: list[Any] = []
        self.store: mini_redis.Store | None = None

    @property
    def is_connected(self) -> bool:  # type: ignore[override]
        return self._inproc_connected

    async def _connect(self) -> None:
        self.store = get_store()
        self._inproc_connected = True
        self._parser.on_connect(self)

    async def connect(self) -> None:
        if not self._inproc_connected:
            await self._connect()

    async def on_connect(self) -> None:
        return None

    async def disconnect(self, nowait: bool = False) -> None:
        if self.store is not None:
            with self.store.lock:
                self.store.watched.pop(self, None)
                self.store.multis.pop(self, None)
        self._inproc_connected = False
        self._pending.clear()
        if getattr(self, '_parser', None) is not None:
            self._parser.on_disconnect()

    async def _disconnect(self, nowait: bool = False, *args: Any, **kwargs: Any) -> None:
        await self.disconnect(nowait=nowait)

    async def send_packed_command(self, command: Any, check_health: bool = True) -> None:
        if not self._inproc_connected:
            await self.connect()
        if isinstance(command, (bytes, bytearray)):
            chunks = [bytes(command)]
        else:
            chunks = [bytes(c) for c in command]
        raw = b''.join(chunks)
        reader = mini_redis.RespReader()
        reader.feed(raw)
        assert self.store is not None
        while True:
            args = reader.read_command()
            if args is None:
                break
            try:
                result = mini_redis.run_command(self.store, self, args)
            except mini_redis.ProtocolError as exc:
                result = exc
            self._pending.append(result)

    async def can_read(self, timeout: float = 0) -> bool:
        return bool(self._pending)

    async def check_health(self) -> None:  # type: ignore[override]
        return None

    def repr_pieces(self) -> list[tuple[str, Any]]:
        return [('in-process', True), ('db', self.db)]


def _make_pool(**kwargs: Any):
    from redis.asyncio.connection import ConnectionPool

    return ConnectionPool(connection_class=InProcessConnection, encoding='utf-8', **kwargs)
