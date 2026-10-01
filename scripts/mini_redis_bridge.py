"""
In-process bridge between redis-py and ``mini_redis``, for local verification in
sandboxes where even unix sockets are forbidden. No real network/socket is used:
the client connection speaks RESP to the in-memory Store over asyncio queues,
and each connection gets its own Conn state so WATCH/MULTI semantics are real.
"""
import asyncio
import sys

from redis.asyncio.connection import Connection

from mini_redis import Conn, Store, enc, err, SimpleError


class QueueStream:
    """Reader side: an asyncio.StreamReader-compatible object fed by the writer."""

    def __init__(self, reader: asyncio.StreamReader):
        self._reader = reader

    async def read(self, n=-1):
        return await self._reader.read(n)

    async def readline(self):
        return await self._reader.readline()

    async def readexactly(self, n):
        return await self._reader.readexactly(n)


class QueueWriter:
    """Writer side: forwards raw RESP command bytes to a consumer callback."""

    def __init__(self, on_data, on_close):
        self._on_data = on_data
        self._on_close = on_close
        self._closed = False

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def write(self, data):
        if not self._closed:
            self._on_data(data)

    async def drain(self):
        await asyncio.sleep(0)

    def close(self):
        if not self._closed:
            self._closed = True
            self._on_close()

    async def wait_closed(self):
        self.close()


class InMemoryConnection(Connection):
    store: Store = None  # type: ignore[assignment]

    async def _connect(self):
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader(limit=2**24)
        conn = Conn(self.store)

        server_buf = b''

        def feed(data: bytes):
            nonlocal server_buf
            server_buf += data
            # parse complete RESP arrays and dispatch immediately
            while server_buf:
                try:
                    argv, consumed = _read_array(server_buf)
                except _Incomplete:
                    return
                server_buf = server_buf[consumed:]
                try:
                    res = conn.run(argv)
                except SimpleError as e:
                    reply = err(e.msg)
                else:
                    reply = b'+QUEUED\r\n' if res == 'QUEUED' else enc(res)
                reader.feed_data(reply)

        def on_close():
            reader.feed_eof()

        self._reader = QueueStream(reader)
        self._writer = QueueWriter(feed, on_close)  # type: ignore[assignment]

    async def check_health(self):
        return None


class _Incomplete(Exception):
    pass


def _read_line(buf: bytes, pos: int):
    i = buf.find(b'\r\n', pos)
    if i < 0:
        raise _Incomplete
    return buf[pos:i], i + 2


def _read_array(buf: bytes):
    pos = 0
    line, pos = _read_line(buf, pos)
    if not line.startswith(b'*'):
        raise SimpleError('expected array')
    n = int(line[1:])
    argv = []
    for _ in range(n):
        hdr, pos = _read_line(buf, pos)
        if not hdr.startswith(b'$'):
            raise SimpleError('expected bulk')
        size = int(hdr[1:])
        payload = buf[pos:pos + size]
        if len(payload) < size:
            raise _Incomplete
        pos += size + 2
        argv.append(payload)
    return argv, pos
