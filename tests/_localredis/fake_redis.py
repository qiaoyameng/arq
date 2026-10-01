"""
A small, single-threaded asyncio server implementing just enough of the RESP3/RESP2 protocol
to run arq's integration tests without docker or a real redis-server.

Supported commands: GET, SET (NX/XX/PX/EX/KEEPTTL), SETEX, PSETEX, MGET, INCR,
EXISTS, DEL, KEYS, EXPIRE, PEXPIRE, TTL, PTTL, TYPE,
ZADD, ZREM, ZSCORE, ZRANGE (BYSCORE / WITHSCORES), ZRANGEBYSCORE, ZINCRBY,
ZCARD, ZCOUNT, ZREMRANGEBYSCORE,
MULTI, EXEC, WATCH, UNWATCH, DISCARD, PING, INFO, DBSIZE, FLUSHALL, FLUSHDB,
CLIENT/COMMAND/RESET/QUIT no-ops.
"""
from __future__ import annotations

import asyncio
import fnmatch
import re
import time
from typing import Any, Optional


class FakeRedisServer:
    def __init__(self) -> None:
        # keys: bytes -> (value bytes, expiry_ms or None)
        self.strings: dict[bytes, tuple[bytes, Optional[float]]] = {}
        # keys: bytes -> {member bytes: score float}
        self.zsets: dict[bytes, dict[bytes, float]] = {}
        self.watched: dict[asyncio.Queue, set[bytes]] = {}
        self.version = '7.2.0-fake'

    def now_ms(self) -> float:
        # wall clock milliseconds, matching arq's timestamp_ms() used for zset scores and px ttl
        return time.time() * 1000

    def _alive(self, key: bytes) -> bool:
        item = self.strings.get(key)
        if item is None:
            return key in self.zsets
        _, expiry = item
        if expiry is not None and expiry <= self.now_ms():
            self.strings.pop(key, None)
            return False
        return True

    def _get(self, key: bytes) -> Optional[bytes]:
        item = self.strings.get(key)
        if item is None:
            return None
        value, expiry = item
        if expiry is not None and expiry <= self.now_ms():
            self.strings.pop(key, None)
            return None
        return value

    def _set(self, key: bytes, value: bytes, px_ms: Optional[int], nx: bool, xx: bool) -> bool:
        exists = self._alive(key)
        if nx and exists:
            return False
        if xx and not exists:
            return False
        expiry = None if px_ms is None else self.now_ms() + px_ms
        self.strings[key] = (value, expiry)
        return True

    def _ttl_ms(self, key: bytes) -> int:
        if key in self.zsets and key not in self.strings:
            return -1
        item = self.strings.get(key)
        if item is None:
            return -2
        _, expiry = item
        if expiry is None:
            return -1
        remaining = expiry - self.now_ms()
        return max(0, int(remaining))

    def _delete(self, *keys: bytes) -> int:
        count = 0
        for key in keys:
            if self.strings.pop(key, None) is not None:
                count += 1
            if key in self.zsets:
                del self.zsets[key]
                count += 1 if key not in self.strings else 0
        return count

    def notify_watch(self, key: bytes) -> None:
        for queue, watched_keys in list(self.watched.items()):
            if key in watched_keys:
                queue.put_nowait(b'X')

    # ------------------------------------------------------------------ commands
    def execute(self, args: list[bytes]) -> Any:
        cmd = args[0].decode().upper()
        handler = getattr(self, f'cmd_{cmd.lower()}', None)
        if handler is None:
            return Err(f"unknown command '{cmd}'")
        return handler(args[1:])

    def cmd_get(self, a):
        return self._get(a[0])

    def cmd_set(self, a):
        key, value = a[0], a[1]
        i = 2
        nx = xx = False
        px_ms: Optional[int] = None
        keep_ttl = False
        while i < len(a):
            opt = a[i].decode().upper()
            if opt == 'NX':
                nx = True
            elif opt == 'XX':
                xx = True
            elif opt == 'PX':
                px_ms = int(a[i + 1])
                i += 1
            elif opt == 'EX':
                px_ms = int(a[i + 1]) * 1000
                i += 1
            elif opt == 'KEEPTTL':
                keep_ttl = True
            i += 1
        if keep_ttl:
            item = self.strings.get(key)
            if item is not None:
                px_ms = None if item[1] is None else max(0, int(item[1] - self.now_ms()))
        ok = self._set(key, value, px_ms, nx, xx)
        if ok:
            self.notify_watch(key)
            return b'OK'
        return None

    def cmd_setex(self, a):
        self._set(a[0], a[2], int(a[1]) * 1000, False, False)
        self.notify_watch(a[0])
        return b'OK'

    def cmd_psetex(self, a):
        self._set(a[0], a[2], int(a[1]), False, False)
        self.notify_watch(a[0])
        return b'OK'

    def cmd_mget(self, a):
        return [self._get(k) for k in a]

    def cmd_incr(self, a):
        value = self._get(a[0])
        n = int(value) + 1 if value is not None else 1
        self.strings[a[0]] = (str(n).encode(), None)
        self.notify_watch(a[0])
        return n

    def cmd_incrby(self, a):
        value = self._get(a[0])
        n = int(value) + int(a[1]) if value is not None else int(a[1])
        self.strings[a[0]] = (str(n).encode(), None)
        self.notify_watch(a[0])
        return n

    def cmd_exists(self, a):
        return sum(1 for k in a if self._alive(k))

    def cmd_del(self, a):
        n = self._delete(*a)
        for k in a:
            self.notify_watch(k)
        return n

    def cmd_type(self, a):
        key = a[0]
        if key in self.strings:
            return b'string'
        if key in self.zsets:
            return b'zset'
        return b'none'

    def cmd_keys(self, a):
        pattern = a[0].decode()
        out = []
        for container in (self.strings, self.zsets):
            for key in container:
                if self._alive(key) and fnmatch.fnmatchcase(key.decode(), pattern):
                    out.append(key)
        return sorted(set(out))

    def cmd_expire(self, a):
        return self._apply_expire(a[0], int(a[1]) * 1000)

    def cmd_pexpire(self, a):
        return self._apply_expire(a[0], int(a[1]))

    def _apply_expire(self, key: bytes, ms: int) -> int:
        if not self._alive(key):
            return 0
        value, _ = self.strings[key]
        self.strings[key] = (value, self.now_ms() + max(0, ms))
        return 1

    def cmd_ttl(self, a):
        ms = self._ttl_ms(a[0])
        return ms if ms < 0 else ms // 1000

    def cmd_pttl(self, a):
        return self._ttl_ms(a[0])

    # --------------------------------------------------------------- sorted sets
    def cmd_zadd(self, a):
        key = a[0]
        i = 1
        flags = set()
        while i < len(a):
            try:
                float(a[i])
                break
            except ValueError:
                flags.add(a[i].decode().upper())
                i += 1
        zset = self.zsets.setdefault(key, {})
        added = changed = 0
        while i < len(a):
            score = float(a[i])
            member = a[i + 1]
            i += 2
            if member in zset:
                if 'NX' in flags:
                    continue
                if ('GT' in flags and score <= zset[member]) or ('LT' in flags and score >= zset[member]):
                    continue
                if zset[member] != score:
                    zset[member] = score
                    changed += 1
            else:
                if 'XX' in flags:
                    continue
                added += 1
                zset[member] = score
        self.notify_watch(key)
        return changed if 'CH' in flags else added

    def _zset_view(self, key: bytes) -> list[tuple[bytes, float]]:
        zset = self.zsets.get(key, {})
        return sorted(zset.items(), key=lambda kv: (kv[1], kv[0]))

    def cmd_zrem(self, a):
        key = a[0]
        zset = self.zsets.get(key)
        if zset is None:
            return 0
        n = 0
        for member in a[1:]:
            if zset.pop(member, None) is not None:
                n += 1
        if not zset:
            self.zsets.pop(key, None)
        self.notify_watch(key)
        return n

    def cmd_zscore(self, a):
        zset = self.zsets.get(a[0])
        if zset is None or a[1] not in zset:
            return None
        return zset[a[1]]

    def cmd_zcard(self, a):
        return len(self.zsets.get(a[0], {}))

    def cmd_zcount(self, a):
        lo, hi = self._parse_bound(a[1]), self._parse_bound(a[2])
        return sum(1 for _, s in self._zset_view(a[0]) if lo <= s <= hi)

    @staticmethod
    def _parse_bound(b: bytes) -> float:
        s = b.decode()
        if s in ('-inf', '+inf'):
            return float(s)
        if s.startswith('('):
            return float(s[1:]) + 1e-12
        return float(s)

    def cmd_zincrby(self, a):
        key, amount, member = a[0], float(a[1]), a[2]
        zset = self.zsets.setdefault(key, {})
        zset[member] = zset.get(member, 0.0) + amount
        self.notify_watch(key)
        return zset[member]

    def cmd_zremrangebyscore(self, a):
        key = a[0]
        lo, hi = self._parse_bound(a[1]), self._parse_bound(a[2])
        zset = self.zsets.get(key)
        if zset is None:
            return 0
        doomed = [m for m, s in zset.items() if lo <= s <= hi]
        for m in doomed:
            del zset[m]
        if not zset:
            self.zsets.pop(key, None)
        if doomed:
            self.notify_watch(key)
        return len(doomed)

    def cmd_zrangebyscore(self, a):
        key, lo, hi = a[0], self._parse_bound(a[1]), self._parse_bound(a[2])
        offset, count = 0, None
        withscores = False
        i = 3
        if i < len(a) and a[i].decode().upper() == 'LIMIT':
            offset = int(a[i + 1])
            count = int(a[i + 2])
            i += 3
        while i < len(a):
            if a[i].decode().upper() == 'WITHSCORES':
                withscores = True
            i += 1
        rows = [(m, s) for m, s in self._zset_view(key) if lo <= s <= hi]
        if offset:
            rows = rows[offset:]
        if count is not None and count >= 0:
            rows = rows[:count]
        return self._format_zrange(rows, withscores)

    def cmd_zrange(self, a):
        key = a[0]
        byscore = False
        withscores = False
        i = 3
        if len(a) > 3 and a[3].decode().upper() == 'BYSCORE':
            byscore = True
            i = 4
        rest = [x.decode().upper() for x in a[i:]]
        withscores = 'WITHSCORES' in rest
        if byscore:
            lo, hi = self._parse_bound(a[1]), self._parse_bound(a[2])
            rows = [(m, s) for m, s in self._zset_view(key) if lo <= s <= hi]
            if 'LIMIT' in rest:
                j = rest.index('LIMIT')
                offset = int(a[i:][j + 1])
                count = int(a[i:][j + 2])
                rows = rows[offset : offset + count if count >= 0 else None]
            return self._format_zrange(rows, withscores)
        start, end = int(a[1]), int(a[2])
        view = self._zset_view(key)
        if end < 0:
            end = len(view) + end
        rows = view[start : end + 1]
        return self._format_zrange(rows, withscores)

    @staticmethod
    def _format_zrange(rows, withscores):
        if not withscores:
            return [m for m, _ in rows]
        out: list[Any] = []
        for member, score in rows:
            out.append(member)
            out.append(score)
        return out

    # ------------------------------------------------------------- admin / misc
    def cmd_ping(self, a):
        return a[0] if a else b'PONG'

    def cmd_info(self, a):
        return (
            f'# Server\r\nredis_version:{self.version}\r\n# Memory\r\nused_memory_human:1M\r\n'
            f'# Clients\r\nconnected_clients:1\r\n'
        ).encode()

    def cmd_dbsize(self, a):
        return len({k for k in list(self.strings) + list(self.zsets) if self._alive(k)})

    def cmd_flushall(self, a):
        self.strings.clear()
        self.zsets.clear()
        return b'OK'

    cmd_flushdb = cmd_flushall

    def cmd_echo(self, a):
        return a[0]

    def cmd_client(self, a):
        return b'OK'

    def cmd_command(self, a):
        # redis-py only uses this on connect, a harmless empty reply is fine
        return []

    def cmd_reset(self, a):
        return b'RESET OK'

    def cmd_select(self, a):
        return b'OK'

    def cmd_quit(self, a):
        return b'OK'


class Err(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def encode_response(value: Any) -> bytes:
    if value is None:
        return b'$-1\r\n'
    if isinstance(value, Err):
        return b'-' + value.message.encode() + b'\r\n'
    if isinstance(value, bool):
        return (b':1\r\n' if value else b':0\r\n')
    if isinstance(value, int):
        return f':{value}\r\n'.encode()
    if isinstance(value, float):
        if value.is_integer():
            text = str(int(value))
        else:
            text = repr(value)
        return f'${len(text)}\r\n{text}\r\n'.encode()
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value)
        return b'$' + str(len(value)).encode() + b'\r\n' + value + b'\r\n'
    if isinstance(value, str):
        return encode_response(value.encode())
    if isinstance(value, (list, tuple)):
        out = b'*' + str(len(value)).encode() + b'\r\n'
        for item in value:
            out += encode_response(item)
        return out
    raise TypeError(f'cannot encode {type(value)}')


class FakeRedisClient:
    def __init__(self, server: FakeRedisServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.server = server
        self.reader = reader
        self.writer = writer
        self.multi: Optional[list[list[bytes]]] = None
        self.watch_queue: Optional[asyncio.Queue] = None
        self.watched_keys: set[bytes] = set()

    async def read_command(self) -> Optional[list[bytes]]:
        line = await self.reader.readline()
        if not line:
            return None
        if line[0:1] == b'*':
            count = int(line[1:])
            parts = []
            for _ in range(count):
                header = await self.reader.readline()
                if not header.startswith(b'$'):
                    raise ValueError(f'expected bulk string header, got {header!r}')
                length = int(header[1:])
                data = await self.reader.readexactly(length)
                await self.reader.readexactly(2)  # trailing \r\n
                parts.append(data)
            return parts
        # inline commands (rare in tests)
        text = line.decode().strip()
        return text.split() if text else await self.read_command()

    def reply(self, value: Any) -> None:
        self.writer.write(encode_response(value))

    async def serve(self) -> None:
        try:
            while True:
                command = await self.read_command()
                if command is None:
                    break
                await self.dispatch(command)
                await self.writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            if self.watch_queue is not None:
                self.server.watched.pop(self.watch_queue, None)
            self.writer.close()

    async def dispatch(self, command: list[bytes]) -> None:
        cmd = command[0].decode().upper()

        if cmd == 'MULTI':
            self.multi = []
            self.reply(b'OK')
            return
        if cmd == 'WATCH':
            if self.watch_queue is None:
                self.watch_queue = asyncio.Queue()
            for key in command[1:]:
                self.watched_keys.add(key)
            self.server.watched[self.watch_queue] = self.watched_keys
            self.reply(b'OK')
            return
        if cmd == 'UNWATCH':
            if self.watch_queue is not None:
                self.server.watched.pop(self.watch_queue, None)
                self.watch_queue = None
            self.watched_keys = set()
            self.reply(b'OK')
            return
        if cmd == 'DISCARD':
            self.multi = None
            self.reply(b'OK')
            return
        if cmd == 'EXEC':
            queued = self.multi
            self.multi = None
            broken = False
            if self.watch_queue is not None:
                broken = not self.watch_queue.empty()
                self.server.watched.pop(self.watch_queue, None)
                self.watch_queue = None
                self.watched_keys = set()
            if broken:
                self.reply(None)
                return
            if queued is None:
                self.reply(Err('EXEC without MULTI'))
                return
            replies = []
            for item in queued:
                try:
                    replies.append(self.server.execute(item))
                except Err as e:
                    replies.append(e)
            self.reply(replies)
            return

        if self.multi is not None:
            self.multi.append(command)
            self.reply(b'QUEUED')
            return

        try:
            result = self.server.execute(command)
        except Err as e:
            self.reply(e)
            return
        self.reply(result)


async def start_fake_redis(host: str = '127.0.0.1', port: int = 0) -> tuple[asyncio.AbstractServer, int]:
    server = FakeRedisServer()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await FakeRedisClient(server, reader, writer).serve()

    srv = await asyncio.start_server(handle, host, port)
    assigned_port = srv.sockets[0].getsockname()[1]
    return srv, assigned_port


class _MemoryTransport(asyncio.Transport):
    """In-memory transport pairing two asyncio StreamReader/StreamWriter pairs."""

    def __init__(
        self,
        peer_reader: asyncio.StreamReader,
        peer_protocol: asyncio.StreamReaderProtocol,
        own_protocol: asyncio.StreamReaderProtocol,
    ):
        super().__init__()
        self._peer_reader = peer_reader
        self._peer_protocol = peer_protocol
        self._own_protocol = own_protocol
        self._closing = False
        self._loop = asyncio.get_event_loop()

    def write(self, data: bytes) -> None:
        peer_closed = getattr(self._peer_protocol, '_connection_lost', self._closing)
        own_closed = getattr(self._own_protocol, '_connection_lost', self._closing)
        if not self._closing and not peer_closed and not own_closed:
            # schedule delivery so the peer's read callbacks don't run synchronously inside our
            # write() (that would starve the peer coroutine and deadlock flow control)
            self._loop.call_soon(self._feed_safe, data)

    def _feed_safe(self, data: bytes) -> None:
        if not self._closing and not getattr(self._peer_protocol, '_connection_lost', False):
            try:
                self._peer_reader.feed_data(data)
            except (AssertionError, RuntimeError):
                pass

    def writelines(self, list_of_data) -> None:  # type: ignore[override]
        self.write(b''.join(list_of_data))

    def can_write_eof(self) -> bool:
        return True

    def write_eof(self) -> None:
        self.close()

    def close(self) -> None:
        if not self._closing:
            self._closing = True
            # connection_lost on the peer drains its reader (EOF) and completes this writer's
            # wait_closed() future, exactly like a real socket close
            self._peer_protocol.connection_lost(None)
            self._own_protocol.connection_lost(None)

    def is_closing(self) -> bool:
        return self._closing

    def abort(self) -> None:
        self.close()

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        pass

    def get_protocol(self) -> asyncio.BaseProtocol:
        return self._peer_protocol

    def is_reading(self) -> bool:
        return True

    def pause_reading(self) -> None:
        pass

    def resume_reading(self) -> None:
        pass


def _connected_stream_pair() -> tuple[asyncio.StreamReader, asyncio.StreamWriter, asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_running_loop()
    reader_a = asyncio.StreamReader(limit=2**20, loop=loop)
    reader_b = asyncio.StreamReader(limit=2**20, loop=loop)
    protocol_a = asyncio.StreamReaderProtocol(reader_a, loop=loop)
    protocol_b = asyncio.StreamReaderProtocol(reader_b, loop=loop)
    transport_a = _MemoryTransport(reader_b, protocol_b, protocol_a)
    transport_b = _MemoryTransport(reader_a, protocol_a, protocol_b)
    protocol_a.connection_made(transport_a)
    protocol_b.connection_made(transport_b)
    writer_a = asyncio.StreamWriter(transport_a, protocol_a, reader_b, loop)
    writer_b = asyncio.StreamWriter(transport_b, protocol_b, reader_a, loop)
    return reader_a, writer_a, reader_b, writer_b


class InProcessFakeRedis:
    """
    Holds a shared :class:`FakeRedisServer` and serves redis-py connections in-process.
    """

    def __init__(self) -> None:
        self.server = FakeRedisServer()

    async def new_endpoints(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        client_reader, client_writer, server_reader, server_writer = _connected_stream_pair()
        client = FakeRedisClient(self.server, server_reader, server_writer)
        task = asyncio.get_running_loop().create_task(client.serve())
        client_writer.transport._server_task = task  # keep a reference
        return client_reader, client_writer
