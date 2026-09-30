"""
In-process, pipe-backed RESP2 redis server used for local demos/tests where no
real redis-server is reachable (the sandbox forbids even loopback sockets).

It speaks genuine RESP over os.pipe pairs, so the unmodified redis-py client
(including pipelines and pub/sub) drives it; only the transport is fake.

Feature set: only what arq needs (GET/SET-with-PX/EXISTS/DEL/TYPE/EXPIRE/PTTL,
ZADD/ZSCORE/ZREM/ZRANGE, WATCH/MULTI/EXEC, PING, PUBLISH/SUBSCRIBE/UNSUBSCRIBE).

The server additionally supports ``#emulator:drop``: writing that raw frame on
a client->server connection makes that connection drop, which lets demos inject
a pub/sub connection outage at precise instants.
"""

from __future__ import annotations

import asyncio
import os
import time
from asyncio.unix_events import _UnixReadPipeTransport, _UnixWritePipeTransport
from typing import Any, Optional

from redis.asyncio.connection import AbstractConnection

CRLF = b'\r\n'


_NO_RESPONSE = object()


def encode(value: Any) -> bytes:
    if value is None:
        return b'$-1' + CRLF
    if isinstance(value, bool):
        value = 1 if value else 0
    if isinstance(value, int):
        return b':' + str(value).encode() + CRLF
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, bytes):
        return b'$' + str(len(value)).encode() + CRLF + value + CRLF
    if isinstance(value, (list, tuple)):
        out = b'*' + str(len(value)).encode() + CRLF
        for item in value:
            out += encode(item)
        return out
    if isinstance(value, BaseException):
        return b'-' + str(value).encode() + CRLF
    raise TypeError(f'cannot encode {value!r}')


async def read_command(reader: asyncio.StreamReader) -> Optional[list[bytes]]:
    line = await reader.readline()
    if not line:
        return None
    if line[0:1] != b'*':
        # inline command, incl. emulator control frames
        return [line.strip()]
    n = int(line[1:])
    args = []
    for _ in range(n):
        header = await reader.readline()
        length = int(header[1:])
        data = await reader.readexactly(length)
        await reader.readexactly(2)  # CRLF
        args.append(data)
    return args


class FakeRedisServer:
    def __init__(self) -> None:
        self.data: dict[bytes, bytes] = {}
        self.expire_at: dict[bytes, float] = {}
        self.zsets: dict[bytes, dict[bytes, float]] = {}
        self.subscribers: dict[bytes, set['FakeConnection']] = {}
        self.connections: set['FakeConnection'] = set()
        self.lock = asyncio.Lock()
        self.publish_count = 0
        # optional awaitable hook(args) invoked before executing every command;
        # used by demos to stall commands at precise race timing points.
        # ``during_subscribe(args)`` runs after the channel is registered on the
        # server but before the SUBSCRIBE confirmation is sent - the real-world
        # window where a publisher can hit the new subscription while the client
        # still thinks SUBSCRIBE is in flight.
        self.before_command: Any = None
        self.during_subscribe: Any = None
        # invoked whenever a client starts a (GET/ZSCORE) state read
        self.before_state_check: Any = None

    def _alive(self, key: bytes) -> bool:
        if key not in self.data:
            return False
        at = self.expire_at.get(key)
        if at is not None and at <= time.monotonic():
            self.data.pop(key, None)
            self.expire_at.pop(key, None)
            return False
        return True

    def _set_expire_ms(self, key: bytes, ms: Optional[int]) -> None:
        if ms is None:
            self.expire_at.pop(key, None)
        elif ms > 0:
            self.expire_at[key] = time.monotonic() + ms / 1000
        else:
            self.data.pop(key, None)
            self.expire_at.pop(key, None)

    async def shutdown(self) -> None:
        """Close every connection so their pipe transports and serve tasks end."""
        conns = list(self.connections)
        for conn in conns:
            conn.closing = True
            for transport in conn.transports:
                try:
                    transport.close()
                except Exception:
                    pass
        await asyncio.sleep(0.05)
        for conn in conns:
            if conn.serve_task is not None:
                try:
                    await asyncio.wait_for(conn.serve_task, timeout=1)
                except (asyncio.TimeoutError, Exception):
                    pass

    async def execute(self, conn: 'FakeConnection', args: list[bytes]) -> Any:
        cmd = args[0].upper()
        a = args[1:]

        if conn.in_subscribe and cmd not in (b'SUBSCRIBE', b'UNSUBSCRIBE', b'PING', b'RESET', b'QUIT'):
            return Exception('Can not execute command while in pub/sub mode')

        if cmd == b'PING':
            if a:
                return a[0]
            return b'PONG'
        if cmd == b'INFO':
            # worker health/version info; a minimal RESP bulk string is enough
            section = a[0].lower() if a else b'server'
            if b'redis_version' in section or section == b'server' or section == b'all':
                return b'redis_version:8.0.0(emulated)\r\n'
            return b'\r\n'
        if cmd == b'DBSIZE':
            return sum(1 for k in list(self.data) if self._alive(k))
        if cmd == b'ECHO':
            return a[0]
        if cmd == b'TIME':
            now = time.time()
            return [str(int(now)).encode(), str(int((now % 1) * 1e6)).encode()]
        if cmd == b'COMMAND' or cmd == b'HELLO':
            return []
        if cmd == b'RESET':
            conn.in_subscribe = False
            conn.channels.clear()
            for waiters in self.subscribers.values():
                waiters.discard(conn)
            return b'RESET OK'
        if cmd == b'QUIT':
            conn.closing = True
            return b'OK'

        if cmd == b'GET':
            return self.data.get(a[0]) if self._alive(a[0]) else None
        if cmd == b'SET':
            key, value = a[0], a[1]
            px = None
            i = 2
            while i < len(a):
                opt = a[i].upper()
                if opt == b'PX':
                    px = int(a[i + 1])
                    i += 2
                elif opt in (b'EX', b'KEEPTTL', b'XX', b'NX', b'GET'):
                    i += 2 if opt == b'EX' else 1
                else:
                    i += 1
            self.data[key] = value
            self._set_expire_ms(key, px)
            return b'OK'
        if cmd == b'PSETEX':
            key, ms, value = a[0], int(a[1]), a[2]
            self.data[key] = value
            self._set_expire_ms(key, ms)
            return b'OK'
        if cmd == b'EXISTS':
            return sum(1 for k in a if self._alive(k))
        if cmd == b'DEL':
            n = 0
            for k in a:
                if self._alive(k):
                    self.data.pop(k, None)
                    self.expire_at.pop(k, None)
                    n += 1
            return n
        if cmd == b'INCR':
            v = int(self.data.get(a[0], b'0')) + 1
            self.data[a[0]] = str(v).encode()
            return v
        if cmd == b'INCRBY':
            v = int(self.data.get(a[0], b'0')) + int(a[1])
            self.data[a[0]] = str(v).encode()
            return v
        if cmd == b'TYPE':
            k = a[0]
            if self._alive(k):
                return b'string'
            if k in self.zsets:
                return b'zset'
            return b'none'
        if cmd == b'EXPIRE':
            k = a[0]
            seconds = int(a[1])
            if self._alive(k):
                self.expire_at[k] = time.monotonic() + seconds
                return 1
            return 0
        if cmd == b'PEXPIRE':
            k = a[0]
            ms = int(a[1])
            if self._alive(k):
                self._set_expire_ms(k, ms)
                return 1
            return 0
        if cmd == b'PTTL':
            k = a[0]
            if not self._alive(k):
                return -2
            at = self.expire_at.get(k)
            return -1 if at is None else max(0, int((at - time.monotonic()) * 1000))

        if cmd == b'ZADD':
            key = a[0]
            members = self.zsets.setdefault(key, {})
            changed = 0
            i = 1
            while i < len(a):
                score = float(a[i])
                member = a[i + 1]
                i += 2
                if member not in members:
                    changed += 1
                members[member] = score
            return changed
        if cmd == b'ZSCORE':
            return None if self.zsets.get(a[0], {}).get(a[1]) is None else repr(float(self.zsets[a[0]][a[1]])).encode()
        if cmd == b'ZREM':
            n = 0
            for member in a[1:]:
                if member in self.zsets.get(a[0], {}):
                    del self.zsets[a[0]][member]
                    n += 1
            return n
        if cmd == b'ZINCRBY':
            key, amount, member = a[0], float(a[1]), a[2]
            members = self.zsets.setdefault(key, {})
            members[member] = members.get(member, 0.0) + amount
            return repr(members[member]).encode()
        if cmd == b'ZRANGE':
            key = a[0]
            start, end = int(a[1]), int(a[2])
            members = self.zsets.get(key, {})
            ordered = sorted(members.items(), key=lambda kv: (kv[1], kv[0]))
            sliced = ordered[start : end + 1 if end >= 0 else (len(ordered) + end + 1)]
            withscores = b'WITHSCORES' in [x.upper() for x in a[3:]]
            flat: list[bytes] = []
            for member, score in sliced:
                flat.append(member)
                if withscores:
                    flat.append(repr(float(score)).encode())
            return flat
        if cmd in (b'KEYS',):
            return [k for k in list(self.data) if self._match(k, a[0])] if a else list(self.data)
        if cmd == b'FLUSHALL' or cmd == b'FLUSHDB':
            self.data.clear()
            self.expire_at.clear()
            self.zsets.clear()
            return b'OK'
        if cmd == b'ZRANGEBYSCORE':
            key = a[0]
            lo, hi = float(a[1] if a[1] != b'-inf' else '-inf'), None
            members = self.zsets.get(key, {})
            items = sorted(members.items(), key=lambda kv: (kv[1], kv[0]))
            low = float('-inf') if a[1] == b'-inf' else float(a[1])
            high = float('inf') if a[2] == b'+inf' else float(a[2])
            return [m for m, sc in items if low <= sc <= high]
        if cmd == b'ZPOPMIN':
            key = a[0]
            members = self.zsets.get(key, {})
            if not members:
                return []
            member, score = min(members.items(), key=lambda kv: (kv[1], kv[0]))
            del members[member]
            return [member, repr(float(score)).encode()]
        if cmd == b'ZCOUNT':
            key = a[0]
            low = float('-inf') if a[1] == b'-inf' else float(a[1])
            high = float('inf') if a[2] == b'+inf' else float(a[2])
            return sum(1 for sc in self.zsets.get(key, {}).values() if low <= sc <= high)
        if cmd == b'ZCARD':
            return len(self.zsets.get(a[0], {}))
        if cmd in (b'SCAN', b'PSCANGE'):
            return [b'0', []]
        if cmd in (b'EVAL', b'EVALSHA', b'EVAL_RO', b'EVALSHA_RO'):
            return 0
        if cmd == b'SCRIPT':
            return b'OK'
        if cmd == b'CLIENT' and a and a[0].upper() == b'GETNAME':
            return None

        if cmd == b'WATCH':
            return b'OK'
        if cmd == b'UNWATCH':
            return b'OK'
        if cmd == b'MULTI':
            conn.multi = True
            conn.queue = []
            return b'OK'
        if cmd == b'DISCARD':
            conn.multi = False
            conn.queue = []
            return b'OK'
        if cmd == b'EXEC':
            queued = conn.queue
            conn.multi = False
            conn.queue = []
            if self.before_state_check is not None and any(q[0].upper() == b'GET' for q in queued):
                get_key = next(q[1] for q in queued if q[0].upper() == b'GET')
                outcome = self.before_state_check(get_key)
                if outcome is not None and hasattr(outcome, '__await__'):
                    await outcome
            results = []
            for q in queued:
                results.append(await self.execute(conn, q))
            return results

        if cmd == b'SUBSCRIBE':
            conn.in_subscribe = True
            for channel in a:
                conn.channels.add(channel)
                self.subscribers.setdefault(channel, set()).add(conn)
            conn.pending.append(encode([b'subscribe', a[-1], len(conn.channels)]))
            return _NO_RESPONSE

        if cmd == b'UNSUBSCRIBE':
            channels = a or list(conn.channels)
            for channel in channels:
                conn.channels.discard(channel)
                waiters = self.subscribers.get(channel)
                if waiters is not None:
                    waiters.discard(conn)
                    if not waiters:
                        del self.subscribers[channel]
            conn.pending.append(encode([b'unsubscribe', channels[-1], len(conn.channels)]))
            if not conn.channels:
                conn.in_subscribe = False
            return _NO_RESPONSE
        if cmd == b'PUBLISH':
            self.publish_count += 1
            channel, message = a[0], a[1]
            n = 0
            for sub in list(self.subscribers.get(channel, ())):
                if not sub.closing:
                    sub.feed([b'message', channel, message])
                    n += 1
            return n

        return Exception(f'ERR unknown command `{cmd.decode()}`')

    @staticmethod
    def _match(key: bytes, pattern: bytes) -> bool:
        import fnmatch

        return fnmatch.fnmatchcase(key.decode(errors='replace'), pattern.decode(errors='replace'))


class FakeConnection:
    def __init__(self, server: FakeRedisServer) -> None:
        self.server = server
        self.channels: set[bytes] = set()
        self.in_subscribe = False
        self.multi = False
        self.queue: list[list[bytes]] = []
        self.closing = False
        self.pending: list[bytes] = []
        self.transports: list[Any] = []
        self.server_writer: Any = None
        self.serve_task: Optional[asyncio.Task[None]] = None
        self.on_server_closed: Any = None
        self.after_closed_task: Any = None
        # downstream (client-facing) pipe: server writes here, client reads
        self.c2s_reader: Optional[asyncio.StreamReader] = None
        self.s2c_writer: Optional[asyncio.StreamWriter] = None

    def feed(self, payload: list[bytes]) -> None:
        assert self.s2c_writer is not None
        self.s2c_writer.write(encode(payload))

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.c2s_reader = reader
        self.server_writer = writer
        self.server.connections.add(self)
        import os as _os
        self._dbg_id = _os.getpid()
        try:
            while not self.closing:
                try:
                    args = await read_command(reader)
                except (asyncio.IncompleteReadError, ConnectionError):
                    break
                if args is None:
                    break
                if args and args[0] == b'#emulator:drop':
                    break
                if self.multi and args[0].upper() not in (b'EXEC', b'DISCARD'):
                    self.queue.append(args)
                    writer.write(b'+QUEUED' + CRLF)
                    await writer.drain()
                    continue
                try:
                    hook = self.server.before_command
                    if hook is not None:
                        await hook(args)
                    async with self.server.lock:
                        result = await self.server.execute(self, args)
                except Exception as e:  # server-side failure
                    result = e
                # frames pushed by the command (e.g. SUBSCRIBE confirmations)
                # are written first and occupy the reply slot
                for frame in self.pending:
                    writer.write(frame)
                self.pending.clear()
                if result is not _NO_RESPONSE:
                    writer.write(encode(result))
                # hook fires after the SUBSCRIBE confirmation is flushed, i.e.
                # redis-py considers the subscription up but its caller has not
                # resumed yet; a publish in here hits the real "completion just
                # after subscribe" window
                hook_cb = self.server.during_subscribe
                is_subscribe = bool(args) and args[0].upper() == b'SUBSCRIBE'
                await writer.drain()
                if hook_cb is not None and is_subscribe:
                    await asyncio.sleep(0)
                    await asyncio.sleep(0)
                    outcome = hook_cb(args)
                    if outcome is not None and hasattr(outcome, '__await__'):
                        await outcome
                if self.closing:
                    break
        finally:
            self.server.connections.discard(self)
            for waiters in self.server.subscribers.values():
                waiters.discard(self)
            for channel in list(self.channels):
                bucket = self.server.subscribers.get(channel)
                if bucket is not None and not bucket:
                    del self.server.subscribers[channel]
            try:
                writer.close()
            except Exception:
                pass
            for transport in self.transports:
                try:
                    transport.close()
                except Exception:
                    pass
            self.fire_closed()

    def drop(self) -> None:
        """Sever this connection immediately, like a network outage."""
        self.closing = True
        self.fire_closed()

    def fire_closed(self) -> None:
        if self.on_server_closed is not None:
            try:
                out = self.on_server_closed()
                if out is not None and hasattr(out, '__await__'):
                    asyncio.get_running_loop().create_task(out)
            except Exception:
                pass
        # close the server->client pipe from the server task: the blocked
        # client reader sees EOF; never touch the client transports here (that
        # re-enters asyncio transport machinery from the wrong task)
        if self.server_writer is not None:
            try:
                self.server_writer.close()
            except Exception:
                pass


def _pipe_streams() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    r_fd, w_fd = os.pipe()
    loop = asyncio.get_running_loop()
    rf = os.fdopen(r_fd, 'rb', buffering=0)
    wf = os.fdopen(w_fd, 'wb', buffering=0)
    reader = asyncio.StreamReader(limit=10 * 1024 * 1024)
    read_protocol = asyncio.StreamReaderProtocol(reader)
    _UnixReadPipeTransport(loop, rf, read_protocol)
    write_protocol = asyncio.StreamReaderProtocol(asyncio.StreamReader())
    write_transport = _UnixWritePipeTransport(loop, wf, write_protocol)
    writer = asyncio.StreamWriter(write_transport, read_protocol, reader, loop)
    return reader, writer


class InProcessConnection(AbstractConnection):
    """redis-py connection whose transport is two pipe pairs inside this process."""

    fake_server: Optional[FakeRedisServer] = None

    def __init__(self, *args: Any, fake_server: Optional[FakeRedisServer] = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.fake_conn: Optional[FakeConnection] = None
        self._pipe_transports: list[Any] = []
        if fake_server is not None:
            type(self).fake_server = fake_server

    def _close_pipe_transports(self) -> None:
        for transport in self._pipe_transports:
            try:
                transport.close()
            except Exception:
                pass

    async def disconnect(self, nowait: bool = False) -> None:  # type: ignore[override]
        # pipes may already be gone (server-side drop); mirror redis-py's
        # disconnect without ever awaiting writer.wait_closed(), which can
        # block forever on a severed pipe
        try:
            self._parser.on_disconnect()
        except Exception:
            pass
        if not self.is_connected:
            self._close_pipe_transports()
            return
        writer = self._writer
        try:
            if writer is not None:
                writer.close()
        except OSError:
            pass
        finally:
            self._reader = None
            self._writer = None
            self._close_pipe_transports()

    def mark_server_gone(self) -> None:
        """Client-side marker that this connection's server has vanished."""
        self._gone = True

    async def _send_packed_command(self, command: Any) -> None:  # type: ignore[override]
        if getattr(self, '_gone', False):
            # a write after the outage: behave like a dead socket so redis-py
            # rebuilds this connection
            raise ConnectionError('Connection closed by server (emulated outage)')
        await super()._send_packed_command(command)

    async def _connect(self) -> None:
        assert type(self).fake_server is not None
        self._gone = False
        old_fake = self.fake_conn
        # pair A: client -> server
        c2s_reader, c2s_writer = _pipe_streams()
        # pair B: server -> client
        s2c_reader, s2c_writer = _pipe_streams()

        self.fake_conn = FakeConnection(type(self).fake_server)
        self.fake_conn.s2c_writer = s2c_writer
        pair = (c2s_writer.transport, s2c_writer.transport)
        self._pipe_transports = list(pair)
        self.fake_conn.transports.extend(pair)
        self.fake_conn.serve_task = asyncio.create_task(self.fake_conn.serve(c2s_reader, s2c_writer))

        self._reader = s2c_reader
        self._writer = c2s_writer

        # the reconnect callback must refer to THIS connection's fresh state;
        # drop the old (dead) FakeConnection's subscription bookkeeping
        if old_fake is not None and old_fake is not self.fake_conn:
            type(self).fake_server.connections.discard(old_fake)
            for bucket in type(self).fake_server.subscribers.values():
                bucket.discard(old_fake)

        client_conn = self

        async def _mark_dead() -> None:
            # emulate redis vanishing mid-connection. Both pipe ends are
            # closed server-side; mark the client half dead so writes fail and
            # redis-py rebuilds fresh pipes on the next command.
            client_conn.mark_server_gone()

        self.fake_conn.on_server_closed = _mark_dead

    def repr_pieces(self):  # type: ignore[override]
        return [('server', 'in-process-fake-redis')]

    def _host_error(self) -> str:
        return 'in-process-fake-redis'


async def make_fake_redis() -> tuple[FakeRedisServer, Any]:
    """Return (server, arq_redis_client) wired through pipes."""
    from arq.connections import ArqRedis

    server = FakeRedisServer()
    InProcessConnection.fake_server = server
    from redis.asyncio.connection import ConnectionPool

    pool = ConnectionPool(connection_class=InProcessConnection, max_connections=10)
    client = ArqRedis(
        connection_pool=pool,
        encoding='utf8',
        auto_close_connection_pool=True,
    )
    await client.ping()
    return server, client


def drop_connection(server: FakeRedisServer, *, subscriber: bool) -> None:
    """
    Drop a connection: subscriber=True targets a connection in pub/sub mode.
    """
    targets = [c for c in server.connections if c.in_subscribe] if subscriber else list(server.connections)
    if not targets:
        raise RuntimeError('no matching connection to drop')
    targets[0].drop()
