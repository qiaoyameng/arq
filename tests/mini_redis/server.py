"""Minimal RESP2 redis server used for offline abort/race tests.

Only implements the subset of commands used by arq and the abort tests:
strings (GET/SET/SETEX/PSETEX/INCR/EXISTS/DEL/EXPIRE/PEXPIRE/PTTL),
hashes (HSETNX/HMGET), sorted sets (ZADD/ZSCORE/ZRANGE/ZRANGEBYSCORE/
ZREMRANGEBYSCORE/ZINCRBY/ZREM/ZCARD), INFO, DBSIZE, PING, SELECT, FLUSHALL,
CLIENT and HELLO. Transactions (MULTI/EXEC/WATCH/DISCARD/RESET) are supported
with optimistic-concurrency semantics good enough for arq's pipelines.
"""

import asyncio
import time
from typing import Any, Optional


class Entry:
    __slots__ = ('expire_at', 'kind', 'value')

    def __init__(self, kind: str, value: Any, expire_at: Optional[float] = None):
        self.kind = kind
        self.value = value
        self.expire_at = expire_at


def _ms() -> int:
    return int(time.time() * 1000)


class MiniRedis:
    def __init__(self, host: str = '127.0.0.1', port: int = 6390):
        self.host = host
        self.port = port
        self.data: dict[bytes, Entry] = {}
        # bumped each time a key is created/modified/deleted, used by WATCH
        self.versions: dict[bytes, int] = {}
        self._server: Optional[asyncio.AbstractServer] = None
        self._gc_task: Optional[asyncio.Task[None]] = None
        # gate: command name (upper) -> asyncio.Event, set when the command runs
        self.gates: dict[str, asyncio.Event] = {}
        # commands that must wait for ``release(name)`` before executing
        self.blocked: dict[str, asyncio.Event] = {}
        self._cond = asyncio.Condition()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self._gc_task = asyncio.create_task(self._gc())

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        if self._gc_task:
            self._gc_task.cancel()

    async def wait_for_command(self, name: str) -> None:
        """Wait until a command with this name has been executed at least once."""
        key = name.upper()
        event = self.gates.setdefault(key, asyncio.Event())
        block = self.blocked.get(key)
        if block is not None and block.is_set():
            return
        await event.wait()

    def reset_gate(self, name: str) -> None:
        event = self.gates.get(name.upper())
        if event:
            event.clear()

    def block_command(self, name: str) -> None:
        """Hold execution of ``name`` until :meth:`release` is called."""
        key = name.upper()
        event = self.blocked.get(key)
        if event is None:
            event = asyncio.Event()
            self.blocked[key] = event
        event.clear()

    def release(self, name: str) -> None:
        event = self.blocked.get(name.upper())
        if event is not None:
            event.set()

    async def _maybe_blocked(self, name: str) -> None:
        event = self.blocked.get(name)
        if event is not None:
            await event.wait()

    async def _gc(self) -> None:
        while True:
            await asyncio.sleep(0.02)
            now = time.time()
            dead = [k for k, e in self.data.items() if e.expire_at is not None and e.expire_at <= now]
            for k in dead:
                self.data.pop(k, None)

    def _alive(self, key: bytes) -> bool:
        entry = self.data.get(key)
        if entry is None:
            return False
        if entry.expire_at is not None and entry.expire_at <= time.time():
            self.data.pop(key, None)
            return False
        return True

    # -- connection handling --------------------------------------------------

    @staticmethod
    def _ok(text: str = 'OK') -> bytes:
        return f'+{text}\r\n'.encode()

    @staticmethod
    def _error(text: str) -> bytes:
        return f'-{text}\r\n'.encode()

    @staticmethod
    def _int(n: int) -> bytes:
        return f':{n}\r\n'.encode()

    @staticmethod
    def _bulk(v: Optional[bytes]) -> bytes:
        if v is None:
            return b'$-1\r\n'
        return b'$' + str(len(v)).encode() + b'\r\n' + v + b'\r\n'

    @classmethod
    def _array(cls, items: list[Optional[bytes]]) -> bytes:
        out = b'*' + str(len(items)).encode() + b'\r\n'
        for item in items:
            out += cls._bulk(item)
        return out

    async def _exec(self, args: list[bytes]) -> bytes:
        if not args:
            return self._error('ERR empty command')
        name = args[0].decode().upper()
        try:
            fn = getattr(self, f'cmd_{name.lower()}')
        except AttributeError:
            return self._error(f"ERR unknown command '{name}'")
        reply = fn(*args[1:])
        if asyncio.iscoroutine(reply):
            reply = await reply
        event = self.gates.get(name)
        if event is not None:
            event.set()
        return reply

    # -- helpers --------------------------------------------------------------

    def _get(self, key: bytes) -> Optional[Entry]:
        return self.data.get(key) if self._alive(key) else None

    @staticmethod
    def _ms_arg(v: bytes) -> int:
        return int(v)

    # -- strings --------------------------------------------------------------

    def cmd_ping(self, *a: bytes) -> bytes:
        return self._bulk(a[0]) if a else self._ok('PONG')

    def cmd_select(self, *a: bytes) -> bytes:
        return self._ok()

    def cmd_flushall(self, *a: bytes) -> bytes:
        self.data.clear()
        self.versions.clear()
        return self._ok()

    def cmd_info(self, *a: bytes) -> bytes:
        return self._bulk(b'')

    def cmd_client(self, *a: bytes) -> bytes:
        return self._ok()

    def cmd_hello(self, *a: bytes) -> bytes:
        return self._array([b'proto', b'2'])

    def cmd_dbsize(self) -> bytes:
        return self._int(sum(1 for k in self.data if self._alive(k)))

    def cmd_keys(self, pattern: bytes) -> bytes:
        # only the 'prefix*' glob shape used by arq is needed, plus exact keys
        if pattern == b'*':
            keys = [k for k in self.data if self._alive(k)]
        elif pattern.endswith(b'*'):
            prefix = pattern[:-1]
            keys = [k for k in self.data if self._alive(k) and k.startswith(prefix)]
        else:
            keys = [k for k in self.data if self._alive(k) and k == pattern]
        return self._array(sorted(keys))

    def cmd_get(self, key: bytes) -> bytes:
        entry = self._get(key)
        if entry is None or entry.kind != 'str':
            return self._bulk(None)
        return self._bulk(entry.value)

    def _apply_set_options(self, args: tuple[bytes, ...]) -> tuple[Optional[int], bool, bool]:
        """Parse [EX s|PX ms|KEEPTTL] [NX|XX] after SET key value. Returns (px_ms, nx, xx)."""
        px: Optional[int] = None
        nx = xx = False
        i = 0
        while i < len(args):
            opt = args[i].decode().upper()
            if opt == 'PX':
                px = int(args[i + 1])
                i += 2
            elif opt == 'EX':
                px = int(args[i + 1]) * 1000
                i += 2
            elif opt == 'KEEPTTL':
                i += 1
            elif opt == 'NX':
                nx = True
                i += 1
            elif opt == 'XX':
                xx = True
                i += 1
            else:
                i += 1
        return px, nx, xx

    def cmd_set(self, key: bytes, value: bytes, *rest: bytes) -> bytes:
        px, nx, xx = self._apply_set_options(rest)
        exists = self._get(key) is not None
        if nx and exists:
            return self._bulk(None)
        if xx and not exists:
            return self._bulk(None)
        expire_at = None
        if px is not None:
            expire_at = time.time() + px / 1000
        self.data[key] = Entry('str', value, expire_at)
        self._touch(key)
        return self._ok()

    def cmd_setex(self, key: bytes, seconds: bytes, value: bytes) -> bytes:
        self.data[key] = Entry('str', value, time.time() + int(seconds))
        self._touch(key)
        return self._ok()

    def cmd_psetex(self, key: bytes, milliseconds: bytes, value: bytes) -> bytes:
        self.data[key] = Entry('str', value, time.time() + int(milliseconds) / 1000)
        self._touch(key)
        return self._ok()

    def cmd_incr(self, key: bytes) -> bytes:
        return self._incrby(key, b'1')

    def cmd_incrby(self, key: bytes, amount: bytes) -> bytes:
        return self._incrby(key, amount)

    def _incrby(self, key: bytes, amount_b: bytes) -> bytes:
        entry = self._get(key)
        if entry is None:
            entry = Entry('str', b'0')
            self.data[key] = entry
        try:
            value = int(entry.value) + int(amount_b)
        except ValueError:
            return self._error('ERR value is not an integer')
        entry.value = str(value).encode()
        self._touch(key)
        return self._int(value)

    def cmd_exists(self, *keys: bytes) -> bytes:
        return self._int(sum(1 for k in keys if self._get(k) is not None))

    def cmd_del(self, *keys: bytes) -> bytes:
        n = 0
        for k in keys:
            if self.data.pop(k, None) is not None:
                n += 1
                self._touch(k)
        return self._int(n)

    def cmd_expire(self, key: bytes, seconds: bytes, *rest: bytes) -> bytes:
        return self._pexpire(key, int(seconds) * 1000)

    def cmd_pexpire(self, key: bytes, milliseconds: bytes, *rest: bytes) -> bytes:
        return self._pexpire(key, int(milliseconds))

    def _pexpire(self, key: bytes, ms: int) -> int:
        entry = self._get(key)
        if entry is None:
            return self._int(0)
        entry.expire_at = time.time() + ms / 1000
        self._touch(key)
        return self._int(1)

    def cmd_pttl(self, key: bytes) -> bytes:
        entry = self._get(key)
        if entry is None:
            return self._int(-2)
        if entry.expire_at is None:
            return self._int(-1)
        return self._int(max(0, int((entry.expire_at - time.time()) * 1000)))

    def cmd_ttl(self, key: bytes) -> bytes:
        entry = self._get(key)
        if entry is None:
            return self._int(-2)
        if entry.expire_at is None:
            return self._int(-1)
        return self._int(max(0, (entry.expire_at - time.time())))

    # -- hashes ---------------------------------------------------------------

    def cmd_hsetnx(self, key: bytes, field: bytes, value: bytes) -> bytes:
        entry = self._get(key)
        if entry is None:
            entry = Entry('hash', {})
            self.data[key] = entry
        if field in entry.value:
            return self._int(0)
        entry.value[field] = value
        self._touch(key)
        return self._int(1)

    def cmd_hget(self, key: bytes, field: bytes) -> bytes:
        entry = self._get(key)
        if entry is None or entry.kind != 'hash':
            return self._bulk(None)
        return self._bulk(entry.value.get(field))

    def cmd_hmget(self, key: bytes, *fields: bytes) -> bytes:
        entry = self._get(key)
        values: list[Optional[bytes]] = []
        for field in fields:
            if entry is not None and entry.kind == 'hash':
                values.append(entry.value.get(field))
            else:
                values.append(None)
        return self._array(values)

    def cmd_hgetall(self, key: bytes) -> bytes:
        entry = self._get(key)
        if entry is None or entry.kind != 'hash':
            return self._array([])
        out: list[Optional[bytes]] = []
        for field, value in entry.value.items():
            out.append(field)
            out.append(value)
        return self._array(out)

    # -- sorted sets ----------------------------------------------------------

    def _zset(self, key: bytes) -> dict[bytes, float]:
        entry = self._get(key)
        if entry is None:
            entry = Entry('zset', {})
            self.data[key] = entry
        return entry.value

    def cmd_zadd(self, key: bytes, *rest: bytes) -> bytes:
        # rest: [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
        i = 0
        while i < len(rest) and rest[i][0:1].isalpha():
            i += 1
        pairs = rest[i:]
        zset = self._zset(key)
        added = 0
        for j in range(0, len(pairs), 2):
            score = float(pairs[j])
            member = pairs[j + 1]
            if member not in zset:
                added += 1
            zset[member] = score
        self._touch(key)
        return self._int(added)

    def cmd_zscore(self, key: bytes, member: bytes) -> bytes:
        zset = self._get(key)
        if zset is None or member not in zset.value:
            return self._bulk(None)
        score = zset.value[member]
        text = str(int(score)) if score == int(score) else repr(score)
        return self._bulk(text.encode())

    def cmd_zcard(self, key: bytes) -> bytes:
        zset = self._get(key)
        return self._int(0 if zset is None else len(zset.value))

    def cmd_zrem(self, key: bytes, *members: bytes) -> bytes:
        zset = self._get(key)
        if zset is None:
            return self._int(0)
        n = 0
        for m in members:
            if zset.value.pop(m, None) is not None:
                n += 1
        if not zset.value:
            self.data.pop(key, None)
        self._touch(key)
        return self._int(n)

    def cmd_zincrby(self, key: bytes, amount: bytes, member: bytes) -> bytes:
        zset = self._zset(key)
        score = zset.get(member, 0.0) + float(amount)
        zset[member] = score
        self._touch(key)
        text = str(int(score)) if score == int(score) else repr(score)
        return self._bulk(text.encode())

    def cmd_zremrangebyscore(self, key: bytes, minv: bytes, maxv: bytes) -> bytes:
        zset = self._get(key)
        if zset is None:
            return self._int(0)
        lo = self._parse_score(minv)
        hi = self._parse_score(maxv)
        victims = [m for m, s in zset.value.items() if lo <= s <= hi]
        for m in victims:
            del zset.value[m]
        if not zset.value:
            self.data.pop(key, None)
        self._touch(key)
        return self._int(len(victims))

    @staticmethod
    def _parse_score(v: bytes) -> float:
        if v == b'-inf':
            return float('-inf')
        if v == b'+inf' or v == b'inf':
            return float('inf')
        return float(v)

    def cmd_zrange(self, key: bytes, start: bytes, end: bytes, *rest: bytes) -> bytes:
        zset = self._get(key)
        ordered = sorted(({}.items() if zset is None else zset.value.items()), key=lambda kv: (kv[1], kv[0]))
        s, e = int(start), int(end)
        if e == -1:
            sliced = ordered[s:]
        else:
            sliced = ordered[s : e + 1]
        if b'REV' in [r.upper() for r in rest]:
            sliced = list(reversed(sliced))
        if b'WITHSCORES' in [r.upper() for r in rest]:
            out = []
            for m, sc in sliced:
                out.append(m)
                out.append((str(int(sc)) if sc == int(sc) else repr(sc)).encode())
            return self._array(out)
        return self._array([m for m, _ in sliced])

    def cmd_zrangebyscore(self, key: bytes, minv: bytes, maxv: bytes, *rest: bytes) -> bytes:
        zset = self._get(key)
        lo = self._parse_score(minv)
        hi = self._parse_score(maxv)
        rest_u = [r.upper() for r in rest]
        start = 0
        num = None
        if b'LIMIT' in rest_u:
            i = rest_u.index(b'LIMIT')
            start = int(rest[i + 1])
            num = int(rest[i + 2])
        members = [m for m, s in ({} if zset is None else zset.value.items()) if lo <= s <= hi]
        members.sort(key=lambda m: (zset.value[m], m))
        if num is not None:
            members = members[start : start + num] if num >= 0 else members[start:]
        else:
            members = members[start:]
        return self._array(members)

    # -- transactions ---------------------------------------------------------

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:  # noqa: C901
        buf = bytearray()
        in_multi = False
        queued: list[list[bytes]] = []
        watched: dict[bytes, int] = {}
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                buf += line
                if line[0:1] != b'*':
                    continue
                n = int(line[1:])
                args = []
                for _ in range(n):
                    hdr = await reader.readline()
                    if not hdr:
                        return
                    buf += hdr
                    length = int(hdr[1:])
                    payload = await reader.readexactly(length + 2)
                    args.append(payload[:-2])

                name = args[0].decode().upper()

                if name == 'WATCH':
                    for k in args[1:]:
                        watched[k] = self._data_version(k)
                    writer.write(self._ok())
                    await writer.drain()
                    continue
                if name == 'UNWATCH':
                    watched.clear()
                    writer.write(self._ok())
                    await writer.drain()
                    continue
                if name == 'MULTI':
                    in_multi = True
                    queued = []
                    writer.write(self._ok())
                    await writer.drain()
                    continue
                if name == 'DISCARD':
                    in_multi = False
                    queued = []
                    writer.write(self._ok())
                    await writer.drain()
                    continue
                if name == 'RESET':
                    in_multi = False
                    queued = []
                    watched.clear()
                    writer.write(self._ok())
                    await writer.drain()
                    continue
                if name == 'EXEC':
                    if not in_multi:
                        writer.write(self._error('ERR EXEC without MULTI'))
                    elif any(self._data_version(k) != ver for k, ver in watched.items()):
                        writer.write(self._null())
                    else:
                        replies = []
                        for qargs in queued:
                            try:
                                reply = await self._run(qargs)
                            except Exception as exc:
                                reply = self._error(f'ERR {exc}')
                            replies.append(reply)
                        writer.write(self._raw_array(replies))
                    in_multi = False
                    queued = []
                    watched.clear()
                    await writer.drain()
                    continue

                if in_multi:
                    queued.append(args)
                    writer.write(self._queued())
                    await writer.drain()
                    continue

                try:
                    reply = await self._run(args)
                except AttributeError:
                    reply = self._error(f"ERR unknown command '{name}'")
                except Exception as exc:  # pragma: no cover - defensive
                    reply = self._error(f'ERR {exc}')
                writer.write(reply)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            writer.close()

    def _data_version(self, key: bytes) -> int:
        return self.versions.get(key, 0)

    def _touch(self, *keys: bytes) -> None:
        for key in keys:
            self.versions[key] = self.versions.get(key, 0) + 1

    async def _run(self, args: list[bytes]) -> bytes:
        name = args[0].decode().upper()
        event = self.gates.get(name)
        if event is not None:
            event.set()
        await self._maybe_blocked(name)
        fn = getattr(self, f'cmd_{name.lower()}')
        reply = fn(*args[1:])
        if asyncio.iscoroutine(reply):
            reply = await reply
        return reply

    @staticmethod
    def _null() -> bytes:
        return b'*-1\r\n'

    @staticmethod
    def _queued() -> bytes:
        return b'+QUEUED\r\n'

    @staticmethod
    def _raw_array(replies: list[bytes]) -> bytes:
        return b'*' + str(len(replies)).encode() + b'\r\n' + b''.join(replies)
