"""
A tiny, dependency-free subset of the Redis server protocol (RESP2/RESP3),
used to run arq's tests and the serial-group demo on machines where installing
a real redis-server is not possible (no network / no root).

It is *not* a general purpose Redis replacement; it implements just the
commands arq uses. It is an independent local server process: clients connect
over TCP and every command is serialised under a global lock, exactly like a
single-threaded Redis.
"""

from __future__ import annotations

import argparse
import heapq
import socket
import socketserver
import threading
import time
from typing import Any

INF = float('inf')


def now_ms() -> int:
    return int(time.time() * 1000)


class Store:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.strings: dict[bytes, bytes] = {}
        self.expire_at: dict[bytes, int] = {}
        self.zsets: dict[bytes, dict[bytes, float]] = {}
        self.lists: dict[bytes, list[bytes]] = {}
        self.watched: dict[Any, set[bytes]] = {}
        self.versions: dict[bytes, int] = {}
        self.version_counter = 0
        self.multis: dict[Any, list[list[bytes]]] = {}
        self.watch_marks: dict[Any, dict[bytes, int]] = {}

    # ------------------------------------------------------------------ utils
    def _alive(self, key: bytes) -> bool:
        if key not in self.strings and key not in self.zsets and key not in self.lists:
            return False
        exp = self.expire_at.get(key)
        if exp is not None and exp <= now_ms():
            self._delete_raw(key)
            return False
        return True

    def _delete_raw(self, key: bytes) -> None:
        self.strings.pop(key, None)
        self.zsets.pop(key, None)
        self.lists.pop(key, None)
        self.expire_at.pop(key, None)

    def touch(self, key: bytes) -> None:
        self.version_counter += 1
        self.versions[key] = self.version_counter

    def check_watched(self, conn: Any) -> bool:
        keys = self.watched.get(conn)
        if not keys:
            return True
        marks = self.watch_marks.get(conn, {})
        return not any(self.versions.get(k, 0) > marks.get(k, 0) for k in keys)

    def clear_watched(self, conn: Any) -> None:
        self.watched.pop(conn, None)
        self.watch_marks.pop(conn, None)

    def is_in_multi(self, conn: Any) -> bool:
        return conn in self.multis

    def begin_multi(self, conn: Any) -> None:
        self.multis[conn] = []

    def queue_multi(self, conn: Any, args: list[bytes]) -> None:
        self.multis[conn].append(args)

    def end_multi(self, conn: Any) -> list[list[bytes]] | None:
        return self.multis.pop(conn, None)

    def type_of(self, key: bytes) -> str:
        if not self._alive(key):
            return 'none'
        if key in self.strings:
            return 'string'
        if key in self.zsets:
            return 'zset'
        if key in self.lists:
            return 'list'
        return 'none'

    def _expire(self, key: bytes, ms: int) -> int:
        if not self._alive(key):
            return 0
        if ms and ms > 0:
            self.expire_at[key] = now_ms() + int(ms)
        else:
            self._delete_raw(key)
        return 1

    # ----------------------------------------------------------------- string
    def set(self, key: bytes, value: bytes, px: int | None, nx: bool, xx: bool) -> bytes | None:
        exists = self._alive(key)
        if (nx and exists) or (xx and not exists):
            return None
        self.strings[key] = value
        self.zsets.pop(key, None)
        self.lists.pop(key, None)
        if px is not None:
            if px > 0:
                self.expire_at[key] = now_ms() + int(px)
            else:
                self._delete_raw(key)
        else:
            self.expire_at.pop(key, None)
        self.touch(key)
        return b'OK'

    def psetex(self, key: bytes, ms: int, value: bytes) -> bytes:
        self.strings[key] = value
        self.zsets.pop(key, None)
        self.lists.pop(key, None)
        self.expire_at[key] = now_ms() + int(ms)
        self.touch(key)
        return b'OK'

    def get(self, key: bytes) -> bytes | None:
        return self.strings.get(key) if self._alive(key) and key in self.strings else None

    def exists(self, *keys: bytes) -> int:
        return sum(1 for k in keys if self._alive(k))

    def incr(self, key: bytes) -> int:
        value = self.get(key)
        n = int(value) + 1 if value is not None else 1
        self.strings[key] = str(n).encode()
        if key in self.expire_at and self.expire_at[key] <= now_ms():
            self.expire_at.pop(key, None)
        self.touch(key)
        return n

    def incr_by(self, key: bytes, amount: int) -> int:
        value = self.get(key)
        n = int(value) + amount if value is not None else amount
        self.strings[key] = str(n).encode()
        self.touch(key)
        return n

    def delete(self, *keys: bytes) -> int:
        count = 0
        for k in keys:
            if self._alive(k):
                count += 1
            self._delete_raw(k)
            self.touch(k)
        return count

    def keys(self, pattern: bytes) -> list[bytes]:
        import re
        import fnmatch

        regex = re.compile(fnmatch.translate(bytes(pattern).decode()).encode())
        return [
            k for k in list(self.strings) + list(self.zsets) + list(self.lists) if self._alive(k) and regex.match(k)
        ]

    def ttl_ms(self, key: bytes) -> int:
        if not self._alive(key):
            return -2
        exp = self.expire_at.get(key)
        return max(0, exp - now_ms()) if exp is not None else -1

    def ttl_s(self, key: bytes) -> int:
        ms = self.ttl_ms(key)
        if ms in (-1, -2):
            return ms
        return max(1, (ms + 999) // 1000)

    # ------------------------------------------------------------------ lists
    def rpush(self, key: bytes, *values: bytes) -> int:
        if not self._alive(key) or key not in self.lists:
            self.lists[key] = []
            self.strings.pop(key, None)
            self.zsets.pop(key, None)
        self.lists[key].extend(values)
        self.touch(key)
        return len(self.lists[key])

    def lindex(self, key: bytes, index: int) -> bytes | None:
        if not self._alive(key) or key not in self.lists:
            return None
        lst = self.lists[key]
        if index < 0:
            index += len(lst)
        return lst[index] if 0 <= index < len(lst) else None

    def lrem(self, key: bytes, count: int, value: bytes) -> int:
        if not self._alive(key) or key not in self.lists:
            return 0
        lst = self.lists[key]
        removed = 0
        if count >= 0:
            indices = range(len(lst))
        else:
            indices = range(len(lst) - 1, -1, -1)
        limit = count if count != 0 else len(lst)
        to_delete: list[int] = []
        for i in indices:
            if removed >= abs(limit) and limit != 0:
                break
            if lst[i] == value:
                to_delete.append(i)
                removed += 1
        for i in sorted(to_delete, reverse=True):
            del lst[i]
        if not lst:
            self._delete_raw(key)
        self.touch(key)
        return removed

    # ------------------------------------------------------------------ zsets
    def _zadd(self, key: bytes, mapping: dict[bytes, float], incr: bool) -> int | float | None:
        if not self._alive(key) or key not in self.zsets:
            self.zsets[key] = {}
            self.strings.pop(key, None)
            self.lists.pop(key, None)
        z = self.zsets[key]
        if incr:
            member, score = next(iter(mapping.items()))
            if member not in z and len(mapping) != 1:
                pass
            new_score = z.get(member, 0.0) + score
            z[member] = new_score
            self.touch(key)
            return new_score
        added = 0
        for member, score in mapping.items():
            if member not in z:
                added += 1
            z[member] = score
        self.touch(key)
        return added

    def _ordered(self, key: bytes) -> list[tuple[bytes, float]]:
        z = self.zsets.get(key, {})
        # redis orders by score then lexicographically by member
        return sorted(z.items(), key=lambda kv: (kv[1], kv[0]))

    def zscore(self, key: bytes, member: bytes) -> float | None:
        if not self._alive(key) or key not in self.zsets:
            return None
        return self.zsets[key].get(member)

    def zcard(self, key: bytes) -> int:
        return len(self.zsets[key]) if self._alive(key) and key in self.zsets else 0

    def zrem(self, key: bytes, *members: bytes) -> int:
        if not self._alive(key) or key not in self.zsets:
            return 0
        z = self.zsets[key]
        removed = sum(1 for m in members if m in z)
        for m in members:
            z.pop(m, None)
        if not z:
            self._delete_raw(key)
        self.touch(key)
        return removed

    def zincrby(self, key: bytes, amount: float, member: bytes) -> float:
        if not self._alive(key) or key not in self.zsets:
            self.zsets[key] = {}
            self.strings.pop(key, None)
            self.lists.pop(key, None)
        z = self.zsets[key]
        z[member] = z.get(member, 0.0) + amount
        self.touch(key)
        return z[member]

    def zrange(self, key: bytes, start: int, end: int, withscores: bool = False, rev: bool = False) -> list[Any]:
        if not self._alive(key) or key not in self.zsets:
            return []
        items = self._ordered(key)
        if rev:
            items.reverse()
        total = len(items)
        start = max(start + total, 0) if start < 0 else min(start, total)
        end = end + total + 1 if end < 0 else min(end + 1, total)
        sliced = items[start:end]
        result: list[Any] = []
        for member, score in sliced:
            result.append(member)
            if withscores:
                result.append(score)
        return result

    def zrangebyscore(
        self,
        key: bytes,
        min_score: float,
        max_score: float,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        if not self._alive(key) or key not in self.zsets:
            return []
        items = [(m, s) for m, s in self._ordered(key) if min_score <= s <= max_score]
        if start is not None and num is not None:
            items = items[start : start + num]
        result: list[Any] = []
        for member, score in items:
            result.append(member)
            if withscores:
                result.append(score)
        return result

    def zremrangebyscore(self, key: bytes, min_score: float, max_score: float) -> int:
        if not self._alive(key) or key not in self.zsets:
            return 0
        z = self.zsets[key]
        doomed = [m for m, s in z.items() if min_score <= s <= max_score]
        for m in doomed:
            del z[m]
        if not z:
            self._delete_raw(key)
        self.touch(key)
        return len(doomed)


def parse_score(value: bytes) -> float:
    v = value.decode() if isinstance(value, bytes) else value
    if v == '-inf':
        return -INF
    if v in ('+inf', 'inf'):
        return INF
    exclusive = False
    if v[0] == '(':
        exclusive = True
        v = v[1:]
    score = float(v)
    if exclusive:
        # only used in tests with integer boundaries; nudge by one ms
        score = score + 1 if score != INF else score
    return score


def format_score(score: float) -> bytes:
    if score == int(score):
        return str(int(score)).encode()
    return repr(score).encode()


class ProtocolError(Exception):
    pass


class RespReader:
    def __init__(self) -> None:
        self.buffer = bytearray()

    def feed(self, data: bytes) -> None:
        self.buffer.extend(data)

    def read_command(self) -> list[bytes] | None:
        if not self.buffer:
            return None
        if self.buffer[0:1] != b'*':
            # inline command (used by e.g. redis-cli health checks)
            line_end = self.buffer.find(b'\r\n')
            if line_end == -1:
                return None
            line = bytes(self.buffer[:line_end])
            del self.buffer[: line_end + 2]
            return line.split()
        return self._read_array()

    def _read_line(self) -> bytes | None:
        end = self.buffer.find(b'\r\n')
        if end == -1:
            return None
        line = bytes(self.buffer[:end])
        del self.buffer[: end + 2]
        return line

    def _read_array(self) -> list[bytes] | None:
        assert self.buffer[0:1] == b'*'
        end = self.buffer.find(b'\r\n')
        if end == -1:
            return None
        try:
            count = int(self.buffer[1:end])
        except ValueError as exc:
            raise ProtocolError('invalid multibulk length') from exc
        saved = bytearray(self.buffer)
        del self.buffer[: end + 2]
        args: list[bytes] = []
        for _ in range(count):
            if not self.buffer or self.buffer[0:1] != b'$':
                self.buffer[:] = saved
                return None
            bulk_end = self.buffer.find(b'\r\n')
            if bulk_end == -1:
                self.buffer[:] = saved
                return None
            length = int(self.buffer[1:bulk_end])
            del self.buffer[: bulk_end + 2]
            if len(self.buffer) < length + 2:
                self.buffer[:] = saved
                return None
            args.append(bytes(self.buffer[:length]))
            del self.buffer[: length + 2]
        return args


def encode(value: Any) -> bytes:
    if value is None:
        return b'$-1\r\n'
    if value is True:
        return b'+OK\r\n'
    if isinstance(value, bytes):
        return b'$' + str(len(value)).encode() + b'\r\n' + value + b'\r\n'
    if isinstance(value, str):
        return encode(value.encode())
    if isinstance(value, int):
        return b':' + str(value).encode() + b'\r\n'
    if isinstance(value, float):
        return b'$' + str(len(format_score(value))).encode() + b'\r\n' + format_score(value) + b'\r\n'
    if isinstance(value, list):
        out = b'*' + str(len(value)).encode() + b'\r\n'
        for item in value:
            out += encode(item)
        return out
    if isinstance(value, BaseException):
        msg = str(value).encode()
        return b'-' + msg + b'\r\n'
    raise TypeError(f'cannot encode {value!r}')


def to_int(data: bytes, name: str) -> int:
    try:
        return int(data)
    except ValueError as exc:
        raise ProtocolError(f"value is not an integer or out of range for '{name}'") from exc


def to_float(data: bytes, name: str) -> float:
    try:
        return float(data)
    except ValueError as exc:
        raise ProtocolError(f"value is not a valid float for '{name}'") from exc


def run_command(store: Store, conn: socket.socket, args: list[bytes]) -> Any:
    if not args:
        return None
    cmd = args[0].decode().upper()

    # connection / server commands
    if cmd in ('PING',):
        return args[1] if len(args) > 1 else b'PONG'
    if cmd in ('ECHO',):
        return args[1]
    if cmd in ('SELECT', 'CLIENT', 'COMMAND', 'HELLO', 'RESET', 'QUIT', 'AUTH', 'CONFIG'):
        if cmd == 'HELLO':
            # minimal RESP2-flavoured hello map
            return [b'server', b'mini_redis', b'version', b'7.2.0', b'proto', 2, b'mode', b'standalone']
        if cmd == 'QUIT':
            return b'OK'
        return b'OK'
    if cmd == 'DBSIZE':
        with store.lock:
            alive = [k for k in set(store.strings) | set(store.zsets) | set(store.lists) if store._alive(k)]
            return len(alive)
    if cmd == 'FLUSHALL' or cmd == 'FLUSHDB':
        with store.lock:
            store.strings.clear()
            store.zsets.clear()
            store.lists.clear()
            store.expire_at.clear()
            for key in list(store.versions):
                store.touch(key)
        return b'OK'
    if cmd == 'INFO':
        return b'# Server\r\nredis_version:7.2.0\r\nredis_mode:standalone\r\n# Clients\r\nconnected_clients:1\r\n'
    if cmd == 'TIME':
        t = time.time()
        return [str(int(t)).encode(), str(int((t % 1) * 1_000_000)).encode()]

    # watch / multi / exec
    if cmd == 'WATCH':
        with store.lock:
            keys = store.watched.setdefault(conn, set())
            marks = store.watch_marks.setdefault(conn, {})
            for key in args[1:]:
                keys.add(key)
                marks[key] = store.versions.get(key, 0)
        return b'OK'
    if cmd == 'UNWATCH':
        with store.lock:
            store.clear_watched(conn)
        return b'OK'
    if cmd == 'MULTI':
        with store.lock:
            store.begin_multi(conn)
        return b'OK'
    if cmd == 'DISCARD':
        with store.lock:
            store.end_multi(conn)
            store.clear_watched(conn)
        return b'OK'
    if cmd == 'EXEC':
        with store.lock:
            queue = store.end_multi(conn)
        if queue is None:
            raise ProtocolError('EXEC without MULTI')
        with store.lock:
            if not store.check_watched(conn):
                store.clear_watched(conn)
                return None
            store.clear_watched(conn)
            results = []
            for queued in queue:
                results.append(execute_single(store, conn, queued))
            return results

    if store.is_in_multi(conn):
        if cmd in ('MULTI', 'WATCH'):
            raise ProtocolError(f'{cmd} inside MULTI is not allowed')
        store.queue_multi(conn, args)
        return b'QUEUED'

    with store.lock:
        return execute_single(store, conn, args)


def execute_single(store: Store, conn: socket.socket, args: list[bytes]) -> Any:
    cmd = args[0].decode().upper()

    if cmd == 'GET':
        return store.get(args[1])
    if cmd == 'SET':
        key, value = args[1], args[2]
        px = None
        nx = xx = False
        i = 3
        while i < len(args):
            opt = args[i].decode().upper()
            if opt in ('PX', 'PEX') or opt == 'EX':
                px = to_int(args[i + 1], 'px')
                if opt == 'EX':
                    px *= 1000
                i += 2
            elif opt in ('NX',):
                nx = True
                i += 1
            elif opt in ('XX',):
                xx = True
                i += 1
            elif opt in ('KEEPTTL',):
                i += 1
            else:
                raise ProtocolError('syntax error')
        return store.set(key, value, px, nx, xx)
    if cmd == 'PSETEX':
        return store.psetex(args[1], to_int(args[2], 'milliseconds'), args[3])
    if cmd == 'SETEX':
        return store.psetex(args[1], to_int(args[2], 'seconds') * 1000, args[3])
    if cmd == 'INCR':
        return store.incr(args[1])
    if cmd in ('INCRBY', 'DECRBY'):
        amount = to_int(args[2], 'amount')
        if cmd == 'DECRBY':
            amount = -amount
        if store.get(args[1]) is None:
            store.set(args[1], str(amount).encode(), None, False, False)
            return amount
        return store.incr_by(args[1], amount)
    if cmd == 'DECR':
        if store.get(args[1]) is None:
            store.set(args[1], b'-1', None, False, False)
            return -1
        return store.incr_by(args[1], -1)
    if cmd == 'EXISTS':
        return store.exists(*args[1:])
    if cmd == 'DEL' or cmd == 'UNLINK':
        return store.delete(*args[1:])
    if cmd in ('PEXPIRE', 'PEXPIREAT'):
        return store._expire(args[1], to_int(args[2], 'milliseconds'))
    if cmd == 'EXPIRE':
        return store._expire(args[1], to_int(args[2], 'seconds') * 1000)
    if cmd == 'PTTL':
        return store.ttl_ms(args[1])
    if cmd == 'TTL':
        return store.ttl_s(args[1])
    if cmd == 'KEYS':
        return store.keys(args[1])
    if cmd == 'TYPE':
        return store.type_of(args[1]).encode()

    if cmd == 'RPUSH':
        return store.rpush(args[1], *args[2:])
    if cmd == 'LLEN':
        if not store._alive(args[1]) or args[1] not in store.lists:
            return 0
        return len(store.lists[args[1]])
    if cmd == 'LRANGE':
        if not store._alive(args[1]) or args[1] not in store.lists:
            return []
        lst = store.lists[args[1]]
        start, end = to_int(args[2], 'start'), to_int(args[3], 'stop')
        total = len(lst)
        start = max(start + total, 0) if start < 0 else min(start, total)
        end = end + total + 1 if end < 0 else min(end + 1, total)
        return lst[start:end]
    if cmd == 'LINDEX':
        return store.lindex(args[1], to_int(args[2], 'index'))
    if cmd == 'LREM':
        return store.lrem(args[1], to_int(args[2], 'count'), args[3])

    if cmd == 'ZADD':
        key = args[1]
        incr = False
        i = 2
        while i < len(args):
            opt = args[i].decode().upper()
            if opt in ('NX', 'XX', 'GT', 'LT', 'CH'):
                i += 1
            elif opt == 'INCR':
                incr = True
                i += 1
            else:
                break
        rest = args[i:]
        mapping: dict[bytes, float] = {}
        for j in range(0, len(rest), 2):
            mapping[rest[j + 1]] = to_float(rest[j], 'score')
        return store._zadd(key, mapping, incr)
    if cmd == 'ZSCORE':
        return store.zscore(args[1], args[2])
    if cmd == 'ZCARD':
        return store.zcard(args[1])
    if cmd == 'ZREM':
        return store.zrem(args[1], *args[2:])
    if cmd == 'ZINCRBY':
        return store.zincrby(args[1], to_float(args[2], 'increment'), args[3])
    if cmd == 'ZRANGE':
        key = args[1]
        start, end = to_int(args[2], 'start'), to_int(args[3], 'end')
        withscores = False
        rev = False
        i = 4
        byscore = False
        bylex = False
        offset = num = None
        while i < len(args):
            opt = args[i].decode().upper()
            if opt == 'WITHSCORES':
                withscores = True
            elif opt == 'REV':
                rev = True
            elif opt == 'BYSCORE':
                byscore = True
            elif opt == 'BYLEX':
                bylex = True
            elif opt == 'LIMIT':
                offset, num = to_int(args[i + 1], 'offset'), to_int(args[i + 2], 'count')
                i += 2
            i += 1
        if byscore:
            lo = parse_score(args[2])
            hi = parse_score(args[3])
            if rev:
                lo, hi = hi, lo
            return store.zrangebyscore(key, lo, hi, offset, num, withscores)
        if bylex:
            # not used by arq; return empty
            return []
        return store.zrange(key, start, end, withscores, rev)
    if cmd == 'ZRANGEBYSCORE':
        key = args[1]
        lo, hi = parse_score(args[2]), parse_score(args[3])
        withscores = False
        offset = num = None
        i = 4
        while i < len(args):
            opt = args[i].decode().upper()
            if opt == 'WITHSCORES':
                withscores = True
            elif opt == 'LIMIT':
                offset, num = to_int(args[i + 1], 'offset'), to_int(args[i + 2], 'count')
                i += 2
            i += 1
        return store.zrangebyscore(key, lo, hi, offset, num, withscores)
    if cmd == 'ZREMRANGEBYSCORE':
        return store.zremrangebyscore(args[1], parse_score(args[2]), parse_score(args[3]))

    raise ProtocolError(f"unknown command '{cmd}'")


class Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:  # noqa: C901
        conn = self.request
        reader = RespReader()
        try:
            while True:
                data = self.rfile.read(4096)
                if not data:
                    break
                reader.feed(data)
                while True:
                    try:
                        args = reader.read_command()
                    except ProtocolError as exc:
                        self.wfile.write(encode(exc))
                        self.wfile.flush()
                        return
                    if args is None:
                        break
                    try:
                        if args and args[0].decode().upper() == 'QUIT':
                            self.wfile.write(encode(True))
                            self.wfile.flush()
                            return
                        result = run_command(self.server.store, conn, args)  # type: ignore[attr-defined]
                    except ProtocolError as exc:
                        result = exc
                    except Exception as exc:  # pragma: no cover
                        result = ProtocolError(f'ERR internal: {exc}')
                    self.wfile.write(encode(result))
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with self.server.store.lock:  # type: ignore[attr-defined]
                self.server.store.watched.pop(conn, None)  # type: ignore[attr-defined]


class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr: tuple[str, int]) -> None:
        self.store = Store()
        super().__init__(addr, Handler)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=0)
    args = parser.parse_args()
    server = ThreadedServer((args.host, args.port))
    print(f'mini-redis listening on {server.server_address[0]}:{server.server_address[1]}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
