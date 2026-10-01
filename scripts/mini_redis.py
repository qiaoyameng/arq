"""
Minimal standalone RESP2 Redis server used only to verify arq's serial_group
feature locally when no redis-server / docker is available.

Command surface (only what arq needs):
  connection: PING, HELLO, SELECT, CLIENT SETINFO/SETNAME/GETNAME, RESET, QUIT,
              COMMAND, CONFIG GET
  strings:    GET, SET (NX/XX/PX/EX), PSETEX, SETEX, INCR, EXISTS, DELETE,
              EXPIRE, PEXPIRE, KEYS, TYPE
  zsets:      ZADD, ZSCORE, ZRANGE / ZRANGEBYSCORE, ZCARD, ZREM, ZINCRBY,
              ZREMRANGEBYSCORE
  tx:         WATCH, MULTI, EXEC, DISCARD, UNWATCH
  server:     INFO, DBSIZE, FLUSHALL, FLUSHDB

Keys live in memory with millisecond TTLs and lazy expiration. WATCH is
implemented with per-key monotonic versions, matching Redis CAS semantics.
"""
import asyncio
import fnmatch
import sys
import time
from collections import defaultdict

CRLF = b'\r\n'


class SimpleError(Exception):
    def __init__(self, msg):
        self.msg = msg


def enc(obj):
    if obj is None:
        return b'$-1\r\n'
    if isinstance(obj, bool):
        return b':1\r\n' if obj else b':0\r\n'
    if isinstance(obj, int):
        return f':{obj}\r\n'.encode()
    if isinstance(obj, float):
        s = ('%.17g' % obj)
        return b'$' + str(len(s)).encode() + CRLF + s.encode() + CRLF
    if isinstance(obj, str):
        return enc(obj.encode())
    if isinstance(obj, bytes):
        return b'$' + str(len(obj)).encode() + CRLF + obj + CRLF
    if isinstance(obj, (list, tuple)):
        out = b'*' + str(len(obj)).encode() + CRLF
        for item in obj:
            out += err(item.msg) if isinstance(item, SimpleError) else enc(item)
        return out
    raise TypeError(f'cannot encode {type(obj)}')


def err(msg):
    return b'-' + msg.encode() + CRLF


def now_ms():
    return int(time.time() * 1000)


class Store:
    def __init__(self):
        self.data = {}          # key -> bytes or dict(member->float)
        self.expire_at = {}     # key -> ms
        self.versions = defaultdict(int)

    def _live(self, key):
        exp = self.expire_at.get(key)
        if exp is not None and exp <= now_ms():
            self._del(key)

    def _del(self, key):
        self.data.pop(key, None)
        self.expire_at.pop(key, None)
        self.versions[key] += 1

    def get(self, key):
        self._live(key)
        v = self.data.get(key)
        return v if isinstance(v, bytes) else None

    def set(self, key, val, px=None, nx=False, xx=False):
        self._live(key)
        exists = key in self.data
        if nx and exists:
            return None
        if xx and not exists:
            return None
        self.data[key] = val
        if px is not None:
            self.expire_at[key] = now_ms() + px
        else:
            self.expire_at.pop(key, None)
        self.versions[key] += 1
        return True

    def delete(self, *keys):
        n = 0
        for k in keys:
            self._live(k)
            if k in self.data:
                self._del(k)
                n += 1
        return n

    def exists(self, *keys):
        return sum(1 for k in keys if (self._live(k) or True) and k in self.data)

    def incr(self, key):
        self._live(key)
        v = self.data.get(key)
        n = 1 if v is None else int(v) + 1
        self.data[key] = str(n).encode()
        self.versions[key] += 1
        return n

    def incrby(self, key, amount):
        self._live(key)
        v = self.data.get(key)
        n = amount if v is None else int(v) + amount
        self.data[key] = str(n).encode()
        self.versions[key] += 1
        return n

    def keys(self, pattern):
        for k in list(self.data):
            self._live(k)
        return [k.encode() for k in self.data if fnmatch.fnmatchcase(k, pattern)]

    def _zset(self, key):
        self._live(key)
        z = self.data.get(key)
        if z is None:
            z = {}
            self.data[key] = z
        elif not isinstance(z, dict):
            raise SimpleError('WRONGTYPE Operation against a key holding the wrong kind of value')
        return z

    def zadd(self, key, mapping, nx=False, xx=False):
        z = self._zset(key)
        added = 0
        for member, score in mapping:
            has = member in z
            if nx and has:
                continue
            if xx and not has:
                continue
            if not has:
                added += 1
            z[member] = score
        if not z:
            self._del(key)
        else:
            self.versions[key] += 1
        return added

    def zscore(self, key, member):
        z = self._zset(key)
        if key not in self.data:
            self._del(key)
        return z.get(member)

    def _ordered(self, z):
        return sorted(z.items(), key=lambda kv: (kv[1], kv[0]))

    def zrange(self, key, start, end, withscores=False):
        self._live(key)
        z = self.data.get(key)
        if not isinstance(z, dict):
            return []
        items = self._ordered(z)
        n = len(items)
        if start < 0:
            start = max(n + start, 0)
        if end < 0:
            end = n + end
        end = min(end, n - 1)
        sel = items[start:end + 1] if start <= end else []
        if withscores:
            out = []
            for m, s in sel:
                out.append(m)
                out.append(repr_intfloat(s))
            return out
        return [m for m, _ in sel]

    def zrangebyscore(self, key, lo, hi, start=0, num=None, withscores=False):
        self._live(key)
        z = self.data.get(key)
        if not isinstance(z, dict):
            return []
        sel = [(m, s) for m, s in self._ordered(z) if lo <= s <= hi]
        if num is not None:
            sel = sel[start:start + num]
        elif start:
            sel = sel[start:]
        if withscores:
            out = []
            for m, s in sel:
                out.append(m)
                out.append(repr_intfloat(s))
            return out
        return [m for m, _ in sel]

    def zrem(self, key, *members):
        self._live(key)
        z = self.data.get(key)
        if not isinstance(z, dict):
            return 0
        n = 0
        for m in members:
            if z.pop(m, None) is not None:
                n += 1
        if not z:
            self._del(key)
        else:
            self.versions[key] += 1
        return n

    def zincrby(self, key, amount, member):
        z = self._zset(key)
        z[member] = z.get(member, 0.0) + amount
        self.versions[key] += 1
        return z[member]

    def zcard(self, key):
        self._live(key)
        z = self.data.get(key)
        return len(z) if isinstance(z, dict) else 0

    def zremrangebyscore(self, key, lo, hi):
        self._live(key)
        z = self.data.get(key)
        if not isinstance(z, dict):
            return 0
        dead = [m for m, s in z.items() if lo <= s <= hi]
        for m in dead:
            del z[m]
        if not z:
            self._del(key)
        else:
            self.versions[key] += 1
        return len(dead)


def repr_intfloat(s):
    if s == int(s):
        return str(int(s)).encode()
    return ('%.17g' % s).encode()


def to_score(b):
    s = b.decode().lower()
    if s in ('-inf', '+inf'):
        return float(s)
    f = float(s)
    return f


class Conn:
    cid_counter = 0

    def __init__(self, store):
        self.store = store
        self.buf = b''
        self.multi = False
        self.queued = []
        self.watched = {}
        self.dirty = False
        self.name = None
        Conn.cid_counter += 1
        self.cid = Conn.cid_counter

    # ---- single command ----
    def run(self, argv):
        cmd = argv[0].decode().upper() if argv else ''
        a = argv[1:]

        if cmd in ('MULTI', 'EXEC', 'DISCARD', 'WATCH', 'UNWATCH'):
            return getattr(self, 'cmd_' + cmd.lower())(a)

        if self.multi:
            self.queued.append(argv)
            return 'QUEUED'

        return self.dispatch(cmd, a)

    def dispatch(self, cmd, a):
        s = self.store
        if cmd == 'PING':
            return b'PONG'
        if cmd == 'SELECT':
            return 'OK'
        if cmd == 'HELLO':
            return [b'server', b'mini-redis', b'version', b'7.0.0', b'proto', 2,
                    b'id', self.cid, b'mode', b'standalone', b'role', b'master']
        if cmd == 'CLIENT':
            return b'OK'
        if cmd in ('COMMAND', 'CONFIG'):
            return [] if cmd == 'COMMAND' else [a[1] if len(a) > 1 else b'*', b'']
        if cmd == 'RESET':
            self.multi = False
            self.queued = []
            self.watched = {}
            self.dirty = False
            return 'OK'
        if cmd == 'QUIT':
            return 'OK'

        if cmd == 'GET':
            return s.get(a[0].decode())
        if cmd == 'SET':
            key, val = a[0].decode(), a[1]
            px, nx, xx = None, False, False
            i = 2
            while i < len(a):
                t = a[i].decode().upper()
                if t == 'PX':
                    px = int(a[i + 1]); i += 2
                elif t == 'EX':
                    px = int(float(a[i + 1]) * 1000); i += 2
                elif t == 'NX':
                    nx = True; i += 1
                elif t == 'XX':
                    xx = True; i += 1
                else:
                    i += 1
            r = s.set(key, val, px=px, nx=nx, xx=xx)
            return 'OK' if r else None
        if cmd == 'PSETEX':
            s.set(a[0].decode(), a[2], px=int(a[1]))
            return 'OK'
        if cmd == 'SETEX':
            s.set(a[0].decode(), a[2], px=int(float(a[1]) * 1000))
            return 'OK'
        if cmd in ('INCR', 'INCRBY'):
            amount = int(a[1]) if len(a) > 1 else 1
            return s.incrby(a[0].decode(), amount)
        if cmd == 'DECR':
            return s.incrby(a[0].decode(), -1)
        if cmd == 'DECRBY':
            return s.incrby(a[0].decode(), -int(a[1]))
        if cmd == 'EXISTS':
            return s.exists(*[x.decode() for x in a])
        if cmd == 'DEL' or cmd == 'DELETE':
            return s.delete(*[x.decode() for x in a])
        if cmd in ('EXPIRE',):
            s.set(a[0].decode(), s.get(a[0].decode()) or b'', px=int(float(a[1]) * 1000))
            return 1
        if cmd == 'PEXPIRE':
            v = s.get(a[0].decode())
            if v is None:
                return 0
            s.set(a[0].decode(), v, px=int(a[1]))
            return 1
        if cmd == 'KEYS':
            return s.keys(a[0].decode())
        if cmd == 'TYPE':
            s._live(a[0].decode())
            v = s.data.get(a[0].decode())
            if v is None:
                return b'none'
            return b'zset' if isinstance(v, dict) else b'string'

        if cmd == 'ZADD':
            key = a[0].decode()
            pairs, nx, xx = [], False, False
            i = 1
            while i + 1 < len(a):
                t = a[i].decode().upper()
                if t in ('NX', 'XX'):
                    nx = nx or t == 'NX'
                    xx = xx or t == 'XX'
                    i += 1
                elif t in ('GT', 'LT', 'CH', 'INCR'):
                    i += 1
                else:
                    pairs.append((a[i + 1], float(a[i])))
                    i += 2
            return s.zadd(key, pairs, nx=nx, xx=xx)
        if cmd == 'ZSCORE':
            v = s.zscore(a[0].decode(), a[1])
            return None if v is None else repr_intfloat(v)
        if cmd == 'ZCARD':
            return s.zcard(a[0].decode())
        if cmd == 'ZREM':
            return s.zrem(a[0].decode(), *a[1:])
        if cmd == 'ZINCRBY':
            return repr_intfloat(s.zincrby(a[0].decode(), float(a[1]), a[2]))
        if cmd == 'ZRANGE':
            key = a[0].decode()
            ws = any(x.decode().upper() == 'WITHSCORES' for x in a)
            if any(x.decode().upper() == 'BYSCORE' for x in a):
                lo, hi = to_score(a[1]), to_score(a[2])
                off, cnt = 0, None
                for i, x in enumerate(a):
                    if x.decode().upper() == 'LIMIT':
                        off, cnt = int(a[i + 1]), int(a[i + 2])
                return s.zrangebyscore(key, lo, hi, off, cnt, withscores=ws)
            return s.zrange(key, int(a[1]), int(a[2]), withscores=ws)
        if cmd == 'ZRANGEBYSCORE':
            key = a[0].decode()
            lo, hi = to_score(a[1]), to_score(a[2])
            ws, off, cnt = False, 0, None
            i = 3
            while i < len(a):
                t = a[i].decode().upper()
                if t == 'WITHSCORES':
                    ws = True
                elif t == 'LIMIT':
                    off, cnt = int(a[i + 1]), int(a[i + 2]); i += 2
                i += 1
            return s.zrangebyscore(key, lo, hi, off, cnt, withscores=ws)
        if cmd == 'ZREMRANGEBYSCORE':
            return s.zremrangebyscore(a[0].decode(), to_score(a[1]), to_score(a[2]))

        if cmd == 'INFO':
            return (
                b'# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\n'
                b'# Memory\r\nused_memory_human:0M\r\n'
                b'# Clients\r\nconnected_clients:1\r\n'
                b'# Keyspace\r\n'
            )
        if cmd == 'DBSIZE':
            return len(s.data)
        if cmd in ('FLUSHALL', 'FLUSHDB'):
            s.data.clear()
            s.expire_at.clear()
            return 'OK'

        raise SimpleError(f'ERR unknown command {cmd}')

    # ---- transaction commands ----
    def cmd_multi(self, a):
        if self.multi:
            raise SimpleError('ERR MULTI calls can not be nested')
        self.multi = True
        self.queued = []
        return 'OK'

    def cmd_watch(self, a):
        if self.multi:
            raise SimpleError('ERR WATCH inside MULTI is not allowed')
        for k in a:
            key = k.decode()
            self.store._live(key)
            self.watched[key] = self.store.versions[key]
        return 'OK'

    def cmd_unwatch(self, a):
        self.watched = {}
        self.dirty = False
        return 'OK'

    def cmd_discard(self, a):
        if not self.multi:
            raise SimpleError('ERR DISCARD without MULTI')
        self.multi = False
        self.queued = []
        return 'OK'

    def cmd_exec(self, a):
        if not self.multi:
            raise SimpleError('ERR EXEC without MULTI')
        for key, ver in self.watched.items():
            if self.store.versions[key] != ver:
                self.dirty = True
                break
        queued, dirty = self.queued, self.dirty
        self.multi = False
        self.queued = []
        self.watched = {}
        self.dirty = False
        if dirty:
            return None
        replies = []
        for argv in queued:
            try:
                replies.append(self.dispatch(argv[0].decode().upper(), argv[1:]))
            except SimpleError as e:
                replies.append(e)
        return replies


async def handle(reader, writer, store):
    conn = Conn(store)
    try:
        while True:
            line = await reader.readuntil(b'\r\n')
            if not line.startswith(b'*'):
                # inline command (not used by redis-py), minimal support
                argv = line.strip().split()
            else:
                n = int(line[1:].strip())
                argv = []
                for _ in range(n):
                    hdr = await reader.readuntil(b'\r\n')
                    if not hdr.startswith(b'$'):
                        raise SimpleError('protocol error')
                    size = int(hdr[1:].strip())
                    payload = await reader.readexactly(size)
                    await reader.readexactly(2)
                    argv.append(payload)
            try:
                res = conn.run(argv)
                if res == 'QUEUED':
                    writer.write(b'+QUEUED\r\n')
                elif isinstance(res, SimpleError):
                    writer.write(err(res.msg))
                else:
                    writer.write(enc(res))
            except SimpleError as e:
                writer.write(err(e.msg))
            await writer.drain()
            if argv and argv[0].decode().upper() == 'QUIT':
                break
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def amain(sock_path):
    store = Store()
    import os
    try:
        os.unlink(sock_path)
    except FileNotFoundError:
        pass
    server = await asyncio.start_unix_server(lambda r, w: handle(r, w, store), path=sock_path)
    print(f'mini-redis listening on {sock_path}', flush=True)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/mini-redis.sock'
    asyncio.run(amain(path))
