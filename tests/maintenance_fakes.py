import fnmatch
import time
from typing import Optional


class FakeString:
    def __init__(self, value: bytes, pttl: Optional[int]):
        self.value = value
        self.expires_at = None if pttl is None else time.monotonic() + pttl / 1000

    def pttl(self) -> int:
        if self.expires_at is None:
            return -1
        return max(int((self.expires_at - time.monotonic()) * 1000), 0)


class FakePipeline:
    def __init__(self, redis: 'FakeRedis', transaction: bool = True):
        self.redis = redis
        self.transaction = transaction
        self.commands = []
        self.multi_commands = []
        self.in_multi = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def watch(self, key: str):
        pass

    def multi(self):
        self.commands = []
        self.in_multi = True

    def pttl(self, key: str):
        return self._read(('pttl', (key,)))

    def exists(self, *keys: str):
        return self._read(('exists', keys))

    def type(self, key: str):
        return self._read(('type', (key,)))

    def zscore(self, queue: str, job_id: str):
        return self._read(('zscore', (queue, job_id)))

    def get(self, key: str):
        return self._read(('get', (key,)))

    def zrem(self, queue: str, *members: str):
        if self.in_multi:
            self.multi_commands.append(('zrem', (queue, *members)))
        elif self.transaction:
            return self.redis.zrem(queue, *members)
        else:
            self.commands.append(('zrem', (queue, *members)))

    def delete(self, *keys: str):
        if self.in_multi:
            self.multi_commands.append(('delete', keys))
        elif self.transaction:
            return self.redis.delete(*keys)
        else:
            self.commands.append(('delete', keys))

    def _read(self, command):
        if not self.transaction:
            self.commands.append(command)
            return None
        if self.in_multi:
            self.multi_commands.append(command)
            return None
        return self._read_async(command)

    async def _read_async(self, command):
        return await self._execute_command(command)

    async def _execute_command(self, command):
        operation, args = command
        method = getattr(self.redis, operation)
        return await method(*args)

    async def execute(self):
        if self.in_multi:
            commands = list(self.multi_commands)
            self.multi_commands = []
            values = [await self._execute_command(command) for command in commands]
            self.in_multi = False
            return values
        values = [await self._execute_command(command) for command in self.commands]
        self.commands = []
        return values


class FakeRedis:
    def __init__(self):
        self.strings: dict[str, FakeString] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def pipeline(self, transaction: bool = True):
        return FakePipeline(self, transaction=transaction)

    async def close(self, close_connection_pool: bool = True):
        return None

    async def scan_iter(self, match: str, count: int = 500):
        for key in sorted((*self.strings, *self.zsets)):
            if fnmatch.fnmatchcase(key, match):
                yield key.encode()

    async def zscan_iter(self, name: str, count: int = 500):
        for member, score in sorted(self.zsets.get(name, {}).items()):
            yield member.encode(), float(score)

    async def type(self, key: str) -> bytes:
        item = self.strings.get(key)
        if item is not None and item.pttl() != 0:
            return b'string'
        if key in self.zsets:
            return b'zset'
        return b'none'

    async def get(self, key: str):
        item = self.strings.get(key)
        if item is None or item.pttl() == 0:
            return None
        return item.value

    async def set(self, key: str, value: bytes, px: Optional[int] = None):
        self.strings[key] = FakeString(value, px)

    async def setex(self, key: str, seconds: int, value: bytes):
        self.strings[key] = FakeString(value, seconds * 1000)

    async def psetex(self, key: str, milliseconds: int, value: bytes):
        self.strings[key] = FakeString(value, milliseconds)

    async def persist(self, key: str):
        self.strings[key].expires_at = None

    async def pttl(self, key: str) -> int:
        item = self.strings.get(key)
        return -2 if item is None else item.pttl()

    async def exists(self, *keys: str) -> int:
        count = 0
        for key in keys:
            item = self.strings.get(key)
            if item is not None and item.pttl() != 0:
                count += 1
        return count

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self.strings:
                del self.strings[key]
                count += 1
        return count

    async def zadd(self, key: str, mapping: dict[str, float]):
        self.zsets.setdefault(key, {}).update(mapping)

    async def zscore(self, key: str, member: str):
        return self.zsets.get(key, {}).get(member)

    async def zrem(self, key: str, *members: str) -> int:
        values = self.zsets.get(key, {})
        count = sum(1 for member in members if member in values)
        for member in members:
            values.pop(member, None)
        return count
