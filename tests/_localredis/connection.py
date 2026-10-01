"""
redis-py ``Connection`` implementation that talks to an in-process :class:`InProcessFakeRedis`
without using real sockets (sandboxes often forbid socket creation).
"""
from __future__ import annotations

import os
from typing import Any

from redis.asyncio.connection import Connection

from .fake_redis import InProcessFakeRedis

# shared server used by all connections inside one pytest process (flushall is called by fixtures)
SERVER = InProcessFakeRedis()


class FakeRedisConnection(Connection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._fake_enabled = int(kwargs.get('port') or 0) == int(os.getenv('ARQ_TEST_REDIS_PORT', '6399'))

    async def _connect(self) -> None:
        if not self._fake_enabled:
            # nothing is listening on this port; raise directly instead of attempting a real
            # socket (which may be blocked by the sandbox)
            raise OSError('connect failed: no redis on this port')
        self._reader, self._writer = await SERVER.new_endpoints()

    @property
    def is_connected(self) -> bool:  # type: ignore[override]
        return self._reader is not None and not self._reader.at_eof()
