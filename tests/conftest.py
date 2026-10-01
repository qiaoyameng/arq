import asyncio
import functools
import os
import sys
from collections.abc import Generator

import msgpack
import pytest
import redis.exceptions
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from testcontainers.redis import RedisContainer

from arq.connections import ArqRedis, RedisSettings, create_pool
from arq.worker import Worker


def _inproc_pool(**kwargs):
    local_redis_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'local-redis'))
    if local_redis_dir not in sys.path:
        sys.path.insert(0, local_redis_dir)
    import inproc_redis

    return inproc_redis._make_pool(**kwargs)


def _external_redis() -> bool:
    # run against an independent redis already available on host/port
    # (eg. ``redis-server`` or ``python local-redis/mini_redis.py``)
    return bool(os.getenv('ARQ_TEST_REDIS_HOST')) and not os.getenv('ARQ_TEST_NO_DOCKER')


if os.getenv('ARQ_TEST_NO_DOCKER'):
    import warnings

    warnings.filterwarnings('ignore', message='Call to deprecated close.*', category=DeprecationWarning)

    # workers created from RedisSettings (rather than an injected pool) must also
    # be redirected to the in-process fake
    from arq import connections as _arq_connections
    from arq import worker as _arq_worker

    async def _inproc_create_pool(settings_=None, *, retry=0, **kwargs):
        from arq.connections import ArqRedis

        return ArqRedis(connection_pool=_inproc_pool(), **kwargs)

    _arq_connections.create_pool = _inproc_create_pool  # type: ignore[assignment]
    _arq_worker.create_pool = _inproc_create_pool  # type: ignore[assignment]


@pytest.fixture(name='loop')
def _fix_loop(event_loop: asyncio.AbstractEventLoop) -> asyncio.AbstractEventLoop:
    return event_loop


@pytest.fixture(scope='session')
def redis_version() -> str:
    return os.getenv('ARQ_TEST_REDIS_VERSION', 'latest')


@pytest.fixture(scope='session')
def redis_container(redis_version: str) -> Generator[RedisContainer, None, None]:
    if os.getenv('ARQ_TEST_NO_DOCKER') or _external_redis():
        yield None
        return
    with RedisContainer(f'redis:{redis_version}') as redis:
        yield redis


@pytest.fixture(scope='session')
def test_redis_host(redis_container: RedisContainer) -> str:
    if _external_redis():
        return os.environ['ARQ_TEST_REDIS_HOST']
    if os.getenv('ARQ_TEST_NO_DOCKER'):
        return os.getenv('ARQ_TEST_REDIS_HOST', 'in-process')
    return redis_container.get_container_host_ip()


@pytest.fixture(scope='session')
def test_redis_port(redis_container: RedisContainer) -> int:
    if os.getenv('ARQ_TEST_NO_DOCKER') or _external_redis():
        return int(os.getenv('ARQ_TEST_REDIS_PORT', '0'))
    return redis_container.get_exposed_port(redis_container.port)


@pytest.fixture(scope='session')
def test_redis_settings(test_redis_host: str, test_redis_port: int) -> RedisSettings:
    return RedisSettings(host=test_redis_host, port=test_redis_port)


@pytest.fixture
async def arq_redis(test_redis_host: str, test_redis_port: int):
    if os.getenv('ARQ_TEST_NO_DOCKER') and test_redis_host == 'in-process':
        redis_ = ArqRedis(connection_pool=_inproc_pool())
        await redis_.flushall()
        yield redis_
        await redis_.close(close_connection_pool=True)
        return

    redis_ = ArqRedis(
        host=test_redis_host,
        port=test_redis_port,
        encoding='utf-8',
    )

    await redis_.flushall()

    yield redis_

    await redis_.close(close_connection_pool=True)


@pytest.fixture
async def arq_redis_msgpack(test_redis_host: str, test_redis_port: int):
    if os.getenv('ARQ_TEST_NO_DOCKER') and test_redis_host == 'in-process':
        redis_ = ArqRedis(
            connection_pool=_inproc_pool(),
            job_serializer=msgpack.packb,
            job_deserializer=functools.partial(msgpack.unpackb, raw=False),
        )
        await redis_.flushall()
        yield redis_
        await redis_.close(close_connection_pool=True)
        return

    redis_ = ArqRedis(
        host=test_redis_host,
        port=test_redis_port,
        encoding='utf-8',
        job_serializer=msgpack.packb,
        job_deserializer=functools.partial(msgpack.unpackb, raw=False),
    )
    await redis_.flushall()
    yield redis_
    await redis_.close(close_connection_pool=True)


@pytest.fixture
async def arq_redis_retry(test_redis_host: str, test_redis_port: int):
    if os.getenv('ARQ_TEST_NO_DOCKER') and test_redis_host == 'in-process':
        redis_ = ArqRedis(
            connection_pool=_inproc_pool(
                retry=Retry(backoff=NoBackoff(), retries=3),
                retry_on_timeout=True,
                retry_on_error=[redis.exceptions.ConnectionError],
            ),
            encoding='utf-8',
        )
        await redis_.flushall()
        yield redis_
        await redis_.close(close_connection_pool=True)
        return

    redis_ = ArqRedis(
        host=test_redis_host,
        port=test_redis_port,
        encoding='utf-8',
        retry=Retry(backoff=NoBackoff(), retries=3),
        retry_on_timeout=True,
        retry_on_error=[redis.exceptions.ConnectionError],
    )
    await redis_.flushall()
    yield redis_
    await redis_.close(close_connection_pool=True)


@pytest.fixture
async def worker(arq_redis):
    worker_: Worker = None

    def create(functions=[], burst=True, poll_delay=0, max_jobs=10, arq_redis=arq_redis, **kwargs):
        nonlocal worker_
        worker_ = Worker(
            functions=functions, redis_pool=arq_redis, burst=burst, poll_delay=poll_delay, max_jobs=max_jobs, **kwargs
        )
        return worker_

    yield create

    if worker_:
        await worker_.close()


@pytest.fixture
async def worker_retry(arq_redis_retry):
    worker_retry_: Worker = None

    def create(functions=[], burst=True, poll_delay=0, max_jobs=10, arq_redis=arq_redis_retry, **kwargs):
        nonlocal worker_retry_
        worker_retry_ = Worker(
            functions=functions,
            redis_pool=arq_redis,
            burst=burst,
            poll_delay=poll_delay,
            max_jobs=max_jobs,
            **kwargs,
        )
        return worker_retry_

    yield create

    if worker_retry_:
        await worker_retry_.close()


@pytest.fixture(name='create_pool')
async def fix_create_pool(loop):
    pools = []

    async def create_pool_(settings, *args, **kwargs):
        if os.getenv('ARQ_TEST_NO_DOCKER'):
            from arq.connections import ArqRedis

            pool = ArqRedis(connection_pool=_inproc_pool(), *args, **kwargs)
            pools.append(pool)
            return pool
        pool = await create_pool(settings, *args, **kwargs)
        pools.append(pool)
        return pool

    yield create_pool_

    await asyncio.gather(*[p.close(close_connection_pool=True) for p in pools])


@pytest.fixture(name='cancel_remaining_task')
def fix_cancel_remaining_task(loop):
    async def cancel_remaining_task():
        tasks = asyncio.all_tasks(loop)
        cancelled = []
        for task in tasks:
            # in repr works in 3.7 where get_coro() is not available
            if 'cancel_remaining_task()' not in repr(task):
                cancelled.append(task)
                task.cancel()
        if cancelled:
            print(f'Cancelled {len(cancelled)} ongoing tasks', file=sys.stderr)
            await asyncio.gather(*cancelled, return_exceptions=True)

    yield

    loop.run_until_complete(cancel_remaining_task())


class SetEnv:
    def __init__(self):
        self.envars = set()

    def set(self, name, value):
        self.envars.add(name)
        os.environ[name] = value

    def clear(self):
        for n in self.envars:
            os.environ.pop(n)


@pytest.fixture
def env():
    setenv = SetEnv()

    yield setenv

    setenv.clear()
