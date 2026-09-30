"""
Notification-based waiting on job results.

The project's main test suite drives redis through testcontainers; these tests
run against an in-process, pipe-backed RESP2 emulator (scripts/redis_emu.py) so
they work without docker/sockets, while exercising the unmodified redis-py
client, pipelines and pub/sub.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from arq.constants import default_queue_name, result_channel_prefix, result_key_prefix
from arq.jobs import Job, ResultExpired, ResultNotFound, serialize_result
from scripts.redis_emu import FakeRedisServer, InProcessConnection, drop_connection
from redis.asyncio.connection import ConnectionPool

from arq.connections import ArqRedis
from redis.asyncio import Redis as PlainRedis


@pytest.fixture
async def fake_redis():
    server = FakeRedisServer()
    InProcessConnection.fake_server = server
    pool = ConnectionPool(connection_class=InProcessConnection, max_connections=10)
    redis = ArqRedis(connection_pool=pool, encoding='utf8')
    await redis.ping()
    yield server, redis
    try:
        await redis.aclose(close_connection_pool=True)
    finally:
        await asyncio.sleep(0.05)
        await server.shutdown()
        import gc
        gc.collect()


def _result(job_id: str, value: object = 42) -> bytes:
    now = int(time.time() * 1000)
    return serialize_result(
        'fn', (), {}, 1, now, True, value, now, now, job_id, default_queue_name, job_id=job_id
    )


async def _finish(redis: ArqRedis, job_id: str, value: object = 42, *, delay: float = 0) -> None:
    if delay:
        await asyncio.sleep(delay)
    async with redis.pipeline(transaction=True) as tr:
        tr.set(result_key_prefix + job_id, _result(job_id, value), px=60_000)
        tr.publish(result_channel_prefix + job_id, job_id)
        tr.zrem(default_queue_name, job_id)
        await tr.execute()


async def test_notification_waiting_replaces_polling(fake_redis):
    server, redis = fake_redis
    await redis.zadd(default_queue_name, {'j': 1})
    job = Job('j', redis)

    t0 = time.monotonic()
    finish_task = asyncio.create_task(_finish(redis, 'j', delay=0.4))
    assert await job.result(timeout=5) == 42
    assert time.monotonic() - t0 < 2
    await finish_task


async def test_result_still_readable_after_notification(fake_redis):
    _, redis = fake_redis
    await redis.set(result_key_prefix + 'j', _result('j', 'kept'), px=60_000)
    job = Job('j', redis)
    assert await job.result(timeout=1) == 'kept'
    # notification was only a wake-up; the persisted result stays readable
    assert await job.result(timeout=1) == 'kept'


async def test_many_waiters_share_one_subscription(fake_redis):
    server, redis = fake_redis
    await redis.zadd(default_queue_name, {'j': 1})
    job = Job('j', redis)
    tasks = [asyncio.create_task(job.result(timeout=5)) for _ in range(5)]
    channel = (result_channel_prefix + 'j').encode()
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and len(redis.result_notifier._waiters.get(channel, ())) < 5:
        await asyncio.sleep(0.02)
    await asyncio.sleep(0.1)

    waiters = redis.result_notifier._waiters
    assert channel in waiters and len(waiters[channel]) == 5
    assert sum(1 for c in server.connections if c.in_subscribe) == 1

    await _finish(redis, 'j')
    assert await asyncio.gather(*tasks) == [42] * 5


async def test_cancelling_one_waiter_keeps_others(fake_redis):
    _, redis = fake_redis
    await redis.zadd(default_queue_name, {'j': 1})
    job = Job('j', redis)
    tasks = [asyncio.create_task(job.result(timeout=5)) for _ in range(3)]
    await asyncio.sleep(0.3)

    tasks[1].cancel()
    with pytest.raises(asyncio.CancelledError):
        await tasks[1]
    await asyncio.sleep(0.2)

    channel = (result_channel_prefix + 'j').encode()
    assert len(redis.result_notifier._waiters[channel]) == 2
    await _finish(redis, 'j', value='ok')
    assert await tasks[0] == 'ok'
    assert await tasks[2] == 'ok'


async def test_completion_in_subscribe_window(fake_redis):
    """Complete the job in the gap between SUBSCRIBE confirmed and state re-check."""
    server, redis = fake_redis
    await redis.zadd(default_queue_name, {'j': 1})
    job = Job('j', redis)

    def before_exec(key: bytes) -> None:
        channel = (result_channel_prefix + 'j').encode()
        if key == result_key_prefix.encode() + b'j' and channel in server.subscribers:
            server.before_state_check = None
            asyncio.create_task(_finish(redis, 'j', value='in-window'))

    server.before_state_check = before_exec
    assert await job.result(timeout=5) == 'in-window'


async def test_lost_notification_on_drop_recovers(fake_redis):
    server, redis = fake_redis
    await redis.zadd(default_queue_name, {'j': 1})
    job = Job('j', redis)
    task = asyncio.create_task(job.result(timeout=10))
    channel = (result_channel_prefix + 'j').encode()
    deadline = time.monotonic() + 3
    while channel not in server.subscribers and time.monotonic() < deadline:
        await asyncio.sleep(0.02)

    drop_connection(server, subscriber=True)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if any(not c.closing for c in server.subscribers.get(channel, ())):
            break
        await asyncio.sleep(0.02)
    # finish while nothing live is subscribed: the PUBLISH reaches nobody
    async with redis.pipeline(transaction=True) as tr:
        tr.set(result_key_prefix + 'j', _result('j', 'after-drop'), px=60_000)
        tr.publish(result_channel_prefix + 'j', 'j')
        tr.zrem(default_queue_name, 'j')
        await tr.execute()

    assert await asyncio.wait_for(task, timeout=10) == 'after-drop'


async def test_idle_drop_keeps_waiting(fake_redis):
    server, redis = fake_redis
    await redis.zadd(default_queue_name, {'j': 1})
    job = Job('j', redis)
    task = asyncio.create_task(job.result(timeout=10))
    channel = (result_channel_prefix + 'j').encode()
    deadline = time.monotonic() + 3
    while channel not in server.subscribers and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    drop_connection(server, subscriber=True)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if any(not c.closing for c in server.subscribers.get(channel, ())):
            break
        await asyncio.sleep(0.02)
    await _finish(redis, 'j', value='later')
    assert await asyncio.wait_for(task, timeout=10) == 'later'


async def test_unknown_job_is_not_found(fake_redis):
    _, redis = fake_redis
    with pytest.raises(ResultNotFound):
        await Job('ghost', redis).result(timeout=0.5)
    assert issubclass(ResultExpired, ResultNotFound)


async def test_expiry_distinguished_from_not_found(fake_redis):
    server, redis = fake_redis
    await redis.zadd(default_queue_name, {'j': 1})
    job = Job('j', redis)
    task = asyncio.create_task(job.result(timeout=5))
    await asyncio.sleep(0.3)

    async with redis.pipeline(transaction=True) as tr:
        tr.set(result_key_prefix + 'j', _result('j'), px=1)
        tr.zrem(default_queue_name, 'j')
        await tr.execute()
    await asyncio.sleep(0.05)
    await redis.publish(result_channel_prefix + 'j', 'j')

    with pytest.raises(ResultExpired):
        await task
    assert issubclass(ResultExpired, ResultNotFound)


async def test_timeout_still_works(fake_redis):
    _, redis = fake_redis
    await redis.zadd(default_queue_name, {'j': 1})
    with pytest.raises(asyncio.TimeoutError):
        await Job('j', redis).result(timeout=0.2)


async def test_polling_fallback_for_plain_redis(fake_redis):
    """A client without a ResultNotifier (plain redis-py) still polls."""
    server, _ = fake_redis
    InProcessConnection.fake_server = server
    pool = ConnectionPool(connection_class=InProcessConnection)
    plain = PlainRedis(connection_pool=pool, encoding='utf8')
    await plain.ping()
    await plain.zadd(default_queue_name, {'j': 1})
    job = Job('j', plain)

    async def finish() -> None:
        await asyncio.sleep(0.2)
        await plain.set(result_key_prefix + 'j', _result('j', 'fallback'), px=60_000)
        await plain.zrem(default_queue_name, 'j')

    task = asyncio.create_task(finish())
    assert await job.result(timeout=3, poll_delay=0.05) == 'fallback'
    await task
    await plain.aclose(close_connection_pool=True)


async def test_end_to_end_with_real_worker(fake_redis):
    """A burst Worker persists+publishes and a notification waiter receives it."""
    from arq import Worker, func

    _, redis = fake_redis

    async def add(ctx, x, y):
        return x + y

    job = await redis.enqueue_job('add', 2, 3)
    worker = Worker(
        [func(add, name='add')],
        redis_pool=redis,
        burst=True,
        poll_delay=0,
        max_jobs=1,
        job_completion_wait=0,
    )

    waiter = asyncio.create_task(job.result(timeout=10))
    await asyncio.sleep(0.1)
    await worker.main()
    assert await waiter == 5


async def test_end_to_end_worker_exception_wakes_waiter(fake_redis):
    from arq import Worker, func

    _, redis = fake_redis

    async def boom(ctx):
        raise RuntimeError('nope')

    job = await redis.enqueue_job('boom')
    worker = Worker([func(boom, name='boom')], redis_pool=redis, burst=True, poll_delay=0, max_jobs=1)
    waiter = asyncio.create_task(job.result(timeout=10))
    await asyncio.sleep(0.1)
    await worker.main()
    with pytest.raises(RuntimeError, match='nope'):
        await waiter
