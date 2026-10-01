"""Tests for the extended Job.abort cancellation interface.

These tests run against an in-process RESP server (``tests.mini_redis``) so
that completion and abort can be deterministically interleaved without docker.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from arq import AbortStatus, AbortTimeoutError
from arq.connections import ArqRedis
from arq.constants import abort_info_key_prefix
from arq.jobs import Job
from arq.worker import Worker, func

from .mini_redis import MiniRedis, patch_connection_for


@pytest.fixture
async def mini_redis():
    server = MiniRedis()
    patch_connection_for(server)
    redis = ArqRedis(host='127.0.0.1', port=1, encoding='utf8')
    await redis.flushall()
    yield redis, server
    await redis.aclose()


def make_worker(redis, functions, **kwargs):
    kwargs.setdefault('burst', True)
    kwargs.setdefault('poll_delay', 0.01)
    kwargs.setdefault('allow_abort_jobs', True)
    kwargs.setdefault('keep_result', 2)
    return Worker(functions=functions, redis_pool=redis, **kwargs)


async def test_abort_running_confirmed(mini_redis):
    redis, _ = mini_redis

    async def longfunc(ctx):
        await asyncio.sleep(3600)

    aborted = {}

    async def on_job_abort(ctx):
        aborted['id'] = ctx['abort_request_id']
        aborted['reason'] = ctx['abort_reason']

    job = await redis.enqueue_job('longfunc', _job_id='running')
    worker = make_worker(redis, [func(longfunc, name='longfunc')], on_job_abort=on_job_abort)
    worker_task = asyncio.create_task(worker.main())

    await asyncio.sleep(0.1)
    result = await job.abort(request_id='req-1', reason='no longer needed', timeout=5, poll_delay=0.02)
    await worker_task

    assert result.status is AbortStatus.aborted
    assert result.request_id == 'req-1'
    assert result.reason == 'no longer needed'
    assert bool(result) is True
    assert aborted == {'id': 'req-1', 'reason': 'no longer needed'}
    assert worker.jobs_failed == 1 and worker.jobs_complete == 0

    info = await job.result_info()
    assert info.success is False
    assert isinstance(info.result, asyncio.CancelledError)
    # reason is delivered to the cleanup hook, never smuggled into the business result
    assert getattr(info.result, 'reason', None) is None


async def test_abort_before_start(mini_redis):
    redis, _ = mini_redis

    async def longfunc(ctx):
        await asyncio.sleep(3600)

    job = await redis.enqueue_job('longfunc', _job_id='queued')
    registration = await job.abort(request_id='req-early', reason='cancelled before start', wait=False)
    assert registration.status is AbortStatus.unconfirmed
    assert registration.request_id == 'req-early'

    worker = make_worker(redis, [func(longfunc, name='longfunc')])
    await worker.main()

    receipt = await job.abort(request_id='req-later', reason='second request')
    assert receipt.status is AbortStatus.before_start
    # first effective request wins, the later one cannot overwrite it
    assert receipt.request_id == 'req-early'
    assert receipt.reason == 'cancelled before start'
    assert bool(receipt) is True

    info = await job.result_info()
    assert info.success is False
    assert isinstance(info.result, asyncio.CancelledError)


async def test_abort_deferred_before_start(mini_redis):
    redis, _ = mini_redis

    async def longfunc(ctx):
        await asyncio.sleep(3600)

    job = await redis.enqueue_job(
        'longfunc', _job_id='deferred', _defer_until=datetime.now(timezone.utc) + timedelta(days=1)
    )
    worker = make_worker(redis, [func(longfunc, name='longfunc')])
    worker_task = asyncio.create_task(worker.main())
    try:
        receipt = await job.abort(request_id='req-def', reason='never mind', timeout=3, poll_delay=0.02)
        assert receipt.status is AbortStatus.before_start
        assert receipt.request_id == 'req-def'
    finally:
        worker_task.cancel()


async def test_abort_already_finished_preserves_result(mini_redis):
    redis, _ = mini_redis

    async def shortfunc(ctx):
        return 'real result'

    job = await redis.enqueue_job('shortfunc', _job_id='finished')
    worker = make_worker(redis, [func(shortfunc, name='shortfunc')], allow_abort_jobs=False)
    await worker.main()

    receipt = await job.abort(request_id='req-late', reason='too late')
    assert receipt.status is AbortStatus.already_finished
    assert bool(receipt) is False

    info = await job.result_info()
    assert info.success is True
    assert info.result == 'real result'
    # no abort acknowledgement may be written for an untouched, finished job
    assert await redis.exists(abort_info_key_prefix + 'finished') == 0


async def test_abort_not_found(mini_redis):
    redis, _ = mini_redis
    receipt = await Job('missing', redis=redis).abort(request_id='req-x')
    assert receipt.status is AbortStatus.not_found
    assert bool(receipt) is False


async def test_abort_timeout_unconfirmed(mini_redis):
    redis, _ = mini_redis

    # no worker at all: the request stays registered but is never confirmed
    async def longfunc(ctx):
        await asyncio.sleep(3600)

    job = await redis.enqueue_job('longfunc', _job_id='lonely')
    with pytest.raises(AbortTimeoutError) as exc_info:
        await job.abort(request_id='req-wait', reason='nobody home', timeout=0.1, poll_delay=0.02)
    assert exc_info.value.request_id == 'req-wait'
    assert exc_info.value.reason == 'nobody home'
    # abort marker survives so a later worker can still honour the request
    assert await redis.zscore('arq:abort', 'lonely') is not None


async def test_multiple_aborts_first_reason_wins(mini_redis):
    redis, _ = mini_redis

    async def longfunc(ctx):
        await asyncio.sleep(3600)

    events = []

    async def on_job_abort(ctx):
        events.append((ctx['abort_request_id'], ctx['abort_reason']))

    job = await redis.enqueue_job('longfunc', _job_id='multi')
    worker = make_worker(redis, [func(longfunc, name='longfunc')], on_job_abort=on_job_abort)
    worker_task = asyncio.create_task(worker.main())
    await asyncio.sleep(0.1)

    first, second = await asyncio.gather(
        job.abort(request_id='r1', reason='first', timeout=5, poll_delay=0.01),
        job.abort(request_id='r2', reason='second', timeout=5, poll_delay=0.01),
    )
    await worker_task

    assert first.status is AbortStatus.aborted
    assert second.status is AbortStatus.aborted
    winning = first if first.request_id == 'r1' else second
    assert winning.request_id == 'r1'
    assert winning.reason == 'first'
    other = second if winning is first else first
    assert other.request_id == 'r1'
    assert other.reason == 'first'
    assert events == [('r1', 'first')]

    values = await redis.hmget(abort_info_key_prefix + 'multi', 'id', 'r', 'st')
    assert values == [b'r1', b'first', b'aborted']


async def test_abort_and_completion_race_result_wins(mini_redis):
    """Result write is held while abort is registered: the finished result wins."""
    redis, server = mini_redis

    async def shortfunc(ctx):
        await asyncio.sleep(0.1)
        return 'completed'

    # hold back the successful result commit (SET happens only in finish_job here)
    server.block_command('SET')

    job = await redis.enqueue_job('shortfunc', _job_id='race')
    worker = make_worker(redis, [func(shortfunc, name='shortfunc')])
    worker_task = asyncio.create_task(worker.main())

    # park the worker inside the finish pipeline, then register the abort request
    await server.wait_for_command('SET')
    registration = await job.abort(request_id='race-req', reason='late cancel', wait=False)
    assert registration.status is AbortStatus.unconfirmed

    # now let the already-computed successful result land
    server.release('SET')

    receipt = await job.abort(request_id='race-req-2', reason='still late', timeout=2, poll_delay=0.01)
    await worker_task

    assert receipt.status is AbortStatus.already_finished
    info = await job.result_info()
    assert info.success is True
    assert info.result == 'completed'

    values = await redis.hmget(abort_info_key_prefix + 'race', 'id', 'r', 'st')
    assert values[2] in (None, b''), values
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0


async def test_abort_and_completion_race_abort_wins(mini_redis):
    """Abort is registered and observed strictly before the result commits."""
    redis, server = mini_redis

    async def longfunc(ctx):
        await asyncio.sleep(3600)

    aborted = []

    async def on_job_abort(ctx):
        aborted.append(ctx['abort_reason'])

    job = await redis.enqueue_job('longfunc', _job_id='race-abort')
    worker = make_worker(redis, [func(longfunc, name='longfunc')], on_job_abort=on_job_abort)
    worker_task = asyncio.create_task(worker.main())
    await asyncio.sleep(0.1)

    # once the worker has scanned the abort zset, but before it cancels and
    # commits the cancellation receipt, is too late to control precisely;
    # instead block the receipt commit (HSETNX in finish_job)
    server.block_command('HSETNX')
    abort_task = asyncio.create_task(
        job.abort(request_id='race-ok', reason='confirmed first', timeout=2, poll_delay=0.01)
    )
    await server.wait_for_command('HSETNX')
    assert not abort_task.done()
    server.release('HSETNX')

    receipt = await abort_task
    await worker_task

    assert receipt.status is AbortStatus.aborted
    assert receipt.request_id == 'race-ok'
    assert receipt.reason == 'confirmed first'
    info = await job.result_info()
    assert info.success is False
    assert isinstance(info.result, asyncio.CancelledError)
    assert aborted == ['confirmed first']
    assert worker.jobs_failed == 1 and worker.jobs_complete == 0


async def test_receipt_ttl_matches_result_ttl(mini_redis):
    redis, _ = mini_redis

    async def longfunc(ctx):
        await asyncio.sleep(3600)

    job = await redis.enqueue_job('longfunc', _job_id='ttl')
    worker = make_worker(redis, [func(longfunc, name='longfunc')], keep_result=2)
    worker_task = asyncio.create_task(worker.main())
    await asyncio.sleep(0.1)
    await job.abort(request_id='ttl-req', reason='x', timeout=5, poll_delay=0.02)
    await worker_task

    result_ttl = await redis.pttl('arq:result:ttl')
    receipt_ttl = await redis.pttl(abort_info_key_prefix + 'ttl')
    assert 1500 < result_ttl <= 2000
    assert abs(result_ttl - receipt_ttl) < 200

    # both keys disappear together once the retention period passes
    deadline = time.monotonic() + 2.6
    while time.monotonic() < deadline:
        if await redis.exists('arq:result:ttl', abort_info_key_prefix + 'ttl') == 0:
            break
        await asyncio.sleep(0.05)
    assert await redis.exists('arq:result:ttl') == 0
    assert await redis.exists(abort_info_key_prefix + 'ttl') == 0


@pytest.fixture
async def mini_redis_msgpack():
    import functools

    import msgpack

    server = MiniRedis()
    patch_connection_for(server)
    redis = ArqRedis(
        host='127.0.0.1',
        port=1,
        encoding='utf8',
        job_serializer=msgpack.packb,
        job_deserializer=functools.partial(msgpack.unpackb, raw=False),
    )
    await redis.flushall()
    yield redis, server
    await redis.aclose()


async def test_abort_with_msgpack_serializer(mini_redis_msgpack):
    redis, _ = mini_redis_msgpack

    # the abort receipt lives in a separate hash with plain string fields, so it
    # is independent of the job serializer; a late abort on a finished job still
    # reports already_finished and keeps the msgpack business result intact
    async def shortfunc(ctx):
        return {'answer': 42}

    job = await redis.enqueue_job('shortfunc', _job_id='mp')
    worker = make_worker(
        redis,
        [func(shortfunc, name='shortfunc')],
        allow_abort_jobs=False,
        job_serializer=redis.job_serializer,
        job_deserializer=redis.job_deserializer,
    )
    await worker.main()

    receipt = await job.abort(request_id='mp-req', reason='too late')
    assert receipt.status is AbortStatus.already_finished
    info = await job.result_info()
    assert info.success is True
    assert info.result == {'answer': 42}
