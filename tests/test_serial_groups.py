import asyncio

import pytest

from arq import Worker, func
from arq.connections import ArqRedis
from arq.constants import (
    default_queue_name,
    serial_group_key_prefix,
    serial_group_lock_key_prefix,
    serial_group_member_key_prefix,
)
from arq.jobs import Job
from arq.worker import Retry

pytestmark = pytest.mark.asyncio


def group_key(name: str, queue: str = default_queue_name) -> str:
    return f'{serial_group_key_prefix}{queue}:{name}'


def lock_key(name: str, queue: str = default_queue_name) -> str:
    return f'{serial_group_lock_key_prefix}{queue}:{name}'


async def _lrange(redis: ArqRedis, key: str):
    length = await redis.llen(key)
    return [b.decode() for b in await redis.lrange(key, 0, length - 1)] if length else []


async def test_serial_group_enqueue_bookkeeping(arq_redis: ArqRedis):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='a', _serial_group='acct-1')
    j2 = await arq_redis.enqueue_job('foobar', _job_id='b', _serial_group='acct-1')
    j3 = await arq_redis.enqueue_job('foobar', _job_id='c')
    assert [j1.job_id, j2.job_id] == await _lrange(arq_redis, group_key('acct-1'))
    assert await arq_redis.get(serial_group_member_key_prefix + 'a') == b'acct-1'
    assert await arq_redis.get(serial_group_member_key_prefix + 'b') == b'acct-1'
    # jobs without a serial group leave no bookkeeping behind
    assert await arq_redis.get(serial_group_member_key_prefix + 'c') is None


async def test_serial_group_runs_in_order(arq_redis: ArqRedis, worker):
    order = []
    gate = asyncio.Event()

    async def step(ctx, label):
        order.append(('start', label))
        if label == 'first':
            await gate.wait()
        order.append(('end', label))
        return label

    await arq_redis.enqueue_job('step', 'first', _job_id='g1', _serial_group='acct')
    await arq_redis.enqueue_job('step', 'second', _job_id='g2', _serial_group='acct')
    await arq_redis.enqueue_job('step', 'third', _job_id='g3', _serial_group='acct')

    worker: Worker = worker(functions=[func(step, name='step')], max_jobs=10)
    task = asyncio.create_task(worker.main())

    # wait until "first" is running; the other two must not have started
    for _ in range(100):
        if ('start', 'first') in order:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)
    assert order == [('start', 'first')]
    # the head holds the group lock
    assert await arq_redis.get(lock_key('acct')) == b'g1'
    assert await _lrange(arq_redis, group_key('acct')) == ['g1', 'g2', 'g3']

    gate.set()
    await asyncio.wait_for(task, timeout=2)
    await worker.close()

    assert order == [
        ('start', 'first'),
        ('end', 'first'),
        ('start', 'second'),
        ('end', 'second'),
        ('start', 'third'),
        ('end', 'third'),
    ]
    assert worker.jobs_complete == 3
    # everything cleaned up once the group drained
    assert await _lrange(arq_redis, group_key('acct')) == []
    assert await arq_redis.get(lock_key('acct')) is None
    assert await arq_redis.get(serial_group_member_key_prefix + 'g2') is None


async def test_different_groups_run_in_parallel(arq_redis: ArqRedis, worker):
    running = set()
    max_parallel = 0
    lock = asyncio.Lock()

    async def slow(ctx, label):
        nonlocal max_parallel
        async with lock:
            running.add(label)
            max_parallel = max(max_parallel, len(running))
        await asyncio.sleep(0.1)
        async with lock:
            running.discard(label)

    for i in range(4):
        await arq_redis.enqueue_job('slow', f'job-{i}', _job_id=f'j{i}', _serial_group=f'acct-{i}')

    worker: Worker = worker(functions=[func(slow, name='slow')], max_jobs=10)
    await worker.main()
    await worker.close()
    assert max_parallel == 4
    assert worker.jobs_complete == 4


async def test_ungrouped_jobs_run_in_parallel_with_group(arq_redis: ArqRedis, worker):
    running = set()
    max_parallel = 0
    gate = asyncio.Event()
    lock = asyncio.Lock()

    async def slow(ctx, label, gated=False):
        nonlocal max_parallel
        async with lock:
            running.add(label)
            max_parallel = max(max_parallel, len(running))
        if gated:
            await gate.wait()
        await asyncio.sleep(0.05)
        async with lock:
            running.discard(label)

    # a long running head blocks its sibling, but nothing else
    await arq_redis.enqueue_job('slow', 'gated', True, _job_id='h1', _serial_group='g')
    await arq_redis.enqueue_job('slow', 'blocked', False, _job_id='h2', _serial_group='g')
    await arq_redis.enqueue_job('slow', 'free-1', False, _job_id='f1')
    await arq_redis.enqueue_job('slow', 'free-2', False, _job_id='f2')

    worker: Worker = worker(functions=[func(slow, name='slow')], max_jobs=10)
    task = asyncio.create_task(worker.main())
    for _ in range(100):
        if max_parallel == 3:
            break
        await asyncio.sleep(0.01)
    assert max_parallel == 3  # gated + free-1 + free-2, "blocked" waits
    gate.set()
    await asyncio.wait_for(task, timeout=2)
    await worker.close()
    assert worker.jobs_complete == 4


async def test_failed_job_releases_group_and_advances(arq_redis: ArqRedis, worker):
    order = []

    async def boom(ctx, label):
        order.append(label)
        if label == 'first':
            raise RuntimeError('boom')

    await arq_redis.enqueue_job('boom', 'first', _job_id='f1', _serial_group='acct')
    await arq_redis.enqueue_job('boom', 'second', _job_id='f2', _serial_group='acct')

    worker: Worker = worker(functions=[func(boom, name='boom')], max_jobs=10, retry_jobs=False)
    await worker.main()
    await worker.close()
    assert order == ['first', 'second']
    assert worker.jobs_failed == 1
    assert worker.jobs_complete == 1
    assert await _lrange(arq_redis, group_key('acct')) == []
    assert await arq_redis.get(lock_key('acct')) is None


async def test_retry_keeps_head_and_runs_again_after_delay(arq_redis: ArqRedis, worker, caplog):
    attempts = []

    async def flaky(ctx):
        attempts.append((ctx['job_id'], ctx['job_try']))
        if ctx['job_id'] == 'r1' and ctx['job_try'] == 1:
            raise Retry(defer=0.05)

    await arq_redis.enqueue_job('flaky', _job_id='r1', _serial_group='acct')
    await arq_redis.enqueue_job('flaky', _job_id='r2', _serial_group='acct')

    worker: Worker = worker(functions=[func(flaky, name='flaky')], max_jobs=10)
    await worker.main()
    await worker.close()

    assert attempts == [('r1', 1), ('r1', 2), ('r2', 1)]  # r1 retried before r2 starts
    assert worker.jobs_retried == 1
    assert worker.jobs_complete == 2
    assert await _lrange(arq_redis, group_key('acct')) == []
    assert await arq_redis.get(lock_key('acct')) is None


async def test_deferred_head_blocks_group_until_due(arq_redis: ArqRedis, worker):
    from datetime import datetime, timedelta, timezone

    order = []

    async def step(ctx, label):
        order.append(label)

    await arq_redis.enqueue_job(
        'step',
        'later',
        _job_id='d1',
        _serial_group='acct',
        _defer_until=datetime.now(tz=timezone.utc) + timedelta(milliseconds=300),
    )
    await arq_redis.enqueue_job('step', 'after', _job_id='d2', _serial_group='acct')

    worker: Worker = worker(functions=[func(step, name='step')], max_jobs=10, poll_delay=0.02)
    await worker.main()
    await worker.close()
    assert order == ['later', 'after']


async def test_serial_group_is_per_queue(arq_redis: ArqRedis):
    await arq_redis.enqueue_job('f', _job_id='x', _queue_name='q1', _serial_group='g')
    await arq_redis.enqueue_job('f', _job_id='y', _queue_name='q2', _serial_group='g')
    assert await _lrange(arq_redis, group_key('g', 'q1')) == ['x']
    assert await _lrange(arq_redis, group_key('g', 'q2')) == ['y']


async def test_jobdef_carries_serial_group(arq_redis: ArqRedis):
    await arq_redis.enqueue_job('foobar', _job_id='z', _serial_group='acct-7')
    [job] = await arq_redis.queued_jobs()
    assert job.serial_group == 'acct-7'


async def test_two_workers_share_groups(arq_redis: ArqRedis):
    """Two workers on the same Redis must never run two members of a group together."""
    gate = asyncio.Event()

    async def slow(ctx, group: str, gated: bool = False):
        # only one job per group may ever be active at the same time
        in_group = sum(1 for t in active_groups if t[0] == group)
        active_groups.append((group, ctx['job_id']))
        assert in_group == 0, f'two jobs of {group} running at once'
        if gated:
            await gate.wait()
        await asyncio.sleep(0.02)
        active_groups.remove((group, ctx['job_id']))

    active_groups: list[tuple[str, str]] = []

    jobs: list[tuple[str, str, bool]] = []
    for group in ('a', 'b', 'c'):
        for i in range(3):
            job_id = f'{group}{i}'
            gated = (group, i) == ('a', 0)
            jobs.append((group, job_id, gated))
            await arq_redis.enqueue_job('slow', group, gated, _job_id=job_id, _serial_group=group)

    worker1: Worker = Worker(
        functions=[func(slow, name='slow')],
        redis_pool=arq_redis,
        burst=False,
        poll_delay=0.01,
        max_jobs=10,
        handle_signals=False,
    )
    worker2: Worker = Worker(
        functions=[func(slow, name='slow')],
        redis_pool=arq_redis,
        burst=False,
        poll_delay=0.01,
        max_jobs=10,
        handle_signals=False,
    )
    t1 = asyncio.create_task(worker1.main())
    t2 = asyncio.create_task(worker2.main())

    for _ in range(200):
        if worker1.jobs_complete + worker2.jobs_complete >= 9:
            break
        await asyncio.sleep(0.02)
    gate.set()
    for _ in range(200):
        if worker1.jobs_complete + worker2.jobs_complete >= 9:
            break
        await asyncio.sleep(0.02)

    assert worker1.jobs_complete + worker2.jobs_complete == 9
    assert worker1.jobs_failed + worker2.jobs_failed == 0
    t1.cancel()
    t2.cancel()
    await asyncio.gather(t1, t2, return_exceptions=True)
    await worker1.close()
    await worker2.close()


async def test_group_unblocks_after_worker_disappears(arq_redis: ArqRedis):
    """
    Simulate a runner vanishing while holding the group: the group lock and the
    in-progress marker are short lived keys, so after they expire another worker
    can claim the head and the group drains instead of sticking forever.
    """
    order = []

    async def record(ctx, label):
        order.append(label)

    await arq_redis.enqueue_job('record', 'first', _job_id='k1', _serial_group='acct')
    await arq_redis.enqueue_job('record', 'second', _job_id='k2', _serial_group='acct')

    # pretend a dead runner claimed the head and left its keys behind
    await arq_redis.psetex('arq:in-progress:k1', 150, b'1')
    await arq_redis.psetex(lock_key('acct'), 150, b'k1')

    worker: Worker = Worker(
        functions=[func(record, name='record')],
        redis_pool=arq_redis,
        burst=True,
        poll_delay=0.02,
        max_jobs=10,
        job_timeout=0.1,
        handle_signals=False,
    )
    await asyncio.wait_for(worker.main(), timeout=5)
    await worker.close()
    assert order == ['first', 'second']
    assert await arq_redis.get(lock_key('acct')) is None


async def test_abort_group_head_advances_group(arq_redis: ArqRedis, worker):
    order = []

    async def longfunc(ctx, label):
        order.append(('start', label))
        if label == 'first':
            await asyncio.sleep(3600)
        order.append(('end', label))

    await arq_redis.enqueue_job('longfunc', 'first', _job_id='a1', _serial_group='acct')
    await arq_redis.enqueue_job('longfunc', 'second', _job_id='a2', _serial_group='acct')

    async def abort_later(job):
        await asyncio.sleep(0.15)
        assert await job.abort() is True

    worker: Worker = worker(functions=[func(longfunc, name='longfunc')], allow_abort_jobs=True, poll_delay=0.05)
    task = asyncio.create_task(worker.main())
    await asyncio.gather(abort_later(Job('a1', arq_redis)), task)
    await worker.main()

    assert ('start', 'first') in order
    assert ('start', 'second') in order
    assert ('end', 'second') in order
    assert worker.jobs_failed == 1
    assert worker.jobs_complete == 1
    assert await _lrange(arq_redis, group_key('acct')) == []
    assert await arq_redis.get(lock_key('acct')) is None
