import asyncio
from datetime import datetime, timedelta

import pytest

from arq.connections import serial_group_key
from arq.constants import serial_group_key_prefix
from arq.jobs import deserialize_job
from arq.worker import Retry, RetryJob, Worker, func

from .test_worker import foobar

order: list[str] = []
block_first = asyncio.Event()
proceed = asyncio.Event()


async def serial_gate(ctx):
    order.append(f'{ctx["job_id"]}:start')
    block_first.set()
    await proceed.wait()
    order.append(f'{ctx["job_id"]}:end')
    return True


async def serial_instant(ctx):
    order.append(f'{ctx["job_id"]}:start')
    await asyncio.sleep(0.02)
    order.append(f'{ctx["job_id"]}:end')
    return True


async def serial_retry_once(ctx):
    order.append(f'try{ctx["job_try"]}')
    if ctx['job_try'] == 1:
        raise Retry(defer=0.3)
    order.append('done')
    return True


async def fail_then_ok(ctx):
    order.append(f'fail-{ctx["job_try"]}')
    if ctx['job_try'] < 2:
        raise RetryJob()
    return 'ok'


def make_worker(arq_redis):
    return Worker(
        [
            func(serial_gate, name='serial_gate'),
            func(serial_instant, name='serial_instant'),
            func(serial_retry_once, name='serial_retry_once'),
            func(fail_then_ok, name='fail_then_ok'),
            func(foobar, name='foobar'),
        ],
        redis_pool=arq_redis,
        burst=False,
        max_jobs=10,
        poll_delay=0.02,
        handle_signals=False,
    )


@pytest.fixture
async def two_workers(arq_redis):
    w1 = make_worker(arq_redis)
    w2 = make_worker(arq_redis)
    t1 = asyncio.create_task(w1.async_run())
    t2 = asyncio.create_task(w2.async_run())
    await asyncio.sleep(0.05)
    yield w1, w2
    t1.cancel()
    t2.cancel()
    await w1.close()
    await w2.close()


async def test_enqueue_creates_group_keys(arq_redis):
    await arq_redis.enqueue_job('foobar', _job_id='j1', _serial_group='acct-1')
    gk = serial_group_key(arq_redis.default_queue_name, 'acct-1')
    assert gk.startswith(serial_group_key_prefix)
    assert await arq_redis.zrange(gk, 0, -1) == [b'j1']
    raw = await arq_redis.get('arq:job:j1')
    assert deserialize_job(raw).serial_group == 'acct-1'

    # a job without a serial group creates no new group entry (only the
    # acct-1 group zset exists; the :seq counter key shares its prefix)
    await arq_redis.enqueue_job('foobar', _job_id='j2')
    assert set(await arq_redis.keys(serial_group_key_prefix + '*')) == {
        gk.encode(),
        (gk + ':seq').encode(),
    }


async def test_serial_group_runs_in_enqueue_order_and_parallel_across_groups(
    two_workers, arq_redis
):
    order.clear()
    block_first.clear()
    proceed.clear()

    await arq_redis.enqueue_job('serial_gate', _job_id='A1', _serial_group='A')
    await asyncio.wait_for(block_first.wait(), 3)
    await arq_redis.enqueue_job('serial_instant', _job_id='A2', _serial_group='A')
    await arq_redis.enqueue_job('serial_instant', _job_id='B1', _serial_group='B')
    await arq_redis.enqueue_job('serial_instant', _job_id='B2', _serial_group='B')

    for _ in range(100):
        if 'B2:end' in order:
            break
        await asyncio.sleep(0.03)
    assert 'B2:end' in order
    assert 'A2:start' not in order
    # independent group B made progress while A1 is still blocked
    assert 'A1:end' not in order

    proceed.set()
    for _ in range(100):
        if 'A2:end' in order:
            break
        await asyncio.sleep(0.03)
    assert 'A1:end' in order
    assert order.index('A1:end') < order.index('A2:start')
    assert order.index('B1:end') < order.index('B2:start')


async def test_deferred_retry_keeps_group_head(two_workers, arq_redis):
    order.clear()
    await arq_redis.enqueue_job('serial_retry_once', _job_id='R1', _serial_group='R')
    await arq_redis.enqueue_job('serial_instant', _job_id='R2', _serial_group='R')
    for _ in range(150):
        if 'R2:end' in order:
            break
        await asyncio.sleep(0.03)
    assert order == ['try1', 'try2', 'done', 'R2:start', 'R2:end']


async def test_deferred_head_blocks_followers(two_workers, arq_redis):
    order.clear()
    await arq_redis.enqueue_job(
        'serial_instant',
        _job_id='D1',
        _serial_group='D',
        _defer_until=datetime.now() + timedelta(seconds=0.8),
    )
    await arq_redis.enqueue_job('serial_instant', _job_id='D2', _serial_group='D')
    await arq_redis.enqueue_job('serial_instant', _job_id='E1', _serial_group='E')

    for _ in range(80):
        if 'E1:end' in order:
            break
        await asyncio.sleep(0.03)
    assert 'D1:start' not in order
    assert 'D2:start' not in order

    for _ in range(150):
        if 'D2:end' in order:
            break
        await asyncio.sleep(0.03)
    assert order.index('D1:end') < order.index('D2:start')
    assert order.index('E1:start') < order.index('D1:start')


async def test_failed_head_unblocks_group(two_workers, arq_redis):
    order.clear()
    await arq_redis.enqueue_job('fail_then_ok', _job_id='F1', _serial_group='F')
    await arq_redis.enqueue_job('serial_instant', _job_id='F2', _serial_group='F')
    for _ in range(150):
        if 'F2:end' in order:
            break
        await asyncio.sleep(0.03)
    assert order.index('F2:start') > order.index('fail-2')

    gk = serial_group_key(arq_redis.default_queue_name, 'F')
    for _ in range(50):
        if await arq_redis.zcard(gk) == 0:
            break
        await asyncio.sleep(0.03)
    assert await arq_redis.zcard(gk) == 0


async def test_non_serial_jobs_unaffected(arq_redis, worker):
    done = []

    async def quick(ctx):
        done.append(ctx['job_id'])

    w = worker(functions=[func(quick, name='quick')], burst=True, poll_delay=0)
    await arq_redis.enqueue_job('quick', _job_id='p1')
    await arq_redis.enqueue_job('quick', _job_id='p2')
    await w.main()
    assert sorted(done) == ['p1', 'p2']
