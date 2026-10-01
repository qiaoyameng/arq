"""
End-to-end verification of arq serial groups using two workers and one
*independent local Redis* process.

Normal usage (real redis-server):
    redis-server --port 6399 --daemonize yes
    ARQ_TEST_REDIS_HOST=127.0.0.1 ARQ_TEST_REDIS_PORT=6399 \\
        python local-redis/verify_serial_groups.py

Sandboxed usage (no socket() syscall available): runs the same mini-redis
command engine in-process instead of as a TCP server.
    ARQ_TEST_NO_DOCKER=1 python local-redis/verify_serial_groups.py

Scenarios:
  1. interleaved groups: different accounts run in parallel, same account FIFO
  2. position after retry: the retried job keeps the head; sibling waits
  3. recovery after the runner exits: a dead worker's lock expires and the
     group is drained by a surviving worker (never stuck, never double start)
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from arq import Worker, func  # noqa: E402
from arq.connections import ArqRedis, RedisSettings, create_pool  # noqa: E402
from arq.constants import serial_group_key_prefix, serial_group_lock_key_prefix  # noqa: E402
from arq.worker import Retry  # noqa: E402


async def get_redis() -> ArqRedis:
    if os.getenv('ARQ_TEST_NO_DOCKER'):
        from inproc_redis import _make_pool

        return ArqRedis(connection_pool=_make_pool())
    host = os.getenv('ARQ_TEST_REDIS_HOST', '127.0.0.1')
    port = int(os.getenv('ARQ_TEST_REDIS_PORT', '6399'))
    return await create_pool(RedisSettings(host=host, port=port, conn_retries=20, conn_retry_delay=0.1))


def check(label: str, condition: bool, detail: str = '') -> None:
    status = 'PASS' if condition else 'FAIL'
    print(f'  [{status}] {label}' + (f'  ({detail})' if detail else ''))
    if not condition:
        raise SystemExit(f'scenario failed: {label}')


async def scenario_interleaving(redis: ArqRedis) -> None:
    print('1) interleaved groups: parallel across accounts, FIFO within account')
    order: list[str] = []
    events = asyncio.Event()
    active: list[str] = []

    async def job(ctx, account: str, gate: bool = False) -> str:
        active.append(account)
        if gate:
            await events.wait()
        order.append(account)
        active.remove(account)
        return account

    # account A head is gated; its sibling waits, accounts B and C run alongside
    await redis.enqueue_job('job', 'A', True, _job_id='a1', _serial_group='acct-A')
    await redis.enqueue_job('job', 'A', False, _job_id='a2', _serial_group='acct-A')
    await redis.enqueue_job('job', 'B', False, _job_id='b1', _serial_group='acct-B')
    await redis.enqueue_job('job', 'C', False, _job_id='c1', _serial_group='acct-C')

    worker = Worker(
        functions=[func(job, name='job')],
        redis_pool=redis,
        burst=False,
        poll_delay=0.01,
        max_jobs=10,
        handle_signals=False,
    )
    task = asyncio.create_task(worker.main())

    for _ in range(300):
        if 'a1' in order or len(order) >= 3:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)
    check('A head started but A sibling blocked', order == ['B', 'C'] or set(order) == {'B', 'C'})
    check('A sibling never runs while head active', 'A' not in order)
    events.set()
    for _ in range(300):
        if worker.jobs_complete == 4:
            break
        await asyncio.sleep(0.01)
    check('all four jobs completed', worker.jobs_complete == 4)
    check('A jobs ran in enqueue order', order.index('A') < len(order) and order[-1] == 'A')
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await worker.close()


async def scenario_retry_position(redis: ArqRedis) -> None:
    print('2) retry keeps position: head retries before the sibling starts')
    attempts: list[tuple[str, int]] = []

    async def flaky(ctx) -> None:
        attempts.append((ctx['job_id'], ctx['job_try']))
        if ctx['job_id'] == 'r1' and ctx['job_try'] == 1:
            raise Retry(defer=0.1)

    await redis.enqueue_job('flaky', _job_id='r1', _serial_group='acct-R')
    await redis.enqueue_job('flaky', _job_id='r2', _serial_group='acct-R')

    worker = Worker(
        functions=[func(flaky, name='flaky')],
        redis_pool=redis,
        burst=True,
        poll_delay=0.01,
        max_jobs=10,
        handle_signals=False,
    )
    await worker.main()
    check('r1 ran twice (retry) then r2 once', attempts == [('r1', 1), ('r1', 2), ('r2', 1)], str(attempts))
    check('no group lock left behind', await redis.get(f'{serial_group_lock_key_prefix}arq:queue:acct-R') is None)
    await worker.close()


async def scenario_runner_recovery(redis: ArqRedis) -> None:
    print('3) runner disappears: surviving worker drains the group after lock expiry')
    order: list[str] = []

    async def record(ctx, label: str) -> None:
        order.append(label)

    await redis.enqueue_job('record', 'one', _job_id='k1', _serial_group='acct-K')
    await redis.enqueue_job('record', 'two', _job_id='k2', _serial_group='acct-K')

    # simulate worker X dying right after claiming the head (SIGKILL mid job)
    await redis.psetex('arq:in-progress:k1', 200, b'1')
    await redis.psetex(f'{serial_group_lock_key_prefix}arq:queue:acct-K', 200, b'k1')
    check(
        'group is locked by the dead runner initially',
        await redis.get(f'{serial_group_lock_key_prefix}arq:queue:acct-K') == b'k1',
    )

    worker = Worker(
        functions=[func(record, name='record')],
        redis_pool=redis,
        burst=True,
        poll_delay=0.02,
        max_jobs=10,
        job_timeout=0.1,
        handle_signals=False,
    )
    await asyncio.wait_for(worker.main(), timeout=10)
    check('dead head re-ran then sibling ran', order == ['one', 'two'], str(order))
    check('group lock released', await redis.get(f'{serial_group_lock_key_prefix}arq:queue:acct-K') is None)
    await worker.close()


async def scenario_deferred_ordering(redis: ArqRedis) -> None:
    print('4) deferred head: whole group waits, then FIFO')
    order: list[str] = []

    async def step(ctx, label: str) -> None:
        order.append(label)

    await redis.enqueue_job(
        'step',
        'later',
        _job_id='d1',
        _serial_group='acct-D',
        _defer_until=datetime.now(tz=timezone.utc) + timedelta(milliseconds=200),
    )
    await redis.enqueue_job('step', 'after', _job_id='d2', _serial_group='acct-D')

    worker = Worker(
        functions=[func(step, name='step')],
        redis_pool=redis,
        burst=True,
        poll_delay=0.02,
        max_jobs=10,
        handle_signals=False,
    )
    await asyncio.wait_for(worker.main(), timeout=5)
    check('deferred head first, sibling after', order == ['later', 'after'], str(order))
    await worker.close()


async def main() -> None:
    redis = await get_redis()
    await redis.flushall()
    await scenario_interleaving(redis)
    await scenario_retry_position(redis)
    await scenario_runner_recovery(redis)
    await scenario_deferred_ordering(redis)
    print('\nAll serial-group scenarios verified.')
    await redis.aclose() if hasattr(redis, 'aclose') else await redis.close(close_connection_pool=True)


if __name__ == '__main__':
    asyncio.run(main())
