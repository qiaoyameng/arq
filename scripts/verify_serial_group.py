"""
End-to-end verification of arq serial groups using TWO real arq.Worker
instances with max_jobs=10 (never 1) against the in-process mini-redis.

Scenarios:
  1. Interleaved groups: two different groups run in parallel while jobs inside
     each group run strictly one-at-a-time in enqueue order.
  2. Retry position: a job raising Retry(defer=...) stays head of its group;
     followers only start after the retry succeeds.
  3. Failure / abort unblock the group (no permanent stall).
  4. Runner disappears mid job: lock/in-progress TTL lets another worker recover
     the group head, and no member is ever started twice concurrently.

Run: .venv/bin/python scripts/verify_serial_group.py
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from arq.connections import ArqRedis
from arq.worker import Retry, RetryJob, Worker, func
from redis.asyncio.connection import ConnectionPool

from mini_redis import Store
from mini_redis_bridge import InMemoryConnection

# ---- test state ---------------------------------------------------------
events = []
events_lock = asyncio.Lock()
barrier_ready = asyncio.Event()
barrier_pass = asyncio.Event()
crash_ready = asyncio.Event()
crash_cancel = asyncio.Event()


async def record(redis, label):
    async with events_lock:
        events.append((time.monotonic(), label))


async def gate(ctx):
    redis = ctx['redis']
    await record(redis, f'{ctx["job_id"]}:start')
    barrier_ready.set()
    await barrier_pass.wait()
    await record(redis, f'{ctx["job_id"]}:end')
    return 'gate-done'


async def instant(ctx):
    redis = ctx['redis']
    await record(redis, f'{ctx["job_id"]}:start')
    await asyncio.sleep(0.05)
    await record(redis, f'{ctx["job_id"]}:end')
    return 'ok'


async def retry_once(ctx):
    redis = ctx['redis']
    await record(redis, f'{ctx["job_id"]}:try{ctx["job_try"]}')
    if ctx['job_try'] == 1:
        raise Retry(defer=0.5)
    await record(redis, f'{ctx["job_id"]}:done')
    return 'retried-ok'


async def always_fail(ctx):
    redis = ctx['redis']
    await record(redis, f'{ctx["job_id"]}:fail{ctx["job_try"]}')
    raise RetryJob('try again then give up')


async def crash_job(ctx):
    redis = ctx['redis']
    await record(redis, f'{ctx["job_id"]}:crash-start')
    crash_ready.set()
    await asyncio.Event().wait()  # killed hard while waiting here


async def crash_hard_job(ctx):
    """
    Simulate a worker process disappearing: the task is cancelled, but we
    neutralise both finish paths so lock/in-progress keys stay in redis.
    """
    redis = ctx['redis']
    worker = ctx.get('worker_obj')
    await record(redis, f'{ctx["job_id"]}:crash-start')
    crash_ready.set()
    if ctx['job_try'] == 1:
        # first run: neutralise every cleanup path, as if the process were kill -9'd
        async def _no_cleanup(*args, **kwargs):
            return None
        worker.finish_job = _no_cleanup  # type: ignore[assignment]
        worker.finish_failed_job = _no_cleanup  # type: ignore[assignment]
        worker._release_serial_lock = _no_cleanup  # type: ignore[assignment]
        await asyncio.Event().wait()
    # recovered run: block until the test cancels this task through normal
    # CancelledError handling, which retries the job (no defer) and completes
    await crash_cancel.wait()


FUNCTIONS = [
    func(gate, name='gate', max_tries=1),
    func(instant, name='instant'),
    func(retry_once, name='retry_once'),
    func(always_fail, name='always_fail', max_tries=2),
    func(crash_job, name='crash_job', max_tries=5),
    func(crash_hard_job, name='crash_hard_job', max_tries=5),
]


def make_redis(store):
    conn_cls = type('InMemoryConn', (InMemoryConnection,), {'store': store})
    pool = ConnectionPool(connection_class=conn_cls)
    return ArqRedis(connection_pool=pool)


def spawn_worker(redis, *, name, in_progress_timeout=None):
    w = Worker(
        FUNCTIONS,
        redis_pool=redis,
        burst=False,
        max_jobs=10,
        poll_delay=0.03,
        handle_signals=False,
        allow_abort_jobs=True,
        log_results=False,
        keep_result=60,
        ctx={'worker_name': name},
    )
    w.ctx['worker_obj'] = w
    if in_progress_timeout is not None:
        w.in_progress_timeout_s = in_progress_timeout
    task = asyncio.create_task(w.async_run())
    return w, task


def stop_worker(w, task):
    task.cancel()
    return w.close()


def labels(*needles):
    return [l for _, l in events if any(n in l for n in needles)]


# ---- scenarios ----------------------------------------------------------
async def scenario_interleaving(redis):
    """A1 blocks; B1/B2 run in parallel-ish order while A waits; then A2 after A1."""
    del events[:]
    barrier_ready.clear()
    barrier_pass.clear()
    await redis.delete('verify:events')

    w1, t1 = spawn_worker(redis, name='w1')
    w2, t2 = spawn_worker(redis, name='w2')
    try:
        await asyncio.sleep(0.1)
        await redis.enqueue_job('gate', _job_id='A1', _serial_group='A')
        # wait until A1 is running and holding group A's lock
        await asyncio.wait_for(barrier_ready.wait(), 3)
        # A2 is behind A1, B group is independent
        await redis.enqueue_job('instant', _job_id='B1', _serial_group='B')
        await redis.enqueue_job('instant', _job_id='B2', _serial_group='B')
        await redis.enqueue_job('instant', _job_id='A2', _serial_group='A')
        # B group must drain even though A1 is stuck
        await asyncio.wait_for(wait_for_labels(('B2:end',), count=1), timeout=3)
        assert labels('A2') == [], 'A2 must not start while A1 holds group A'
        assert labels('B1:start') and labels('B2:end'), 'group B should run independently'
        # release A1; A2 must follow
        barrier_pass.set()
        await asyncio.wait_for(wait_for_labels(('A2:end',), count=1), timeout=3)

        seq = labels('A1:start', 'A1:end', 'A2:start', 'A2:end', 'B1:start', 'B2:start')
        ia1e = index_of(seq, 'A1:end')
        ia2s = index_of(seq, 'A2:start')
        assert ia2s > ia1e, f'A2 must start after A1 ends: {seq}'
        # B work happens while A1 is blocked (proves parallelism across accounts)
        b_start = min(t for t, l in events if l in ('B1:start', 'B2:start'))
        a1_end = t_of(events, 'A1:end')
        assert b_start < a1_end, f'group B must progress while A1 blocked: {seq}'
        print(f'PASS interleaving: {seq}')
    finally:
        await stop_worker(w1, t1)
        await stop_worker(w2, t2)


async def scenario_retry_position(redis):
    """R1 deferred retry keeps head; R2 starts only after R1 done, order preserved."""
    del events[:]
    await redis.delete('verify:events')

    w1, t1 = spawn_worker(redis, name='w1')
    w2, t2 = spawn_worker(redis, name='w2')
    try:
        await asyncio.sleep(0.1)
        await redis.enqueue_job('retry_once', _job_id='R1', _serial_group='R')
        await redis.enqueue_job('instant', _job_id='R2', _serial_group='R')
        await asyncio.wait_for(wait_for_labels(('R2:end',), count=1), timeout=5)
        seq = labels('R1', 'R2')
        assert seq[:3] == ['R1:try1', 'R1:try2', 'R1:done'], seq
        assert index_of(seq, 'R2:start') > index_of(seq, 'R1:done'), seq
        # no duplicate starts: exactly one try1, one try2
        assert seq.count('R1:try1') == 1 and seq.count('R1:try2') == 1, seq
        print(f'PASS retry-position: {seq}')
    finally:
        await stop_worker(w1, t1)
        await stop_worker(w2, t2)


async def scenario_failure_and_abort(redis):
    """Failed head and aborted head both unblock followers; group never stalls."""
    del events[:]
    await redis.delete('verify:events')

    w1, t1 = spawn_worker(redis, name='w1')
    w2, t2 = spawn_worker(redis, name='w2')
    try:
        await asyncio.sleep(0.1)
        # failure path: F1 exhausts retries, F2 must still run
        await redis.enqueue_job('always_fail', _job_id='F1', _serial_group='F')
        await redis.enqueue_job('instant', _job_id='F2', _serial_group='F')
        await asyncio.wait_for(wait_for_labels(('F2:end',), count=1), timeout=5)
        assert labels('F1:fail1') and labels('F1:fail2'), labels('F1')
        assert index_of(labels('F1', 'F2'), 'F2:start') > index_of(
            labels('F1', 'F2'), 'F1:fail2'
        ), labels('F1', 'F2')
        print('PASS failure-unblock:', labels('F1', 'F2'))

        # abort path: aborting running X1 unblocks X2
        await _abort_scenario(redis, w1, w2)
    finally:
        await stop_worker(w1, t1)
        await stop_worker(w2, t2)


gate_ready_holder = None


async def abortable_gate(ctx):
    redis = ctx['redis']
    await record(redis, f'{ctx["job_id"]}:start')
    gate_ready_holder.set()
    await asyncio.Event().wait()


async def _abort_scenario(redis, w1, w2):
    from arq.jobs import Job
    del events[:]
    await redis.delete('verify:events')
    # register the extra function on both workers
    w1.functions['abortable_gate'] = func(abortable_gate, name='abortable_gate', max_tries=1)
    w2.functions['abortable_gate'] = func(abortable_gate, name='abortable_gate', max_tries=1)
    ready = asyncio.Event()
    global gate_ready_holder
    gate_ready_holder = ready
    await redis.enqueue_job('abortable_gate', _job_id='X1', _serial_group='X')
    await redis.enqueue_job('instant', _job_id='X2', _serial_group='X')
    await asyncio.wait_for(ready.wait(), 3)
    assert labels('X2') == [], 'X2 blocked behind running X1'
    job = Job('X1', redis)
    aborted = await job.abort(timeout=3)
    assert aborted, 'X1 should abort'
    await asyncio.wait_for(wait_for_labels(('X2:end',), count=1), timeout=3)
    assert labels('X2:start'), labels('X1', 'X2')
    print('PASS abort-unblock:', labels('X1', 'X2'))


async def scenario_runner_crash(redis):
    """Worker running group head is killed; TTL expiry lets the other worker recover."""
    del events[:]
    crash_ready.clear()
    crash_cancel.clear()
    await redis.delete('verify:events')

    w1, t1 = spawn_worker(redis, name='w1', in_progress_timeout=1.0)
    w2, t2 = spawn_worker(redis, name='w2', in_progress_timeout=1.0)
    try:
        await asyncio.sleep(0.1)
        await redis.enqueue_job('crash_hard_job', _job_id='C1', _serial_group='C')
        await redis.enqueue_job('instant', _job_id='C2', _serial_group='C')
        await asyncio.wait_for(crash_ready.wait(), 3)
        t0 = time.monotonic()
        # "process disappears": cancel the worker loop and all tasks abruptly.
        # crash_hard_job already neutralised every cleanup path, so the lock /
        # in-progress keys stay in redis exactly as they would after a kill -9.
        t1.cancel()
        for t in list(w1.tasks.values()):
            t.cancel()
        w1.allow_pick_jobs = False
        await asyncio.wait_for(wait_for_labels(('C1:crash-start',), count=2), timeout=5)
        recovered_at = time.monotonic()
        assert recovered_at - t0 >= 0.9, 'group must stay locked until TTL expires'
        assert labels('C1:crash-start').count('C1:crash-start') >= 2
        assert labels('C2') == [], 'C2 must never overtake the head'
        # release the recovered run so it completes; C2 must follow
        crash_cancel.set()
        await asyncio.wait_for(wait_for_labels(('C2:end',), count=1), timeout=5)
        seq = labels('C1', 'C2')
        assert index_of(seq, 'C2:start') > index_of(seq, 'C1:crash-start')
        print('PASS runner-crash-recovery: recovered after %.2fs, %s' % (
            recovered_at - t0, seq))
    finally:
        # w1 task already cancelled; close pools gracefully ignoring
        with_care(t1)
        await stop_worker(w2, t2)


def with_care(task):
    if not task.done():
        task.cancel()


# ---- helpers ------------------------------------------------------------
async def wait_for_labels(needles, count=1):
    """Wait until any label containing any needle appears count times."""
    for _ in range(3000):
        async with events_lock:
            hits = [l for _, l in events if any(n in l for n in needles)]
        if len(hits) >= count:
            return hits
        await asyncio.sleep(0.01)
    raise TimeoutError(f'labels {needles} not observed')


def index_of(seq, label):
    return seq.index(label)


def t_of(evs, label):
    return next(t for t, l in evs if l == label)


async def amain():
    store = Store()
    redis = make_redis(store)
    try:
        await scenario_interleaving(redis)
        await scenario_retry_position(redis)
        await scenario_failure_and_abort(redis)
        await scenario_runner_crash(redis)
        print('\nALL SERIAL-GROUP SCENARIOS PASSED')
    finally:
        await redis.close(close_connection_pool=True)


if __name__ == '__main__':
    asyncio.run(amain())
