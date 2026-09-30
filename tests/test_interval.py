import asyncio
from datetime import timedelta

import pytest

from arq import Worker, interval
from arq.constants import interval_state_key_prefix

fake_clock = None
long_job_release = None


class FakeClock:
    def __init__(self, current_ms: int):
        self.current_ms = current_ms

    def timestamp_ms(self) -> int:
        return self.current_ms

    def advance(self, milliseconds: int) -> None:
        self.current_ms += milliseconds


async def local_interval_job(ctx):
    return 'ok'


async def variable_duration_interval_job(ctx):
    fake_clock.advance(4_000)
    return 'ok'


async def failing_interval_job(ctx):
    raise RuntimeError('boom')


async def long_interval_job(ctx):
    await long_job_release.wait()


async def cancelled_once(ctx):
    if ctx['job_try'] == 1:
        raise asyncio.CancelledError()
    return 'ok'


def test_interval_validates_interval():
    with pytest.raises(ValueError):
        interval(local_interval_job, interval=0)
    assert interval(local_interval_job, seconds=1).interval_ms == 1000
    with pytest.raises(ValueError):
        interval(local_interval_job, interval=1, seconds=1)


async def test_success_waits_for_actual_completion(worker, arq_redis, mocker):
    global fake_clock
    clock = FakeClock(1_000)
    fake_clock = clock
    mocker.patch('arq.worker.timestamp_ms', clock.timestamp_ms)
    mocker.patch('arq.connections.timestamp_ms', clock.timestamp_ms)

    scheduled = interval(
        variable_duration_interval_job,
        interval=timedelta(seconds=10),
        run_at_startup=True,
    )
    worker_: Worker = worker(cron_jobs=[scheduled], poll_delay=0, retry_jobs=False)
    await worker_.main()
    assert worker_.jobs_complete == 1

    state_key = f'{interval_state_key_prefix}{worker_.queue_name}:{scheduled.name}'
    state = await arq_redis.hgetall(state_key)
    assert state[b'status'] == b'idle'
    assert int(state[b'next_run']) == 15_000
    assert state[b'last_outcome'] == b'success'
    assert state[b'claimed'] == b'0'
    fake_clock = None


async def test_failure_policy(worker, arq_redis, mocker):
    clock = FakeClock(2_000)
    mocker.patch('arq.worker.timestamp_ms', clock.timestamp_ms)
    mocker.patch('arq.connections.timestamp_ms', clock.timestamp_ms)

    stopped = interval(failing_interval_job, interval=timedelta(seconds=10), run_at_startup=True)
    worker_: Worker = worker(cron_jobs=[stopped], poll_delay=0, retry_jobs=False)
    await worker_.main()

    stopped_key = f'{interval_state_key_prefix}{worker_.queue_name}:{stopped.name}'
    stopped_state = await arq_redis.hgetall(stopped_key)
    assert stopped_state[b'status'] == b'stopped'
    assert stopped_state[b'next_run'] == b''
    assert stopped_state[b'last_outcome'] == b'failure'

    await arq_redis.flushall()
    clock.advance(5_000)
    continuing = interval(
        failing_interval_job,
        name='failing:continue',
        interval=timedelta(seconds=10),
        run_at_startup=True,
        continue_on_failure=True,
    )
    worker_ = worker(cron_jobs=[continuing], poll_delay=0, retry_jobs=False)
    await worker_.main()

    continuing_key = f'{interval_state_key_prefix}{worker_.queue_name}:{continuing.name}'
    continuing_state = await arq_redis.hgetall(continuing_key)
    assert continuing_state[b'status'] == b'idle'
    assert int(continuing_state[b'next_run']) == 17_000
    assert continuing_state[b'last_outcome'] == b'failure'


async def test_cancel_policy(worker, arq_redis, mocker):
    clock = FakeClock(3_000)
    mocker.patch('arq.worker.timestamp_ms', clock.timestamp_ms)
    mocker.patch('arq.connections.timestamp_ms', clock.timestamp_ms)

    scheduled = interval(
        cancelled_once,
        interval=timedelta(seconds=10),
        run_at_startup=True,
        continue_on_cancel=True,
        max_tries=1,
    )
    worker_: Worker = worker(cron_jobs=[scheduled], poll_delay=0, retry_jobs=False)
    await worker_.main()

    state_key = f'{interval_state_key_prefix}{worker_.queue_name}:{scheduled.name}'
    state = await arq_redis.hgetall(state_key)
    assert state[b'status'] == b'idle'
    assert int(state[b'next_run']) == 13_000
    assert state[b'last_outcome'] == b'cancel'


async def wait_until(condition):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 1
    while not condition():
        if loop.time() > deadline:
            raise AssertionError('timed out waiting for condition')
        await asyncio.sleep(0.01)


async def test_long_running_job_does_not_accumulate_runs(worker, arq_redis, mocker):
    global long_job_release
    release = long_job_release = asyncio.Event()

    clock = FakeClock(10_000)
    mocker.patch('arq.worker.timestamp_ms', clock.timestamp_ms)
    mocker.patch('arq.connections.timestamp_ms', clock.timestamp_ms)

    scheduled = interval(long_interval_job, interval=timedelta(seconds=1), run_at_startup=True)
    worker_: Worker = worker(cron_jobs=[scheduled], poll_delay=0, burst=False, handle_signals=False)
    worker_.main_task = worker_.loop.create_task(worker_.main())
    await wait_until(lambda: len(worker_.tasks) == 1)
    state_key = f'{interval_state_key_prefix}{worker_.queue_name}:{scheduled.name}'
    running_state = await arq_redis.hgetall(state_key)
    assert running_state[b'status'] == b'running'

    clock.advance(10_000)
    await asyncio.sleep(0.05)
    queued = await arq_redis.queued_jobs(queue_name=worker_.queue_name)
    assert queued == []
    running_state = await arq_redis.hgetall(state_key)
    assert running_state[b'run_id'] == b'10000'

    release.set()
    await asyncio.wait_for(asyncio.gather(*worker_.tasks.values()), timeout=1)
    if worker_.main_task and not worker_.main_task.done():
        worker_.main_task.cancel()
    await worker_.close()

    state = await arq_redis.hgetall(state_key)
    assert state[b'status'] == b'idle'
    assert int(state[b'next_run']) == 21_000


async def test_state_survives_worker_restart(worker, arq_redis, mocker):
    clock = FakeClock(5_000)
    mocker.patch('arq.worker.timestamp_ms', clock.timestamp_ms)
    mocker.patch('arq.connections.timestamp_ms', clock.timestamp_ms)

    scheduled = interval(local_interval_job, interval=timedelta(seconds=10), run_at_startup=False)
    first_worker: Worker = worker(cron_jobs=[scheduled], poll_delay=0)
    await first_worker.initialize_interval_jobs()

    clock.advance(30_000)
    second_worker: Worker = worker(cron_jobs=[scheduled], poll_delay=0, retry_jobs=False)
    await second_worker.main()

    state_key = f'{interval_state_key_prefix}{second_worker.queue_name}:{scheduled.name}'
    state = await arq_redis.hgetall(state_key)
    assert state[b'status'] == b'idle'
    assert int(state[b'next_run']) == 45_000


async def test_due_interval_is_enqueued_once_across_workers(worker, arq_redis, mocker):
    clock = FakeClock(6_000)
    mocker.patch('arq.worker.timestamp_ms', clock.timestamp_ms)
    mocker.patch('arq.connections.timestamp_ms', clock.timestamp_ms)

    scheduled = interval(local_interval_job, interval=timedelta(seconds=1), run_at_startup=False)
    first_worker: Worker = worker(cron_jobs=[scheduled], poll_delay=0, burst=False, handle_signals=False)
    second_worker: Worker = worker(cron_jobs=[scheduled], poll_delay=0, burst=False, handle_signals=False)
    await asyncio.gather(first_worker.initialize_interval_jobs(), second_worker.initialize_interval_jobs())
    await asyncio.gather(first_worker.run_intervals(), second_worker.run_intervals())
    queued = await arq_redis.queued_jobs(queue_name=first_worker.queue_name)
    assert len(queued) == 1


async def test_other_worker_runs_enqueued_interval(worker, arq_redis, mocker):
    clock = FakeClock(7_000)
    mocker.patch('arq.worker.timestamp_ms', clock.timestamp_ms)
    mocker.patch('arq.connections.timestamp_ms', clock.timestamp_ms)

    scheduled = interval(local_interval_job, interval=timedelta(seconds=10), run_at_startup=True)
    scheduling_worker: Worker = worker(cron_jobs=[scheduled], poll_delay=0, burst=False, handle_signals=False)
    executing_worker: Worker = worker(cron_jobs=[scheduled], poll_delay=0, retry_jobs=False)
    await scheduling_worker.initialize_interval_jobs()
    await scheduling_worker.run_intervals()
    await executing_worker.main()

    state_key = f'{interval_state_key_prefix}{executing_worker.queue_name}:{scheduled.name}'
    state = await arq_redis.hgetall(state_key)
    assert state[b'status'] == b'idle'
    assert int(state[b'next_run']) == 17_000
