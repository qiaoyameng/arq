import asyncio
import pytest

from arq.constants import abort_jobs_ss, interval_jobs_zset_prefix
from arq.cron import IntervalJob, interval
from arq.worker import Worker


class FakeClock:
    def __init__(self, now_ms: int = 0):
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, seconds: float) -> None:
        self.now_ms += int(seconds * 1000)


@pytest.fixture
def fake_clock(mocker):
    clock = FakeClock()
    mocker.patch('arq.worker.timestamp_ms', clock)
    mocker.patch('arq.connections.timestamp_ms', clock)
    return clock


async def test_interval_requires_positive_interval():
    async def job(ctx):
        pass

    with pytest.raises(ValueError, match='interval must be greater than zero'):
        interval(job, interval=0)


def test_interval_creates_interval_job():
    async def job(ctx):
        pass

    scheduled = interval(
        job,
        interval=1.5,
        run_after_success=False,
        run_after_failure=False,
        run_after_cancellation=True,
    )

    assert isinstance(scheduled, IntervalJob)
    assert scheduled.interval_ms == 1500
    assert scheduled.run_after_success is False
    assert scheduled.run_after_failure is False
    assert scheduled.run_after_cancellation is True


def test_interval_jobs_are_separate_from_cron_jobs():
    async def maintenance(ctx):
        pass

    scheduled = interval(maintenance, interval=1)

    class Redis:
        default_queue_name = 'arq:queue'

        async def close(self, close_connection_pool=True):
            pass

    worker = Worker(
        redis_pool=Redis(),
        interval_jobs=[scheduled],
        handle_signals=False,
    )

    assert worker.cron_jobs == []
    assert worker.interval_jobs == [scheduled]
    assert worker.functions[scheduled.name] is scheduled


async def test_success_uses_actual_completion_and_does_not_catch_up(worker, arq_redis, fake_clock):
    started_at = []

    async def maintenance(ctx):
        started_at.append(fake_clock.now_ms)

    first_worker = worker(
        cron_jobs=[interval(maintenance, interval=0.1, run_at_startup=True)],
        poll_delay=0,
    )
    await first_worker.main()
    assert started_at == [0]
    schedule_key = interval_jobs_zset_prefix + first_worker.queue_name
    assert await arq_redis.zscore(schedule_key, 'interval:maintenance') == 100

    fake_clock.advance(1)

    second_worker = worker(
        cron_jobs=[interval(maintenance, interval=0.1, run_at_startup=True)],
        poll_delay=0,
    )
    await second_worker.main()

    assert started_at == [0, 1000]
    assert second_worker.jobs_complete == 1
    assert await arq_redis.zcard(second_worker.queue_name) == 0
    assert await arq_redis.zscore(schedule_key, 'interval:maintenance') == 1100


async def test_first_run_can_wait_one_interval(worker, arq_redis, fake_clock):
    async def maintenance(ctx):
        pass

    current_worker = worker(
        cron_jobs=[interval(maintenance, interval=0.1, run_at_startup=False)],
        poll_delay=0,
    )
    await current_worker.main()

    schedule_key = interval_jobs_zset_prefix + current_worker.queue_name
    assert current_worker.jobs_complete == 0
    assert await arq_redis.zscore(schedule_key, 'interval:maintenance') == 100

    fake_clock.advance(0.1)
    next_worker = worker(
        cron_jobs=[interval(maintenance, interval=0.1, run_at_startup=False)],
        poll_delay=0,
    )
    await next_worker.main()

    assert next_worker.jobs_complete == 1
    assert await arq_redis.zscore(schedule_key, 'interval:maintenance') == 200


async def test_failure_can_continue_from_failure_time(worker, arq_redis, fake_clock):
    attempts = []

    async def fails(ctx):
        attempts.append(fake_clock.now_ms)
        raise RuntimeError('boom')

    first_worker = worker(
        cron_jobs=[interval(fails, interval=0.1, run_at_startup=True, run_after_failure=True)],
        poll_delay=0,
    )
    await first_worker.main()
    fake_clock.advance(1)

    second_worker = worker(
        cron_jobs=[interval(fails, interval=0.1, run_at_startup=True, run_after_failure=True)],
        poll_delay=0,
    )
    await second_worker.main()

    schedule_key = interval_jobs_zset_prefix + second_worker.queue_name
    assert attempts == [0, 1000]
    assert second_worker.jobs_failed == 1
    assert await arq_redis.zscore(schedule_key, 'interval:fails') == 1100


async def test_failure_can_stop_schedule(worker, arq_redis, fake_clock):
    async def fails(ctx):
        raise RuntimeError('boom')

    first_worker = worker(
        cron_jobs=[interval(fails, interval=0.1, run_at_startup=True, run_after_failure=False)],
        poll_delay=0,
    )
    await first_worker.main()
    fake_clock.advance(1)

    schedule_key = interval_jobs_zset_prefix + first_worker.queue_name
    assert await arq_redis.zscore(schedule_key, 'interval:fails') == -1

    second_worker = worker(
        cron_jobs=[interval(fails, interval=0.1, run_at_startup=True, run_after_failure=False)],
        poll_delay=0,
    )
    await second_worker.main()

    assert second_worker.jobs_complete == 0
    assert second_worker.jobs_failed == 0
    assert await arq_redis.zcard(second_worker.queue_name) == 0
    assert await arq_redis.zscore(schedule_key, 'interval:fails') == -1


@pytest.mark.parametrize('run_after_cancellation,expected_score', [(True, 350), (False, -1)])
async def test_cancellation_policy(worker, arq_redis, fake_clock, run_after_cancellation, expected_score):
    started = asyncio.Event()

    async def cancelled(ctx):
        started.set()
        await asyncio.Event().wait()

    schedule_job = interval(
        cancelled,
        name='interval:cancelled',
        interval=0.1,
        run_at_startup=True,
        run_after_cancellation=run_after_cancellation,
    )
    current_worker = worker(
        cron_jobs=[schedule_job],
        burst=False,
        poll_delay=0,
        allow_abort_jobs=True,
        retry_jobs=False,
    )
    await current_worker.initialize_interval_jobs()
    await current_worker.run_intervals()
    job_id = f'interval:cancelled:{fake_clock.now_ms}'
    main_task = asyncio.create_task(current_worker.main())

    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        fake_clock.advance(0.25)
        await arq_redis.zadd(abort_jobs_ss, {job_id: fake_clock.now_ms})
        await asyncio.sleep(0.05)

        assert current_worker.jobs_failed == 1
    finally:
        main_task.cancel()
        await asyncio.gather(main_task, return_exceptions=True)

    schedule_key = interval_jobs_zset_prefix + current_worker.queue_name
    assert await arq_redis.zscore(schedule_key, 'interval:cancelled') == expected_score


async def test_long_job_deduplicates_workers_and_continues_on_another_worker(arq_redis, fake_clock):
    started = asyncio.Event()
    completed = asyncio.Event()
    release = asyncio.Event()
    starts = 0

    async def long_maintenance(ctx):
        nonlocal starts
        starts += 1
        started.set()
        await release.wait()
        completed.set()

    schedule = interval(long_maintenance, interval=0.1, run_at_startup=True)
    first_worker = Worker(
        redis_pool=arq_redis,
        burst=False,
        poll_delay=0,
        cron_jobs=[schedule],
        handle_signals=False,
    )
    second_worker = Worker(
        redis_pool=arq_redis,
        burst=False,
        poll_delay=0,
        cron_jobs=[interval(long_maintenance, interval=0.1, run_at_startup=True)],
        handle_signals=False,
    )
    first_main = asyncio.create_task(first_worker.main())

    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        fake_clock.advance(600)
        await second_worker.initialize_interval_jobs()
        await asyncio.gather(first_worker.run_intervals(), second_worker.run_intervals())

        assert starts == 1
        assert await arq_redis.zcard(first_worker.queue_name) == 1

        release.set()
        await asyncio.wait_for(completed.wait(), timeout=1)
        await asyncio.sleep(0.05)
        assert first_worker.jobs_complete == 1
    finally:
        first_main.cancel()
        await asyncio.gather(first_main, return_exceptions=True)

    schedule_key = interval_jobs_zset_prefix + first_worker.queue_name
    assert await arq_redis.zscore(schedule_key, 'interval:long_maintenance') == 600100
    assert await arq_redis.zcard(first_worker.queue_name) == 0

    fake_clock.advance(0.1)
    third_worker = Worker(
        redis_pool=arq_redis,
        burst=True,
        poll_delay=0,
        interval_jobs=[interval(long_maintenance, interval=0.1, run_at_startup=True)],
        handle_signals=False,
    )
    await third_worker.main()

    assert starts == 2
    assert third_worker.jobs_complete == 1
    assert await arq_redis.zscore(schedule_key, 'interval:long_maintenance') == 600200
