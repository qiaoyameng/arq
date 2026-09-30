import asyncio

from arq import Worker, func
from arq.connections import ArqRedis
from arq.constants import in_progress_key_prefix, job_key_prefix, retry_key_prefix
from arq.maintenance import (
    MaintenanceStatus,
    MaintenancePlan,
    apply_maintenance_plan,
    inspect_jobs,
    make_maintenance_plan,
)


async def _status_by_id(report):
    return {job.job_id: job for job in report.jobs}


async def test_inspect_classifies_residue_and_safe_states(arq_redis: ArqRedis):
    queue = 'arq:queue'
    await arq_redis.zadd(queue, {'missing': 1, 'deferred': 10_000_000_000_000})
    await arq_redis.psetex(job_key_prefix + 'deferred', 10_000, b'payload')

    await arq_redis.psetex(job_key_prefix + 'orphan-payload', 10_000, b'payload')
    await arq_redis.setex(retry_key_prefix + 'orphan-retry', 10, b'1')
    await arq_redis.psetex(in_progress_key_prefix + 'running', 10_000, b'1')
    await arq_redis.psetex(job_key_prefix + 'running', 10_000, b'payload')
    await arq_redis.zadd(queue, {'running': 1})

    await arq_redis.setex(retry_key_prefix + 'retrying', 10, b'1')
    await arq_redis.psetex(job_key_prefix + 'retrying', 10_000, b'payload')
    await arq_redis.zadd(queue, {'retrying': 10_000_000_000_000})

    await arq_redis.psetex(job_key_prefix + 'unknown-payload', 10_000, b'payload')
    await arq_redis.persist(job_key_prefix + 'unknown-payload')

    report = await inspect_jobs(arq_redis)
    jobs = await _status_by_id(report)

    assert jobs['missing'].status == MaintenanceStatus.orphan_queue_member
    assert jobs['orphan-payload'].status == MaintenanceStatus.orphan_payload
    assert jobs['orphan-retry'].status == MaintenanceStatus.orphan_retry
    assert jobs['deferred'].status == MaintenanceStatus.deferred
    assert jobs['running'].status == MaintenanceStatus.in_progress
    assert jobs['retrying'].status == MaintenanceStatus.retrying
    assert jobs['unknown-payload'].status == MaintenanceStatus.unknown


async def test_apply_plan_is_limited_idempotent_and_prefix_scoped(arq_redis: ArqRedis):
    queue = 'arq:queue'
    await arq_redis.zadd(queue, {'missing': 1, 'second': 2})
    await arq_redis.psetex(job_key_prefix + 'orphan-payload', 10_000, b'payload')
    await arq_redis.setex(retry_key_prefix + 'orphan-retry', 10, b'1')
    await arq_redis.set('unrelated:key', 'keep')
    await arq_redis.set('other-prefix:unrelated-like', 'keep')

    report = await inspect_jobs(arq_redis)
    plan = make_maintenance_plan(report, max_actions=2)
    assert [action.job_id for action in plan.actions] == ['missing', 'orphan-payload']

    result = await apply_maintenance_plan(arq_redis, plan)
    assert len(result.applied) == 2
    assert result.skipped == []
    assert result.failed == []

    second_result = await apply_maintenance_plan(arq_redis, plan)
    assert second_result.applied == []
    assert {item.reason for item in second_result.skipped} == {
        'queue member already absent',
        'payload already absent, expiry changed, or payload changed',
    }

    assert await arq_redis.zscore(queue, 'second') == 2
    assert await arq_redis.get(retry_key_prefix + 'orphan-retry') == b'1'
    assert await arq_redis.get('unrelated:key') == b'keep'
    assert await arq_redis.get('other-prefix:unrelated-like') == b'keep'


async def test_apply_rechecks_lease_and_state_changes(arq_redis: ArqRedis):
    queue = 'arq:queue'
    await arq_redis.zadd(queue, {'missing': 1})
    await arq_redis.psetex(job_key_prefix + 'orphan-payload', 10_000, b'payload')
    plan = make_maintenance_plan(await inspect_jobs(arq_redis))

    await arq_redis.psetex(in_progress_key_prefix + 'missing', 10_000, b'1')
    await arq_redis.zadd(queue, {'orphan-payload': 1})

    result = await apply_maintenance_plan(arq_redis, plan)

    assert result.applied == []
    assert {(item.job_id, item.reason) for item in result.skipped} == {
        ('missing', 'unexpired execution lease exists'),
        ('orphan-payload', 'queue reference appeared after plan was generated'),
    }
    assert await arq_redis.zscore(queue, 'missing') == 1
    assert await arq_redis.get(in_progress_key_prefix + 'missing') == b'1'
    assert await arq_redis.get(job_key_prefix + 'orphan-payload') == b'payload'


async def test_apply_rechecks_payload_value_and_queue_score(arq_redis: ArqRedis):
    queue = 'arq:queue'
    await arq_redis.zadd(queue, {'queued-orphan': 1})
    await arq_redis.psetex(job_key_prefix + 'changed-payload', 10_000, b'old')
    plan = make_maintenance_plan(await inspect_jobs(arq_redis))

    await arq_redis.psetex(job_key_prefix + 'changed-payload', 10_000, b'new')
    await arq_redis.psetex(job_key_prefix + 'queued-orphan', 10_000, b'payload')

    result = await apply_maintenance_plan(arq_redis, plan)

    assert result.applied == []
    assert {item.reason for item in result.skipped} == {
        'payload changed after plan was generated',
        'payload appeared after plan was generated',
    }


async def test_scan_allows_worker_and_does_not_plan_running_job(arq_redis: ArqRedis, worker):
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_job(ctx):
        started.set()
        await release.wait()

    worker_instance: Worker = worker(functions=[func(slow_job, name='slow_job', keep_result=0)], burst=False)
    task = asyncio.create_task(worker_instance.main())
    try:
        await arq_redis.enqueue_job('slow_job', _job_id='running')
        await asyncio.wait_for(started.wait(), timeout=2)

        report = await inspect_jobs(arq_redis)
        jobs = await _status_by_id(report)
        assert jobs['running'].status == MaintenanceStatus.in_progress
        assert make_maintenance_plan(report).actions == ()
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_retry_deferral_is_not_orphan(arq_redis: ArqRedis):
    await arq_redis.setex(retry_key_prefix + 'retry-job', 10, b'1')
    await arq_redis.psetex(job_key_prefix + 'retry-job', 10_000, b'payload')
    await arq_redis.zadd('arq:queue', {'retry-job': 10_000_000_000_000})

    report = await inspect_jobs(arq_redis)
    job = next(job for job in report.jobs if job.job_id == 'retry-job')

    assert job.status == MaintenanceStatus.retrying
    assert make_maintenance_plan(report).actions == ()


async def test_unexpired_malformed_lease_is_protected(arq_redis: ArqRedis):
    await arq_redis.zadd('arq:queue', {'weird-lease': 1})
    await arq_redis.psetex(in_progress_key_prefix + 'weird-lease', 10_000, b'not-a-standard-lease')

    report = await inspect_jobs(arq_redis)
    job = next(job for job in report.jobs if job.job_id == 'weird-lease')

    assert job.status == MaintenanceStatus.in_progress
    assert make_maintenance_plan(report).actions == ()


async def test_plan_json_roundtrip(arq_redis: ArqRedis):
    await arq_redis.zadd('arq:queue', {'missing': 1})
    plan = make_maintenance_plan(await inspect_jobs(arq_redis))

    restored = MaintenancePlan.from_dict(plan.to_dict())

    assert restored == plan
