import asyncio

from arq.audit import AuditStatus, apply_fix_plan, audit_redis, build_fix_plan
from arq.connections import ArqRedis
from arq.constants import (
    abort_jobs_ss,
    default_queue_name,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)
from arq.worker import Worker, func


async def test_audit_classifies_residue_and_valid_states(arq_redis: ArqRedis):
    await arq_redis.zadd(default_queue_name, {'orphan-member': 0})
    await arq_redis.set(job_key_prefix + 'orphan-payload', b'payload', px=60_000)
    await arq_redis.enqueue_job('unused', _job_id='deferred', _defer_by=60)
    await arq_redis.enqueue_job('unused', _job_id='ready')
    await arq_redis.enqueue_job('unused', _job_id='retrying')
    await arq_redis.set(in_progress_key_prefix + 'retrying', b'1', px=60_000)
    await arq_redis.set(retry_key_prefix + 'retrying', b'1', px=60_000)
    await arq_redis.set(job_key_prefix + 'stale-retry', b'payload', px=60_000)
    await arq_redis.set(retry_key_prefix + 'stale-retry', b'1', px=60_000)
    await arq_redis.set(job_key_prefix + 'retry-deferred', b'payload', px=90_000)
    await arq_redis.set(retry_key_prefix + 'retry-deferred', b'1', px=60_000)
    await arq_redis.zadd(default_queue_name, {'retry-deferred': 2**62})
    await arq_redis.zadd(default_queue_name, {'completed-member': 0})
    await arq_redis.set(result_key_prefix + 'completed-member', b'result', px=60_000)
    await arq_redis.set('unrelated:prefix', b'keep')
    report = await audit_redis(arq_redis)
    findings = {finding.job_id: finding for finding in report.findings}

    assert findings['orphan-member'].status is AuditStatus.orphan_queue_member
    assert findings['orphan-payload'].status is AuditStatus.orphan_payload
    assert findings['deferred'].status is AuditStatus.deferred
    assert findings['ready'].status is AuditStatus.queued
    assert findings['retrying'].status is AuditStatus.in_progress
    assert findings['retry-deferred'].status is AuditStatus.retry_wait
    assert findings['stale-retry'].status is AuditStatus.unknown
    assert findings['completed-member'].status is AuditStatus.unknown
    assert all(item['job_id'] != 'completed-member' for item in build_fix_plan(report)['actions'])


async def test_audit_excludes_internal_abort_zset(arq_redis: ArqRedis):
    await arq_redis.zadd(abort_jobs_ss, {'abort-id': 0})

    report = await audit_redis(arq_redis, queue_prefix='arq:')

    assert abort_jobs_ss not in report.queues
    assert not any(finding.job_id == 'abort-id' for finding in report.findings)


async def test_apply_plan_is_idempotent_and_scoped(arq_redis: ArqRedis):
    await arq_redis.zadd(default_queue_name, {'orphan-member': 0})
    await arq_redis.set(job_key_prefix + 'orphan-payload', b'payload', px=60_000)
    await arq_redis.set('unrelated:prefix', b'keep')

    plan = build_fix_plan(await audit_redis(arq_redis))
    first = await apply_fix_plan(arq_redis, plan)
    second = await apply_fix_plan(arq_redis, plan)

    assert {(item['type'], item['job_id']) for item in first.applied} == {
        ('zrem_queue_member', 'orphan-member'),
        ('delete_job_key', 'orphan-payload'),
    }
    assert first.skipped == []
    assert second.applied == []
    assert {item['job_id'] for item in second.skipped} == {'orphan-member', 'orphan-payload'}
    assert await arq_redis.get('unrelated:prefix') == b'keep'
    assert await arq_redis.zcard(default_queue_name) == 0


async def test_apply_plan_skips_when_state_changes(arq_redis: ArqRedis):
    await arq_redis.zadd(default_queue_name, {'member': 0})
    await arq_redis.set(job_key_prefix + 'payload', b'payload', px=60_000)
    plan = build_fix_plan(await audit_redis(arq_redis))

    await arq_redis.set(job_key_prefix + 'member', b'payload', px=60_000)
    await arq_redis.zadd(default_queue_name, {'payload': 1})

    result = await apply_fix_plan(arq_redis, plan)

    assert result.applied == []
    assert await arq_redis.get(job_key_prefix + 'member') == b'payload'
    assert await arq_redis.zscore(default_queue_name, 'payload') == 1


async def test_audit_requires_custom_queues_to_avoid_false_orphan(arq_redis: ArqRedis):
    custom_queue = 'team:queue'
    await arq_redis.enqueue_job('unused', _job_id='custom', _queue_name=custom_queue)

    default_report = await audit_redis(arq_redis)
    custom_report = await audit_redis(arq_redis, queues=[custom_queue], queue_prefix='none:')

    default_finding = next(finding for finding in default_report.findings if finding.job_id == 'custom')
    custom_finding = next(finding for finding in custom_report.findings if finding.job_id == 'custom')
    assert default_finding.status is AuditStatus.orphan_payload
    assert custom_finding.status is AuditStatus.queued


async def test_audit_does_not_treat_real_running_job_as_orphan(arq_redis: ArqRedis, worker):
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_job(ctx):
        started.set()
        await release.wait()

    await arq_redis.enqueue_job('slow_job', _job_id='running')
    running_worker: Worker = worker(
        functions=[func(slow_job, name='slow_job', keep_result=0)],
        burst=True,
        poll_delay=0,
        job_timeout=10,
        health_check_interval=10,
    )
    task = asyncio.create_task(running_worker.main())
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        finding = next(finding for finding in (await audit_redis(arq_redis)).findings if finding.job_id == 'running')
        assert finding.status is AuditStatus.in_progress
    finally:
        release.set()
        await task


async def test_apply_does_not_delete_real_running_job(arq_redis: ArqRedis, worker):
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_job(ctx):
        started.set()
        await release.wait()

    await arq_redis.set(job_key_prefix + 'running', b'stale-report-payload', px=60_000)
    plan = build_fix_plan(await audit_redis(arq_redis))

    await arq_redis.delete(job_key_prefix + 'running')
    await arq_redis.enqueue_job('slow_job', _job_id='running')
    running_worker: Worker = worker(
        functions=[func(slow_job, name='slow_job', keep_result=0)],
        burst=True,
        poll_delay=0,
        job_timeout=10,
        health_check_interval=10,
    )
    task = asyncio.create_task(running_worker.main())
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        result = await apply_fix_plan(arq_redis, plan)
        assert result.applied == []
        assert await arq_redis.exists(job_key_prefix + 'running') == 1
        assert await arq_redis.exists(in_progress_key_prefix + 'running') == 1
    finally:
        release.set()
        await task


async def test_apply_does_not_remove_job_became_running_from_deferred(arq_redis: ArqRedis, worker):
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_job(ctx):
        started.set()
        await release.wait()

    await arq_redis.zadd(default_queue_name, {'became-running': 0})
    plan = build_fix_plan(await audit_redis(arq_redis))

    await arq_redis.enqueue_job('slow_job', _job_id='became-running')
    running_worker: Worker = worker(
        functions=[func(slow_job, name='slow_job', keep_result=0)],
        burst=True,
        poll_delay=0,
        job_timeout=10,
        health_check_interval=10,
    )
    task = asyncio.create_task(running_worker.main())
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        result = await apply_fix_plan(arq_redis, plan)
        assert result.applied == []
        assert await arq_redis.zscore(default_queue_name, 'became-running') is not None
        assert await arq_redis.exists(job_key_prefix + 'became-running') == 1
        assert await arq_redis.exists(in_progress_key_prefix + 'became-running') == 1
    finally:
        release.set()
        await task
