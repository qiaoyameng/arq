import pytest

from arq.constants import in_progress_key_prefix, job_key_prefix, retry_key_prefix
from arq.maintenance import (
    MaintenancePlan,
    MaintenanceStatus,
    apply_maintenance_plan,
    inspect_jobs,
    make_maintenance_plan,
)

from .maintenance_fakes import FakeRedis


@pytest.fixture
async def fake_redis():
    redis = FakeRedis()
    yield redis


async def _jobs(redis):
    return {job.job_id: job for job in (await inspect_jobs(redis)).jobs}


async def test_fake_classifies_residue_and_safe_states(fake_redis):
    await fake_redis.zadd('arq:queue', {'missing': 1, 'deferred': 10_000_000_000_000})
    await fake_redis.psetex(job_key_prefix + 'deferred', 10_000, b'payload')
    await fake_redis.psetex(job_key_prefix + 'orphan-payload', 10_000, b'payload')
    await fake_redis.setex(retry_key_prefix + 'orphan-retry', 10, b'1')
    await fake_redis.psetex(in_progress_key_prefix + 'running', 10_000, b'1')
    await fake_redis.psetex(job_key_prefix + 'running', 10_000, b'payload')
    await fake_redis.zadd('arq:queue', {'running': 1})
    await fake_redis.setex(retry_key_prefix + 'retrying', 10, b'1')
    await fake_redis.psetex(job_key_prefix + 'retrying', 10_000, b'payload')
    await fake_redis.zadd('arq:queue', {'retrying': 10_000_000_000_000})
    await fake_redis.psetex(job_key_prefix + 'unknown', 10_000, b'payload')
    await fake_redis.persist(job_key_prefix + 'unknown')

    jobs = await _jobs(fake_redis)

    assert jobs['missing'].status == MaintenanceStatus.orphan_queue_member
    assert jobs['orphan-payload'].status == MaintenanceStatus.orphan_payload
    assert jobs['orphan-retry'].status == MaintenanceStatus.orphan_retry
    assert jobs['deferred'].status == MaintenanceStatus.deferred
    assert jobs['running'].status == MaintenanceStatus.in_progress
    assert jobs['retrying'].status == MaintenanceStatus.retrying
    assert jobs['unknown'].status == MaintenanceStatus.unknown


async def test_fake_apply_limited_idempotent_and_prefix_scoped(fake_redis):
    await fake_redis.zadd('arq:queue', {'missing': 1, 'second': 2})
    await fake_redis.psetex(job_key_prefix + 'orphan-payload', 10_000, b'payload')
    await fake_redis.setex(retry_key_prefix + 'orphan-retry', 10, b'1')
    await fake_redis.set('unrelated:key', b'keep')
    await fake_redis.set('other-prefix:unrelated-like', b'keep')

    plan = make_maintenance_plan(await inspect_jobs(fake_redis), max_actions=2)
    assert [action.job_id for action in plan.actions] == ['missing', 'orphan-payload']

    result = await apply_maintenance_plan(fake_redis, plan)
    assert len(result.applied) == 2
    second = await apply_maintenance_plan(fake_redis, plan)
    assert len(second.applied) == 0
    assert await fake_redis.zscore('arq:queue', 'second') == 2
    assert await fake_redis.get(retry_key_prefix + 'orphan-retry') == b'1'
    assert await fake_redis.get('unrelated:key') == b'keep'
    assert await fake_redis.get('other-prefix:unrelated-like') == b'keep'


async def test_fake_apply_rechecks_lease_and_state_changes(fake_redis):
    await fake_redis.zadd('arq:queue', {'missing': 1})
    await fake_redis.psetex(job_key_prefix + 'orphan-payload', 10_000, b'payload')
    plan = make_maintenance_plan(await inspect_jobs(fake_redis))

    await fake_redis.psetex(in_progress_key_prefix + 'missing', 10_000, b'1')
    await fake_redis.zadd('arq:queue', {'orphan-payload': 1})

    result = await apply_maintenance_plan(fake_redis, plan)

    assert result.applied == []
    assert {item.reason for item in result.skipped} == {
        'unexpired execution lease exists',
        'queue reference appeared after plan was generated',
    }


async def test_fake_apply_rechecks_payload_value_and_queue_score(fake_redis):
    await fake_redis.zadd('arq:queue', {'queued-orphan': 1})
    await fake_redis.psetex(job_key_prefix + 'changed-payload', 10_000, b'old')
    plan = make_maintenance_plan(await inspect_jobs(fake_redis))

    await fake_redis.psetex(job_key_prefix + 'changed-payload', 10_000, b'new')
    await fake_redis.psetex(job_key_prefix + 'queued-orphan', 10_000, b'payload')

    result = await apply_maintenance_plan(fake_redis, plan)

    assert result.applied == []
    assert {item.reason for item in result.skipped} == {
        'payload changed after plan was generated',
        'payload appeared after plan was generated',
    }


async def test_fake_apply_skips_changed_queue_score(fake_redis):
    await fake_redis.zadd('arq:queue', {'missing': 1})
    plan = make_maintenance_plan(await inspect_jobs(fake_redis))
    await fake_redis.zadd('arq:queue', {'missing': 2})

    result = await apply_maintenance_plan(fake_redis, plan)

    assert result.applied == []
    assert result.skipped[0].reason == 'queue score changed after plan was generated'
    assert await fake_redis.zscore('arq:queue', 'missing') == 2


async def test_fake_apply_skips_persisted_payload(fake_redis):
    await fake_redis.psetex(job_key_prefix + 'persisted', 10_000, b'payload')
    plan = make_maintenance_plan(await inspect_jobs(fake_redis))
    await fake_redis.persist(job_key_prefix + 'persisted')

    result = await apply_maintenance_plan(fake_redis, plan)

    assert result.applied == []
    assert result.skipped[0].reason == 'payload already absent, expiry changed, or payload changed'


async def test_fake_plan_json_roundtrip(fake_redis):
    await fake_redis.zadd('arq:queue', {'missing': 1})
    plan = make_maintenance_plan(await inspect_jobs(fake_redis))
    assert MaintenancePlan.from_dict(plan.to_dict()) == plan


async def test_fake_queue_scope_is_explicit_without_discovery(fake_redis):
    await fake_redis.psetex(job_key_prefix + 'scoped', 10_000, b'payload')
    await fake_redis.zadd('custom:queue', {'scoped': 1})
    await fake_redis.zadd('unscanned:queue', {'other': 1})

    default_report = await inspect_jobs(fake_redis, queue_names=('custom:queue',), discover_queues=False)
    default_jobs = {job.job_id: job for job in default_report.jobs}
    assert default_jobs['scoped'].status == MaintenanceStatus.queued
    assert 'other' not in default_jobs

    await fake_redis.delete(job_key_prefix + 'scoped')
    scoped_report = await inspect_jobs(fake_redis, queue_names=('custom:queue',), discover_queues=False)
    scoped_plan = make_maintenance_plan(scoped_report)
    await apply_maintenance_plan(fake_redis, scoped_plan)

    assert await fake_redis.zscore('custom:queue', 'scoped') is None
    assert await fake_redis.zscore('unscanned:queue', 'other') == 1


async def test_fake_due_retry_is_active(fake_redis):
    await fake_redis.setex(retry_key_prefix + 'due-retry', 10, b'1')
    await fake_redis.psetex(job_key_prefix + 'due-retry', 10_000, b'payload')
    await fake_redis.zadd('arq:queue', {'due-retry': 1})

    job = next(job for job in (await inspect_jobs(fake_redis)).jobs if job.job_id == 'due-retry')

    assert job.status == MaintenanceStatus.retrying
    assert make_maintenance_plan(await inspect_jobs(fake_redis)).actions == ()


async def test_fake_unexpired_malformed_lease_is_protected(fake_redis):
    await fake_redis.zadd('arq:queue', {'weird-lease': 1})
    await fake_redis.psetex(in_progress_key_prefix + 'weird-lease', 10_000, b'not-a-standard-lease')

    report = await inspect_jobs(fake_redis)
    job = next(job for job in report.jobs if job.job_id == 'weird-lease')

    assert job.status == MaintenanceStatus.in_progress
    assert make_maintenance_plan(report).actions == ()
