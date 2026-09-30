from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from redis.exceptions import WatchError

from .connections import RedisSettings, create_pool
from .constants import (
    abort_jobs_ss,
    default_queue_name,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)
from .utils import timestamp_ms

if TYPE_CHECKING:
    from redis.asyncio import Redis

    AnyRedis = Redis[bytes]
else:
    AnyRedis = object

PLAN_VERSION = 1


class AuditConfigurationError(ValueError):
    pass


class AuditStatus(str, Enum):
    orphan_queue_member = 'orphan_queue_member'
    orphan_payload = 'orphan_payload'
    deferred = 'deferred'
    queued = 'queued'
    in_progress = 'in_progress'
    retry_wait = 'retry_wait'
    unknown = 'unknown'


FIXABLE_STATUSES = {AuditStatus.orphan_queue_member, AuditStatus.orphan_payload}


@dataclass
class AuditFinding:
    job_id: str
    status: AuditStatus
    queues: dict[str, int] = field(default_factory=dict)
    has_payload: bool = False
    payload_ttl_ms: int = -2
    has_lease: bool = False
    lease_ttl_ms: int = -2
    has_retry: bool = False
    retry_ttl_ms: int = -2
    has_result: bool = False
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data['status'] = self.status.value
        return data


@dataclass
class AuditReport:
    scanned_at_ms: int
    queues: list[str]
    findings: list[AuditFinding]
    warnings: list[str] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        counts = {status.value: 0 for status in AuditStatus}
        for finding in self.findings:
            counts[finding.status.value] += 1
        return counts

    def to_dict(self) -> dict[str, Any]:
        return {
            'scanned_at_ms': self.scanned_at_ms,
            'queues': self.queues,
            'warnings': self.warnings,
            'counts': self.counts,
            'findings': [finding.to_dict() for finding in self.findings],
        }


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _job_key(job_id: str) -> str:
    return job_key_prefix + job_id


async def discover_queues(
    redis: AnyRedis,
    *,
    queues: Sequence[str] = (),
    queue_prefix: str = default_queue_name,
    scan_count: int = 500,
) -> tuple[list[str], list[str]]:
    if not queue_prefix:
        raise AuditConfigurationError('queue prefix must not be empty')
    candidates = {queue for queue in queues}
    warnings: list[str] = []
    async for key in redis.scan_iter(match=queue_prefix + '*', count=scan_count):
        decoded_key = _decode(key)
        if decoded_key == abort_jobs_ss:
            continue
        if await redis.type(key) == b'zset':
            candidates.add(decoded_key)

    result: list[str] = []
    for queue in sorted(candidates):
        if queue == abort_jobs_ss:
            warnings.append(f'ignoring internal key {queue!r}; it is not a job queue')
            continue
        key_type = await redis.type(queue)
        if key_type == b'zset':
            result.append(queue)
        elif key_type != b'none':
            warnings.append(f'ignoring queue {queue!r}: Redis key has type {key_type.decode()}')
    return result, warnings


async def audit_redis(
    redis: AnyRedis,
    *,
    queues: Sequence[str] = (),
    queue_prefix: str = default_queue_name,
    scan_count: int = 500,
) -> AuditReport:
    discovered_queues, warnings = await discover_queues(
        redis, queues=queues, queue_prefix=queue_prefix, scan_count=scan_count
    )
    now = timestamp_ms()
    queue_members: dict[str, dict[str, int]] = {}
    job_ids: set[str] = set()

    for queue in discovered_queues:
        async for job_id_b, score in redis.zscan_iter(queue, count=scan_count):
            job_id = _decode(job_id_b)
            job_ids.add(job_id)
            queue_members.setdefault(job_id, {})[queue] = int(score)

    async for key in redis.scan_iter(match=job_key_prefix + '*', count=scan_count):
        job_ids.add(_decode(key)[len(job_key_prefix) :])

    semaphore = asyncio.Semaphore(64)

    async def audit_one(job_id: str) -> AuditFinding:
        async with semaphore:
            return await _audit_job(redis, job_id, queue_members.get(job_id, {}), now)

    findings = await asyncio.gather(*[audit_one(job_id) for job_id in sorted(job_ids)])
    return AuditReport(scanned_at_ms=now, queues=discovered_queues, findings=list(findings), warnings=warnings)


async def _audit_job(redis: AnyRedis, job_id: str, job_queues: dict[str, int], now: int) -> AuditFinding:
    job_key = _job_key(job_id)
    in_progress_key = in_progress_key_prefix + job_id
    retry_key = retry_key_prefix + job_id
    result_key = result_key_prefix + job_id
    async with redis.pipeline(transaction=False) as pipe:
        pipe.get(job_key)
        pipe.pttl(job_key)
        pipe.pttl(in_progress_key)
        pipe.pttl(retry_key)
        pipe.exists(result_key)
        payload, payload_ttl, lease_ttl, retry_ttl, has_result = await pipe.execute()

    has_payload = payload is not None
    has_lease = lease_ttl != -2
    has_retry = retry_ttl != -2
    finding = AuditFinding(
        job_id=job_id,
        status=AuditStatus.unknown,
        queues=dict(job_queues),
        has_payload=has_payload,
        payload_ttl_ms=payload_ttl,
        has_lease=has_lease,
        lease_ttl_ms=lease_ttl,
        has_retry=has_retry,
        retry_ttl_ms=retry_ttl,
        has_result=bool(has_result),
    )

    _classify_finding(finding, now)
    return finding


def _classify_finding(finding: AuditFinding, now: int) -> None:
    if len(finding.queues) > 1:
        finding.reason = 'job is present in more than one queue'
    elif finding.has_lease and finding.lease_ttl_ms > 0:
        finding.status = AuditStatus.in_progress
    elif finding.queues and finding.has_payload and finding.payload_ttl_ms >= 0:
        score = next(iter(finding.queues.values()))
        if finding.has_retry:
            finding.status = AuditStatus.retry_wait
        else:
            finding.status = AuditStatus.deferred if score > now else AuditStatus.queued
    elif finding.queues and finding.has_payload:
        finding.reason = 'queue member has a payload without an expiry'
    elif finding.queues:
        _classify_queue_member_without_payload(finding)
    elif finding.has_payload:
        _classify_payload_without_queue(finding)
    elif finding.has_retry:
        finding.reason = 'retry counter exists without a queue member or payload'


def _classify_queue_member_without_payload(finding: AuditFinding) -> None:
    if finding.has_retry:
        finding.status = AuditStatus.retry_wait
    elif finding.has_result or finding.has_lease:
        finding.reason = 'queue member is involved in an unfinished or completed lifecycle'
    else:
        finding.status = AuditStatus.orphan_queue_member
        finding.reason = 'queue member has no job payload, lease, retry counter, or result'


def _classify_payload_without_queue(finding: AuditFinding) -> None:
    if finding.payload_ttl_ms < 0:
        finding.reason = 'job payload has no expiry or has expired'
    elif finding.has_retry or finding.has_result or finding.has_lease:
        finding.reason = 'payload is involved in an unfinished or completed lifecycle'
    else:
        finding.status = AuditStatus.orphan_payload
        finding.reason = 'job payload is not a member of any scanned queue'


def build_fix_plan(
    report: AuditReport,
    *,
    max_items: int | None = None,
    queue_prefix: str = default_queue_name,
) -> dict[str, Any]:
    actions: list[dict[str, str]] = []
    for finding in report.findings:
        if finding.status not in FIXABLE_STATUSES:
            continue
        if finding.status is AuditStatus.orphan_queue_member:
            actions.append({'type': 'zrem_queue_member', 'queue': next(iter(finding.queues)), 'job_id': finding.job_id})
        else:
            actions.append({'type': 'delete_job_key', 'job_id': finding.job_id})
        if max_items is not None and len(actions) >= max_items:
            break

    return {
        'version': PLAN_VERSION,
        'created_at_ms': timestamp_ms(),
        'queue_prefix': queue_prefix,
        'queues': report.queues,
        'actions': actions,
    }


def write_fix_plan(path: str | Path, plan: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(plan, indent=2, sort_keys=True) + '\n')


def load_fix_plan(path: str | Path) -> dict[str, Any]:
    plan = json.loads(Path(path).read_text())
    if plan.get('version') != PLAN_VERSION:
        raise ValueError('unsupported repair plan version')
    if not isinstance(plan.get('actions'), list):
        raise ValueError('repair plan does not contain an actions list')
    return plan


@dataclass
class FixResult:
    applied: list[dict[str, str]] = field(default_factory=list)
    skipped: list[dict[str, str]] = field(default_factory=list)


async def apply_fix_plan(
    redis: AnyRedis,
    plan: dict[str, Any],
    *,
    queue_prefix: str | None = None,
    scan_count: int = 500,
) -> FixResult:
    prefix = queue_prefix or plan.get('queue_prefix', default_queue_name)
    if not isinstance(prefix, str) or not prefix:
        raise ValueError('repair plan has an invalid queue prefix')
    plan_queues = plan.get('queues', ())
    if not isinstance(plan_queues, (list, tuple)) or not all(isinstance(queue, str) for queue in plan_queues):
        raise ValueError('repair plan has an invalid queues list')
    current_queues, _ = await discover_queues(redis, queues=plan_queues, queue_prefix=prefix, scan_count=scan_count)
    action_queues = {
        action['queue']
        for action in plan['actions']
        if action.get('type') == 'zrem_queue_member' and isinstance(action.get('queue'), str)
    }
    extra_queues, _ = await discover_queues(
        redis, queues=action_queues.difference(current_queues), queue_prefix=prefix, scan_count=scan_count
    )
    current_queues = sorted(set(current_queues).union(extra_queues))
    result = FixResult()
    for action in plan['actions']:
        if (
            not isinstance(action, dict)
            or not isinstance(action.get('type'), str)
            or not isinstance(action.get('job_id'), str)
        ):
            result.skipped.append({'type': 'invalid', 'job_id': '', 'reason': 'repair action is malformed'})
            continue
        if action.get('type') == 'delete_job_key':
            applied = await _delete_orphan_payload(redis, action['job_id'], current_queues)
        elif action.get('type') == 'zrem_queue_member':
            if not isinstance(action.get('queue'), str):
                result.skipped.append(
                    {'type': action['type'], 'job_id': action['job_id'], 'reason': 'repair action is malformed'}
                )
                continue
            applied = await _delete_orphan_queue_member(redis, action['job_id'], action['queue'], current_queues)
        else:
            applied = False
        record = dict(action)
        if applied:
            result.applied.append(record)
        else:
            record['reason'] = 'state changed or repair preconditions no longer hold'
            result.skipped.append(record)
    return result


async def _delete_orphan_payload(redis: AnyRedis, job_id: str, queues: Sequence[str]) -> bool:
    job_key = _job_key(job_id)
    in_progress_key = in_progress_key_prefix + job_id
    retry_key = retry_key_prefix + job_id
    result_key = result_key_prefix + job_id
    watched_keys = list(dict.fromkeys([job_key, in_progress_key, retry_key, result_key, *queues]))
    pipe = redis.pipeline(transaction=True)
    try:
        await pipe.watch(*watched_keys)
        payload = await pipe.get(job_key)
        payload_ttl = await pipe.pttl(job_key)
        has_lease = await pipe.exists(in_progress_key)
        has_retry = await pipe.exists(retry_key)
        has_result = await pipe.exists(result_key)
        queue_scores = [await pipe.zscore(queue, job_id) for queue in queues]
        if (
            payload is None
            or payload_ttl < 0
            or has_lease
            or has_retry
            or has_result
            or any(score is not None for score in queue_scores)
        ):
            return False
        pipe.multi()
        pipe.delete(job_key)
        await pipe.execute()
        return True
    except WatchError:
        return False
    finally:
        await pipe.reset()


async def _delete_orphan_queue_member(redis: AnyRedis, job_id: str, queue: str, scanned_queues: Sequence[str]) -> bool:
    job_key = _job_key(job_id)
    in_progress_key = in_progress_key_prefix + job_id
    retry_key = retry_key_prefix + job_id
    result_key = result_key_prefix + job_id
    pipe = redis.pipeline(transaction=True)
    other_queues = list(dict.fromkeys(other_queue for other_queue in scanned_queues if other_queue != queue))
    try:
        await pipe.watch(queue, job_key, in_progress_key, retry_key, result_key, *other_queues)
        score = await pipe.zscore(queue, job_id)
        other_scores = [await pipe.zscore(other_queue, job_id) for other_queue in other_queues]
        payload = await pipe.get(job_key)
        has_lease = await pipe.exists(in_progress_key)
        has_retry = await pipe.exists(retry_key)
        has_result = await pipe.exists(result_key)
        if score is None or any(other_score is not None for other_score in other_scores):
            return False
        if payload is not None or has_lease or has_retry or has_result:
            return False
        pipe.multi()
        pipe.zrem(queue, job_id)
        await pipe.execute()
        return True
    except WatchError:
        return False
    finally:
        await pipe.reset()


def redis_settings_from_url(url: str | None) -> RedisSettings:
    return RedisSettings.from_dsn(url) if url else RedisSettings()


async def run_audit(
    redis_settings: RedisSettings,
    *,
    queues: Sequence[str] = (),
    queue_prefix: str = default_queue_name,
    plan_path: str | None = None,
    max_items: int | None = None,
    scan_count: int = 500,
) -> tuple[AuditReport, dict[str, Any] | None]:
    redis = await create_pool(redis_settings)
    try:
        report = await audit_redis(redis, queues=queues, queue_prefix=queue_prefix, scan_count=scan_count)
        plan = None
        if plan_path:
            plan = build_fix_plan(report, max_items=max_items, queue_prefix=queue_prefix)
            write_fix_plan(plan_path, plan)
        return report, plan
    finally:
        await redis.close(close_connection_pool=True)


async def run_apply(
    redis_settings: RedisSettings,
    plan_path: str,
    *,
    queue_prefix: str | None = None,
    scan_count: int = 500,
) -> FixResult:
    redis = await create_pool(redis_settings)
    try:
        return await apply_fix_plan(redis, load_fix_plan(plan_path), queue_prefix=queue_prefix, scan_count=scan_count)
    finally:
        await redis.close(close_connection_pool=True)
