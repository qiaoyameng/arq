import hashlib
from collections import Counter
from collections.abc import Collection
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional

from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from .constants import (
    default_queue_name,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)
from .utils import timestamp_ms


_QUEUE_TYPE_CHANGED = '\0queue-type-changed\0'


class MaintenanceStatus(str, Enum):
    orphan_queue_member = 'orphan_queue_member'
    orphan_payload = 'orphan_payload'
    orphan_retry = 'orphan_retry'
    deferred = 'deferred'
    queued = 'queued'
    in_progress = 'in_progress'
    retrying = 'retrying'
    complete = 'complete'
    unknown = 'unknown'


class MaintenanceCategory(str, Enum):
    orphan = 'orphan'
    delayed = 'delayed'
    active = 'active'
    complete = 'complete'
    unknown = 'unknown'


@dataclass(frozen=True)
class QueueMember:
    job_id: str
    queue: str
    score: int


@dataclass(frozen=True)
class ValueSnapshot:
    value: bytes
    pttl: int


@dataclass(frozen=True)
class JobMaintenance:
    job_id: str
    status: MaintenanceStatus
    category: MaintenanceCategory
    queues: tuple[QueueMember, ...] = ()
    has_payload: bool = False
    payload_pttl: Optional[int] = None
    has_retry: bool = False
    retry: Optional[int] = None
    retry_pttl: Optional[int] = None
    has_lease: bool = False
    lease_pttl: Optional[int] = None
    has_result: bool = False
    reason: str = ''
    payload_sha256: Optional[str] = None
    retry_sha256: Optional[str] = None


@dataclass(frozen=True)
class MaintenanceIssue:
    key: str
    kind: str
    detail: str


@dataclass(frozen=True)
class MaintenanceAction:
    job_id: str
    operation: str
    queue_members: tuple[QueueMember, ...] = ()
    delete_payload: bool = False
    payload_sha256: Optional[str] = None
    delete_retry: bool = False
    retry_value: Optional[str] = None
    retry_sha256: Optional[str] = None


@dataclass
class MaintenanceReport:
    queues: list[str]
    jobs: list[JobMaintenance] = field(default_factory=list)
    issues: list[MaintenanceIssue] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        return dict(Counter(job.status.value for job in self.jobs))

    @property
    def category_counts(self) -> dict[str, int]:
        return dict(Counter(job.category.value for job in self.jobs))

    def actions(self, limit: Optional[int] = None) -> list[MaintenanceAction]:
        actions: list[MaintenanceAction] = []
        for job in sorted((j for j in self.jobs if j.category == MaintenanceCategory.orphan), key=lambda j: j.job_id):
            action = _action_for_job(job)
            if action is not None:
                actions.append(action)
                if limit is not None and len(actions) >= limit:
                    break
        return actions


@dataclass(frozen=True)
class MaintenancePlan:
    queues: tuple[str, ...]
    actions: tuple[MaintenanceAction, ...]
    version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'MaintenancePlan':
        if data.get('version') != 1:
            raise ValueError('unsupported maintenance plan version')
        queues = tuple(data['queues'])
        actions = tuple(
            MaintenanceAction(
                job_id=action['job_id'],
                operation=action['operation'],
                queue_members=tuple(
                    QueueMember(member['job_id'], member['queue'], member['score'])
                    for member in action['queue_members']
                ),
                delete_payload=action['delete_payload'],
                payload_sha256=action.get('payload_sha256'),
                delete_retry=action['delete_retry'],
                retry_value=action.get('retry_value'),
                retry_sha256=action.get('retry_sha256'),
            )
            for action in data['actions']
        )
        return cls(queues=queues, actions=actions)


@dataclass(frozen=True)
class MaintenanceActionResult:
    job_id: str
    operation: str
    applied: bool
    reason: str


@dataclass(frozen=True)
class MaintenanceApplyResult:
    applied: list[MaintenanceActionResult]
    skipped: list[MaintenanceActionResult]
    failed: list[MaintenanceActionResult]


def make_maintenance_plan(report: MaintenanceReport, *, max_actions: int = 100) -> MaintenancePlan:
    return MaintenancePlan(queues=tuple(report.queues), actions=tuple(report.actions(max_actions)))


async def apply_maintenance_plan(redis: 'Redis[bytes]', plan: MaintenancePlan) -> MaintenanceApplyResult:
    applied: list[MaintenanceActionResult] = []
    skipped: list[MaintenanceActionResult] = []
    failed: list[MaintenanceActionResult] = []
    for action in plan.actions:
        try:
            reason = await _apply_action(redis, plan, action)
        except WatchError:
            reason = 'state changed during application'
        except RedisError as exc:
            failed.append(MaintenanceActionResult(action.job_id, action.operation, False, str(exc)))
            continue

        result = MaintenanceActionResult(action.job_id, action.operation, reason == 'applied', reason)
        (applied if result.applied else skipped).append(result)

    return MaintenanceApplyResult(applied=applied, skipped=skipped, failed=failed)


async def inspect_jobs(
    redis: 'Redis[bytes]',
    *,
    queue_names: Optional[Collection[str]] = None,
    discover_queues: bool = True,
) -> MaintenanceReport:
    """
    Build a non-blocking snapshot of arq queue, job, retry, result and lease keys.

    The scan uses SCAN/ZSCAN and never mutates Redis, so workers can continue picking
    up and finishing jobs while the report is being generated.
    """
    queues = set(queue_names) if queue_names is not None else {default_queue_name}
    if discover_queues:
        async for key in redis.scan_iter(match='arq:queue*', count=500):
            try:
                key_name = _decode(key)
            except UnicodeDecodeError:
                continue
            if key_name and await redis.type(key_name) == b'zset':
                queues.add(key_name)

    queue_names_sorted = sorted(queues)
    report = MaintenanceReport(queues=queue_names_sorted)
    queue_members: dict[str, list[QueueMember]] = {}
    job_ids: set[str] = set()

    for queue in queue_names_sorted:
        members: list[QueueMember] = []
        try:
            async for job_id, score in redis.zscan_iter(queue, count=500):
                try:
                    job_id_text = _decode(job_id)
                except UnicodeDecodeError:
                    report.issues.append(MaintenanceIssue(queue, 'invalid_queue_member', repr(job_id)))
                    continue
                if not job_id_text:
                    report.issues.append(MaintenanceIssue(queue, 'invalid_queue_member', repr(job_id)))
                    continue
                member = QueueMember(job_id_text, queue, int(score))
                members.append(member)
                job_ids.add(job_id_text)
        except RedisError as exc:
            report.issues.append(MaintenanceIssue(queue, 'queue_scan_failed', str(exc)))
        queue_members[queue] = members

    payloads, malformed_payloads = await _scan_values(redis, job_key_prefix + '*', job_ids, report)
    retries, malformed_retries = await _scan_values(redis, retry_key_prefix + '*', job_ids, report)
    leases, malformed_leases = await _scan_values(redis, in_progress_key_prefix + '*', job_ids, report)
    result_ids = await _scan_key_ids(redis, result_key_prefix + '*', result_key_prefix)
    job_ids.update(result_ids)

    jobs: list[JobMaintenance] = []
    now = timestamp_ms()
    for job_id in sorted(job_ids):
        members = tuple(
            sorted(
                (
                    member
                    for members_for_queue in queue_members.values()
                    for member in members_for_queue
                    if member.job_id == job_id
                ),
                key=lambda member: (member.queue, member.score),
            )
        )
        payload = payloads.get(job_id)
        retry_snapshot = retries.get(job_id)
        lease = leases.get(job_id)
        has_result = job_id in result_ids
        jobs.append(
            _classify_job(
                job_id=job_id,
                members=members,
                payload=payload,
                retry=retry_snapshot,
                lease=lease,
                malformed_payload=job_id in malformed_payloads,
                malformed_retry=job_id in malformed_retries,
                malformed_lease=job_id in malformed_leases,
                has_result=has_result,
                now=now,
                checked_queues=bool(queue_names_sorted),
            )
        )

    report.jobs = jobs
    return report


def _classify_job(
    *,
    job_id: str,
    members: tuple[QueueMember, ...],
    payload: Optional[ValueSnapshot],
    retry: Optional[ValueSnapshot],
    lease: Optional[ValueSnapshot],
    malformed_payload: bool,
    malformed_retry: bool,
    malformed_lease: bool,
    has_result: bool,
    now: int,
    checked_queues: bool,
) -> JobMaintenance:
    has_payload = payload is not None
    payload_pttl = None if payload is None else payload.pttl
    has_retry = retry is not None
    retry_value = None
    retry_pttl = None if retry is None else retry.pttl
    if retry is not None:
        try:
            retry_value = int(retry.value)
        except ValueError:
            retry_value = None
    payload_sha256 = None if payload is None else _fingerprint(payload.value)
    retry_sha256 = None if retry is None else _fingerprint(retry.value)
    has_lease = lease is not None
    lease_pttl = None if lease is None else lease.pttl
    live_lease = has_lease and lease_pttl is not None and lease_pttl > 0
    expired_lease = has_lease and lease_pttl == 0
    unknown_lease = has_lease and (lease_pttl is None or lease_pttl < 0 or lease.value != b'1')
    future_members = tuple(member for member in members if member.score > now)
    due_members = tuple(member for member in members if member.score <= now)

    def job(status: MaintenanceStatus, reason: str) -> JobMaintenance:
        category = _category_for_status(status)
        return JobMaintenance(
            job_id=job_id,
            status=status,
            category=category,
            queues=members,
            has_payload=has_payload,
            payload_pttl=payload_pttl,
            has_retry=has_retry,
            retry=retry_value,
            retry_pttl=retry_pttl,
            has_lease=has_lease,
            lease_pttl=lease_pttl,
            has_result=has_result,
            reason=reason,
            payload_sha256=payload_sha256,
            retry_sha256=retry_sha256,
        )

    if malformed_payload or (has_payload and payload_pttl is not None and payload_pttl < 0):
        return job(MaintenanceStatus.unknown, 'job payload has no expiry')
    if malformed_retry or (has_retry and (retry_value is None or retry_pttl is None or retry_pttl < 0)):
        return job(MaintenanceStatus.unknown, 'retry counter is malformed or has no expiry')
    if len({member.queue for member in members}) != len(members):
        return job(MaintenanceStatus.unknown, 'job is present more than once in a queue')

    if live_lease:
        status = MaintenanceStatus.retrying if has_retry else MaintenanceStatus.in_progress
        return job(status, 'unexpired execution lease exists')
    if malformed_lease or unknown_lease:
        return job(MaintenanceStatus.unknown, 'execution lease is malformed')

    if not has_payload and members and not expired_lease:
        return job(MaintenanceStatus.orphan_queue_member, 'queue member has no payload')

    if has_payload and future_members and has_retry:
        return job(MaintenanceStatus.retrying, 'scheduled retry has not reached its score')
    if has_payload and future_members:
        return job(MaintenanceStatus.deferred, 'scheduled for a future time')
    if has_payload and due_members and has_retry:
        return job(MaintenanceStatus.retrying, 'due queue member with retry counter')
    if has_payload and due_members:
        return job(MaintenanceStatus.queued, 'due queue member with payload')

    if has_result and (members or has_payload or has_retry):
        return job(MaintenanceStatus.unknown, 'result coexists with other job metadata')

    if has_payload and not members and not has_result and checked_queues:
        return job(MaintenanceStatus.orphan_payload, 'payload is not referenced by a queue or result')
    if has_retry and not members and not has_payload and not has_lease and checked_queues:
        return job(MaintenanceStatus.orphan_retry, 'retry counter has no queue, payload or lease')
    if has_result and not members and not has_payload:
        return job(MaintenanceStatus.complete, 'result is available')
    if expired_lease and members and not has_payload:
        return job(MaintenanceStatus.orphan_queue_member, 'queue member has no payload and lease is expired')

    return job(MaintenanceStatus.unknown, 'insufficient consistent state to classify job')


def _category_for_status(status: MaintenanceStatus) -> MaintenanceCategory:
    if status in {
        MaintenanceStatus.orphan_queue_member,
        MaintenanceStatus.orphan_payload,
        MaintenanceStatus.orphan_retry,
    }:
        return MaintenanceCategory.orphan
    if status == MaintenanceStatus.deferred:
        return MaintenanceCategory.delayed
    if status in {MaintenanceStatus.in_progress, MaintenanceStatus.retrying, MaintenanceStatus.queued}:
        return MaintenanceCategory.active
    if status == MaintenanceStatus.complete:
        return MaintenanceCategory.complete
    return MaintenanceCategory.unknown


def _action_for_job(job: JobMaintenance) -> Optional[MaintenanceAction]:
    if job.status == MaintenanceStatus.orphan_queue_member:
        return MaintenanceAction(
            job.job_id,
            'remove_queue_member',
            queue_members=job.queues,
            delete_retry=job.has_retry,
            retry_value=str(job.retry) if job.retry is not None else None,
            retry_sha256=job.retry_sha256,
        )
    if job.status == MaintenanceStatus.orphan_payload:
        return MaintenanceAction(
            job.job_id,
            'delete_payload',
            delete_payload=True,
            payload_sha256=job.payload_sha256,
            delete_retry=job.has_retry,
            retry_value=str(job.retry) if job.retry is not None else None,
            retry_sha256=job.retry_sha256,
        )
    if job.status == MaintenanceStatus.orphan_retry:
        return MaintenanceAction(
            job.job_id,
            'delete_retry',
            delete_retry=True,
            retry_value=str(job.retry) if job.retry is not None else None,
            retry_sha256=job.retry_sha256,
        )
    return None


async def _scan_values(
    redis: 'Redis[bytes]',
    pattern: str,
    known_job_ids: set[str],
    report: MaintenanceReport,
) -> tuple[dict[str, ValueSnapshot], set[str]]:
    values: dict[str, ValueSnapshot] = {}
    malformed: set[str] = set()
    async for key in redis.scan_iter(match=pattern, count=500):
        try:
            key_name = _decode(key)
        except UnicodeDecodeError:
            report.issues.append(MaintenanceIssue(repr(key), 'invalid_key', 'undecodable key name'))
            continue
        if not key_name:
            continue
        prefix = pattern[:-1]
        job_id = key_name[len(prefix) :]
        if not job_id:
            report.issues.append(MaintenanceIssue(key_name, 'invalid_key', 'empty job id'))
            continue
        known_job_ids.add(job_id)
        async with redis.pipeline(transaction=False) as pipe:
            pipe.type(key_name)
            pipe.get(key_name)
            pipe.pttl(key_name)
            key_type, value, pttl = await pipe.execute()
        if key_type != b'string':
            report.issues.append(MaintenanceIssue(key_name, 'unexpected_key_type', key_type.decode(errors='replace')))
            malformed.add(job_id)
            continue
        if value is None or pttl is None:
            report.issues.append(MaintenanceIssue(key_name, 'key_disappeared_during_scan', 'skipping'))
            malformed.add(job_id)
            continue
        values[job_id] = ValueSnapshot(value=value, pttl=pttl)
    return values, malformed


async def _scan_key_ids(redis: 'Redis[bytes]', pattern: str, prefix: str) -> set[str]:
    job_ids: set[str] = set()
    async for key in redis.scan_iter(match=pattern, count=500):
        try:
            key_name = _decode(key)
            if key_name and key_name.startswith(prefix):
                job_id = key_name[len(prefix) :]
                if job_id:
                    job_ids.add(job_id)
        except UnicodeDecodeError:
            continue
    return job_ids


def _decode(value: bytes) -> str:
    return value.decode('utf-8')


def _fingerprint(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


async def _apply_action(
    redis: 'Redis[bytes]',
    plan: MaintenancePlan,
    action: MaintenanceAction,
) -> str:
    job_key = job_key_prefix + action.job_id
    retry_key = retry_key_prefix + action.job_id
    lease_key = in_progress_key_prefix + action.job_id
    planned_queues: set[str] = set(plan.queues)
    action_queues = {member.queue for member in action.queue_members}
    expected_scores = {member.queue: member.score for member in action.queue_members}
    queues = sorted(planned_queues | action_queues)
    watched_keys = [job_key, retry_key, lease_key, *queues]

    async with redis.pipeline(transaction=True) as pipe:
        for key in dict.fromkeys(watched_keys):
            await pipe.watch(key)

        lease_pttl = await pipe.pttl(lease_key)
        if lease_pttl is not None and lease_pttl != -2 and lease_pttl != 0:
            return 'unexpired execution lease exists'

        queue_members = await _current_queue_members(pipe, action.job_id, queues)
        if _QUEUE_TYPE_CHANGED in queue_members:
            return 'queue key type changed after plan was generated'
        changed_scores = {
            queue: score
            for queue, score in queue_members.items()
            if queue in expected_scores and score != expected_scores[queue]
        }
        if changed_scores:
            return 'queue score changed after plan was generated'

        payload = await pipe.get(job_key)
        payload_pttl = await pipe.pttl(job_key)
        retry_value = await pipe.get(retry_key)
        retry_pttl = await pipe.pttl(retry_key)

        if action.operation == 'remove_queue_member':
            if payload is not None:
                return 'payload appeared after plan was generated'
            if set(queue_members) - action_queues:
                return 'job is referenced by a queue not covered by the plan'
            if not any(queue in queue_members for queue in action_queues):
                return 'queue member already absent'
            if not _retry_matches(action, retry_value, retry_pttl):
                return 'retry counter changed after plan was generated'
            pipe.multi()
            for queue in queue_members:
                pipe.zrem(queue, action.job_id)
            if action.delete_retry:
                pipe.delete(retry_key)
            await pipe.execute()
            return 'applied'

        if action.operation == 'delete_payload':
            if payload is None or payload_pttl is None or payload_pttl < 0:
                return 'payload already absent, expiry changed, or payload changed'
            if action.payload_sha256 != _fingerprint(payload):
                return 'payload changed after plan was generated'
            if queue_members:
                return 'queue reference appeared after plan was generated'
            if not _retry_matches(action, retry_value, retry_pttl):
                return 'retry counter changed after plan was generated'
            pipe.multi()
            pipe.delete(job_key)
            if action.delete_retry:
                pipe.delete(retry_key)
            await pipe.execute()
            return 'applied'

        if action.operation == 'delete_retry':
            if not _retry_matches(action, retry_value, retry_pttl):
                return 'retry counter already absent or changed'
            if payload is not None or queue_members:
                return 'state changed after plan was generated'
            pipe.multi()
            pipe.delete(retry_key)
            await pipe.execute()
            return 'applied'

        return f'unsupported action {action.operation}'


def _retry_matches(action: MaintenanceAction, value: Optional[bytes], pttl: Optional[int]) -> bool:
    if not action.delete_retry:
        return value is None
    return value is not None and pttl is not None and pttl >= 0 and action.retry_sha256 == _fingerprint(value)


async def _current_queue_members(pipe: Any, job_id: str, queues: Collection[str]) -> dict[str, int]:
    members: dict[str, int] = {}
    for queue in queues:
        queue_type = await pipe.type(queue)
        if queue_type not in (b'none', b'zset'):
            return {_QUEUE_TYPE_CHANGED: -1}
        if queue_type == b'zset':
            score = await pipe.zscore(queue, job_id)
            if score is not None:
                members[queue] = int(score)
    return members
