import asyncio
import logging
import pickle
import warnings
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
from uuid import uuid4

from redis.asyncio import Redis

from .constants import (
    abort_jobs_ss,
    abort_outcome_key_prefix,
    abort_request_key_prefix,
    default_queue_name,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
)
from .utils import ms_to_datetime, poll, timestamp_ms

logger = logging.getLogger('arq.jobs')

Serializer = Callable[[dict[str, Any]], bytes]
Deserializer = Callable[[bytes], dict[str, Any]]


class ResultNotFound(RuntimeError):
    pass


class JobStatus(str, Enum):
    """
    Enum of job statuses.
    """

    #: job is in the queue, time it should be run not yet reached
    deferred = 'deferred'
    #: job is in the queue, time it should run has been reached
    queued = 'queued'
    #: job is in progress
    in_progress = 'in_progress'
    #: job is complete, result is available
    complete = 'complete'
    #: job not found in any way
    not_found = 'not_found'


class AbortStatus(str, Enum):
    """
    Outcome of an :meth:`Job.abort` call.
    """

    #: the job had not started yet (it was still queued or deferred) and was cancelled
    before_start = 'before_start'
    #: the job was running and the worker confirmed that it was interrupted
    confirmed = 'confirmed'
    #: the job had already finished (success, failure or an earlier abort) before this abort took effect
    already_finished = 'already_finished'
    #: the abort was requested, but no worker confirmed interrupting the job before ``timeout`` elapsed
    timeout = 'timeout'


@dataclass
class AbortResult:
    """
    Receipt describing what happened to an abort request.

    ``request_id`` and ``reason`` are the values supplied to :meth:`Job.abort`. ``status`` tells whether
    the job was cancelled before it started, cancelled while running, had already finished or whether the
    cancellation was never confirmed.

    When an earlier abort request is the one that actually interrupted the job, ``effective_request_id`` and
    ``effective_reason`` carry that first, effective request. Otherwise they match ``request_id`` and
    ``reason``.
    """

    request_id: str
    reason: Optional[str]
    status: AbortStatus
    effective_request_id: Optional[str] = None
    effective_reason: Optional[str] = None

    def __bool__(self) -> bool:
        return self.status in (AbortStatus.before_start, AbortStatus.confirmed)


@dataclass
class AbortOutcome:
    """
    Record, stored by the worker, describing the abort that actually interrupted a job.
    """

    request_id: str
    reason: Optional[str]
    phase: str


@dataclass
class JobDef:
    function: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    job_try: int
    enqueue_time: datetime
    score: Optional[int]
    job_id: Optional[str]

    def __post_init__(self) -> None:
        if isinstance(self.score, float):
            self.score = int(self.score)


@dataclass
class JobResult(JobDef):
    success: bool
    result: Any
    start_time: datetime
    finish_time: datetime
    queue_name: str
    #: phase in which the job was aborted ('before_start' or 'running'), None if it was not aborted
    abort_phase: Optional[str] = None
    #: id of the abort request which actually interrupted the job, if any
    abort_request_id: Optional[str] = None
    #: reason attached to the abort request which actually interrupted the job, if any
    abort_reason: Optional[str] = None


class Job:
    """
    Holds data a reference to a job.
    """

    __slots__ = 'job_id', '_redis', '_queue_name', '_deserializer'

    def __init__(
        self,
        job_id: str,
        redis: 'Redis[bytes]',
        _queue_name: str = default_queue_name,
        _deserializer: Optional[Deserializer] = None,
    ):
        self.job_id = job_id
        self._redis = redis
        self._queue_name = _queue_name
        self._deserializer = _deserializer

    async def result(
        self, timeout: Optional[float] = None, *, poll_delay: float = 0.5, pole_delay: Optional[float] = None
    ) -> Any:
        """
        Get the result of the job or, if the job raised an exception, reraise it.

        This function waits for the result if it's not yet available and the job is
        present in the queue. Otherwise ``ResultNotFound`` is raised.

        :param timeout: maximum time to wait for the job result before raising ``TimeoutError``, will wait forever
        :param poll_delay: how often to poll redis for the job result
        :param pole_delay: deprecated, use poll_delay instead
        """
        if pole_delay is not None:
            warnings.warn(
                '"pole_delay" is deprecated, use the correct spelling "poll_delay" instead', DeprecationWarning
            )
            poll_delay = pole_delay

        async for delay in poll(poll_delay):
            async with self._redis.pipeline(transaction=True) as tr:
                tr.get(result_key_prefix + self.job_id)
                tr.zscore(self._queue_name, self.job_id)
                v, s = await tr.execute()

            if v:
                info = deserialize_result(v, deserializer=self._deserializer)
                if info.success:
                    return info.result
                elif isinstance(info.result, (Exception, asyncio.CancelledError)):
                    raise info.result
                else:
                    raise SerializationError(info.result)
            elif s is None:
                raise ResultNotFound(
                    'Not waiting for job result because the job is not in queue. '
                    'Is the worker function configured to keep result?'
                )

            if timeout is not None and delay > timeout:
                raise asyncio.TimeoutError()

    async def info(self) -> Optional[JobDef]:
        """
        All information on a job, including its result if it's available, does not wait for the result.
        """
        info: Optional[JobDef] = await self.result_info()
        if not info:
            v = await self._redis.get(job_key_prefix + self.job_id)
            if v:
                info = deserialize_job(v, deserializer=self._deserializer)
        if info:
            s = await self._redis.zscore(self._queue_name, self.job_id)
            info.score = None if s is None else int(s)
        return info

    async def result_info(self) -> Optional[JobResult]:
        """
        Information about the job result if available, does not wait for the result. Does not raise an exception
        even if the job raised one.
        """
        v = await self._redis.get(result_key_prefix + self.job_id)
        if v:
            return deserialize_result(v, deserializer=self._deserializer)
        else:
            return None

    async def status(self) -> JobStatus:
        """
        Status of the job.
        """
        async with self._redis.pipeline(transaction=True) as tr:
            tr.exists(result_key_prefix + self.job_id)
            tr.exists(in_progress_key_prefix + self.job_id)
            tr.zscore(self._queue_name, self.job_id)
            is_complete, is_in_progress, score = await tr.execute()

        if is_complete:
            return JobStatus.complete
        elif is_in_progress:
            return JobStatus.in_progress
        elif score:
            return JobStatus.deferred if score > timestamp_ms() else JobStatus.queued
        else:
            return JobStatus.not_found

    async def abort(
        self,
        *,
        request_id: Optional[str] = None,
        reason: Optional[str] = None,
        timeout: Optional[float] = None,
        poll_delay: float = 0.5,
    ) -> AbortResult:
        """
        Request abortion of the job.

        Several abort requests can be issued concurrently: the first one that actually interrupts the job
        wins, later requests never overwrite a stored result or the effective abort reason.

        :param request_id: identifier of the abort request, a random hex id is generated when None;
            can be used to deduplicate requests
        :param reason: human readable reason for the abort, this is passed to the ``on_job_end`` /
            ``after_job_end`` hooks but never becomes the job's business result
        :param timeout: maximum time to wait for the abort to be confirmed by a worker,
            waits forever on None; on timeout an :class:`AbortResult` with
            :attr:`AbortStatus.timeout` is returned
        :param poll_delay: how often to poll redis for the abort outcome
        :return: an :class:`AbortResult` receipt describing what happened
        """
        if request_id is None:
            request_id = uuid4().hex

        # if the job has already finished (result or abort outcome present, job gone), do nothing:
        # this abort must never overwrite the completed result
        current_status = await self.status()
        if current_status in (JobStatus.complete, JobStatus.not_found):
            outcome = await self._get_abort_outcome() if current_status is JobStatus.complete else None
            if outcome is not None:
                return AbortResult(
                    request_id=request_id,
                    reason=reason,
                    status=AbortStatus.confirmed
                    if outcome.phase == 'running'
                    else AbortStatus.before_start,
                    effective_request_id=outcome.request_id,
                    effective_reason=outcome.reason,
                )
            # already finished, or nothing ever existed for this id: nothing to abort and no marker
            return AbortResult(request_id=request_id, reason=reason, status=AbortStatus.already_finished)

        job_info = await self.info()
        if job_info and job_info.score and job_info.score > timestamp_ms():
            async with self._redis.pipeline(transaction=True) as tr:
                tr.zrem(self._queue_name, self.job_id)
                tr.zadd(self._queue_name, {self.job_id: 1})
                await tr.execute()

        # NX on the abort request keeps the first request's id and reason; both point to the same
        # outcome, but only the first request is actually effective
        request_blob = serialize_abort_request(request_id, reason)
        async with self._redis.pipeline(transaction=True) as tr:
            tr.zadd(abort_jobs_ss, {self.job_id: timestamp_ms()})
            tr.set(abort_request_key_prefix + self.job_id, request_blob, nx=True)
            await tr.execute()

        # poll both the result key and the abort outcome key: an abort before start may leave no
        # result (keep_result=0) while still writing an outcome
        async for delay in poll(poll_delay):
            async with self._redis.pipeline(transaction=True) as tr:
                tr.get(abort_outcome_key_prefix + self.job_id)
                tr.get(result_key_prefix + self.job_id)
                tr.zscore(self._queue_name, self.job_id)
                tr.exists(in_progress_key_prefix + self.job_id)
                outcome_v, result_v, queue_score, in_progress = await tr.execute()

            if outcome_v:
                outcome = deserialize_abort_outcome(outcome_v)
                return AbortResult(
                    request_id=request_id,
                    reason=reason,
                    status=AbortStatus.confirmed
                    if outcome.phase == 'running'
                    else AbortStatus.before_start,
                    effective_request_id=outcome.request_id,
                    effective_reason=outcome.reason,
                )
            if result_v:
                # job finished with a stored result that carries no abort outcome: the abort never
                # took effect (normal success or failure), never claim it as cancelled
                return AbortResult(request_id=request_id, reason=reason, status=AbortStatus.already_finished)
            if queue_score is None and not in_progress:
                # nothing left in the queue and no result/outcome: we cannot tell what happened
                return AbortResult(request_id=request_id, reason=reason, status=AbortStatus.already_finished)

            if timeout is not None and delay > timeout:
                return AbortResult(request_id=request_id, reason=reason, status=AbortStatus.timeout)

    async def _get_abort_outcome(self) -> Optional[AbortOutcome]:
        v = await self._redis.get(abort_outcome_key_prefix + self.job_id)
        if v:
            return deserialize_abort_outcome(v)
        return None

    def __repr__(self) -> str:
        return f'<arq job {self.job_id}>'


class SerializationError(RuntimeError):
    pass


class DeserializationError(SerializationError):
    pass


def serialize_job(
    function_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    job_try: Optional[int],
    enqueue_time_ms: int,
    *,
    serializer: Optional[Serializer] = None,
) -> bytes:
    data = {'t': job_try, 'f': function_name, 'a': args, 'k': kwargs, 'et': enqueue_time_ms}
    if serializer is None:
        serializer = pickle.dumps
    try:
        return serializer(data)
    except Exception as e:
        raise SerializationError(f'unable to serialize job "{function_name}"') from e


def serialize_result(
    function: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    job_try: int,
    enqueue_time_ms: int,
    success: bool,
    result: Any,
    start_ms: int,
    finished_ms: int,
    ref: str,
    queue_name: str,
    job_id: str,
    abort_phase: Optional[str] = None,
    abort_request_id: Optional[str] = None,
    abort_reason: Optional[str] = None,
    *,
    serializer: Optional[Serializer] = None,
) -> Optional[bytes]:
    data = {
        't': job_try,
        'f': function,
        'a': args,
        'k': kwargs,
        'et': enqueue_time_ms,
        's': success,
        'r': result,
        'st': start_ms,
        'ft': finished_ms,
        'q': queue_name,
        'id': job_id,
    }
    if abort_phase is not None:
        data['ap'] = abort_phase
    if abort_request_id is not None:
        data['arid'] = abort_request_id
    if abort_reason is not None:
        data['arr'] = abort_reason
    if serializer is None:
        serializer = pickle.dumps
    try:
        return serializer(data)
    except Exception:
        logger.warning('error serializing result of %s', ref, exc_info=True)

    # use string in case serialization fails again
    data.update(r='unable to serialize result', s=False)
    try:
        return serializer(data)
    except Exception:
        logger.critical('error serializing result of %s even after replacing result', ref, exc_info=True)
    return None


def deserialize_job(r: bytes, *, deserializer: Optional[Deserializer] = None) -> JobDef:
    if deserializer is None:
        deserializer = pickle.loads
    try:
        d = deserializer(r)
        return JobDef(
            function=d['f'],
            args=d['a'],
            kwargs=d['k'],
            job_try=d['t'],
            enqueue_time=ms_to_datetime(d['et']),
            score=None,
            job_id=None,
        )
    except Exception as e:
        raise DeserializationError('unable to deserialize job') from e


def deserialize_job_raw(
    r: bytes, *, deserializer: Optional[Deserializer] = None
) -> tuple[str, tuple[Any, ...], dict[str, Any], int, int]:
    if deserializer is None:
        deserializer = pickle.loads
    try:
        d = deserializer(r)
        return d['f'], d['a'], d['k'], d['t'], d['et']
    except Exception as e:
        raise DeserializationError('unable to deserialize job') from e


def deserialize_result(r: bytes, *, deserializer: Optional[Deserializer] = None) -> JobResult:
    if deserializer is None:
        deserializer = pickle.loads
    try:
        d = deserializer(r)
        return JobResult(
            job_try=d['t'],
            function=d['f'],
            args=d['a'],
            kwargs=d['k'],
            enqueue_time=ms_to_datetime(d['et']),
            score=None,
            success=d['s'],
            result=d['r'],
            start_time=ms_to_datetime(d['st']),
            finish_time=ms_to_datetime(d['ft']),
            queue_name=d.get('q', '<unknown>'),
            job_id=d.get('id', '<unknown>'),
            abort_phase=d.get('ap'),
            abort_request_id=d.get('arid'),
            abort_reason=d.get('arr'),
        )
    except Exception as e:
        raise DeserializationError('unable to deserialize job result') from e


def serialize_abort_request(request_id: str, reason: Optional[str]) -> bytes:
    return pickle.dumps({'id': request_id, 'r': reason})


def deserialize_abort_request(r: bytes) -> tuple[str, Optional[str]]:
    try:
        d = pickle.loads(r)
        return d['id'], d.get('r')
    except Exception as e:
        raise DeserializationError('unable to deserialize abort request') from e


def serialize_abort_outcome(request_id: str, reason: Optional[str], phase: str) -> bytes:
    return pickle.dumps({'id': request_id, 'r': reason, 'p': phase})


def deserialize_abort_outcome(r: bytes) -> AbortOutcome:
    try:
        d = pickle.loads(r)
        return AbortOutcome(request_id=d['id'], reason=d.get('r'), phase=d['p'])
    except Exception as e:
        raise DeserializationError('unable to deserialize abort outcome') from e
