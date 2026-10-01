import asyncio
import logging
import pickle
import uuid
import warnings
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional, cast

from redis.asyncio import Redis

from .constants import (
    abort_info_key_prefix,
    abort_jobs_ss,
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


class AbortTimeoutError(asyncio.TimeoutError):
    """
    Raised when an abort request was registered but the worker did not confirm
    the cancellation within the requested timeout.
    """

    def __init__(self, request_id: str, reason: Optional[str]):
        self.request_id = request_id
        self.reason = reason
        super().__init__(f'abort request {request_id!r} was not confirmed before the timeout')


class AbortStatus(str, Enum):
    """
    Outcome of an abort request.
    """

    #: the job had not started running yet and was cancelled before start
    before_start = 'before_start'
    #: the job was running and the worker confirmed the cancellation
    aborted = 'aborted'
    #: the job had already finished (success, failure or retry); its result was preserved
    already_finished = 'already_finished'
    #: the job could not be found in the queue, in progress or among results
    not_found = 'not_found'
    #: the abort request was registered but the worker never confirmed it
    unconfirmed = 'unconfirmed'


@dataclass
class AbortResult:
    """
    Acknowledgement of an abort request.

    The object is truthy when the job was actually cancelled (``before_start`` or
    ``aborted``), so existing ``if await job.abort():`` code keeps working.

    :param status: outcome of the abort request, see :class:`AbortStatus`
    :param request_id: id of the abort request that actually took effect;
        when several requests race, this is always the *first* one
    :param reason: cancellation reason that actually took effect, the first one wins
    """

    status: AbortStatus
    request_id: Optional[str] = None
    reason: Optional[str] = None

    def __bool__(self) -> bool:
        return self.status in (AbortStatus.before_start, AbortStatus.aborted)

    @property
    def confirmed(self) -> bool:
        return bool(self)


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


class Job:
    """
    Holds data a reference to a job.
    """

    __slots__ = '_deserializer', '_queue_name', '_redis', 'job_id'

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
        wait: bool = True,
    ) -> AbortResult:
        """
        Request the job to be aborted.

        The request is tagged with ``request_id`` and ``reason``; the worker records
        an acknowledgement (in a separate hash, never inside the job result) once the
        cancellation takes effect. When several abort requests race, the first one that
        actually takes effect wins and later requests cannot overwrite it.

        :param request_id: id of this abort request, auto generated if ``None``
        :param reason: human readable reason for the cancellation, it is passed to the
            worker's ``on_job_abort`` cleanup hook but never becomes the job's business result
        :param timeout: maximum time to wait for the worker to confirm the cancellation;
            ``None`` waits forever and ``0`` registers the request and raises
            :class:`AbortTimeoutError` immediately without waiting
        :param poll_delay: how often to poll redis for the acknowledgement
        :param wait: when False only register the request and immediately return an
            ``unconfirmed`` acknowledgement, never raise :class:`AbortTimeoutError`
        :return: an :class:`AbortResult` describing the outcome
        :raises AbortTimeoutError: the request was registered but not confirmed in time
        """
        if request_id is None:
            request_id = uuid.uuid4().hex

        now = timestamp_ms()
        abort_key = abort_info_key_prefix + self.job_id

        # 1. fast paths: a finished result or no trace of the job at all
        async with self._redis.pipeline(transaction=True) as tr:
            tr.exists(result_key_prefix + self.job_id)
            tr.exists(in_progress_key_prefix + self.job_id)
            tr.zscore(self._queue_name, self.job_id)
            is_complete, is_in_progress, score = await tr.execute()

        if is_complete:
            info = await self._read_abort_info(abort_key)
            status = info['st'] if info and info.get('st') else AbortStatus.already_finished.value
            return AbortResult(AbortStatus(status), info.get('id') if info else None, info.get('r') if info else None)

        if not is_in_progress and score is None:
            return AbortResult(AbortStatus.not_found)

        # 2. register the request. The zset is the worker's wake-up signal;
        # the hash fields use HSETNX so the first effective request wins.
        if score and score > now:
            # deferred job: move it to the front of the queue so it is picked up
            # and cancelled before start rather than waiting for its scheduled time
            async with self._redis.pipeline(transaction=True) as tr:
                tr.zrem(self._queue_name, self.job_id)
                tr.zadd(self._queue_name, {self.job_id: 1})
                await tr.execute()

        async with self._redis.pipeline(transaction=True) as tr:
            tr.zadd(abort_jobs_ss, {self.job_id: now})
            tr.hsetnx(abort_key, 'id', request_id)
            tr.hsetnx(abort_key, 'r', reason if reason is not None else '')
            await tr.execute()

        if not wait:
            return AbortResult(AbortStatus.unconfirmed, request_id, reason)

        if timeout == 0:
            raise AbortTimeoutError(request_id, reason)

        # only conclude "already finished" from disappearing keys once we have
        # observed the worker actually processing the job; a queued job is simply
        # waiting for a free worker
        seen_started = bool(is_in_progress)

        # 3. wait for the worker's acknowledgement. The result of a finished job
        # always wins over a late abort: we never overwrite or ignore it.
        async for delay in poll(poll_delay):
            info = await self._read_abort_info(abort_key)
            if info and info.get('st'):
                return AbortResult(AbortStatus(info['st']), info.get('id') or request_id, info.get('r') or None)

            if await self._redis.exists(result_key_prefix + self.job_id):
                # the job finished normally at the same time as the abort request
                return AbortResult(
                    AbortStatus.already_finished,
                    info.get('id') if info else request_id,
                    info.get('r') if info else reason,
                )

            async with self._redis.pipeline(transaction=True) as tr:
                tr.zscore(self._queue_name, self.job_id)
                tr.exists(in_progress_key_prefix + self.job_id)
                queued_score, is_in_progress = await tr.execute()
            if is_in_progress:
                seen_started = True
            elif seen_started and queued_score is None:
                return AbortResult(
                    AbortStatus.already_finished,
                    (info.get('id') if info else request_id),
                    (info.get('r') if info else reason),
                )

            if timeout is not None and delay > timeout:
                raise AbortTimeoutError(request_id, reason)
        raise RuntimeError('unreachable: poll() loop should not terminate')

    async def _read_abort_info(self, abort_key: str) -> Optional[dict[str, str]]:
        redis = cast(Any, self._redis)
        request_id = await cast('Awaitable[Any]', redis.hget(abort_key, 'id'))
        if request_id is None:
            return None
        reason = await cast('Awaitable[Any]', redis.hget(abort_key, 'r'))
        status = await cast('Awaitable[Any]', redis.hget(abort_key, 'st'))

        def _str(v: Any) -> str:
            return v.decode() if isinstance(v, bytes) else (v or '')

        return {'id': _str(request_id), 'r': _str(reason), 'st': _str(status)}

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
        )
    except Exception as e:
        raise DeserializationError('unable to deserialize job result') from e
