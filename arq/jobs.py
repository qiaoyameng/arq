import asyncio
import logging
import pickle
import warnings
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional

from redis.asyncio import Redis

from .constants import abort_jobs_ss, default_queue_name, in_progress_key_prefix, job_key_prefix, result_key_prefix
from .utils import ms_to_datetime, timestamp_ms

logger = logging.getLogger('arq.jobs')

Serializer = Callable[[dict[str, Any]], bytes]
Deserializer = Callable[[bytes], dict[str, Any]]


class ResultNotFound(RuntimeError):
    pass


class ResultExpired(ResultNotFound):
    """
    The job used to exist (was queued or running while we waited) but its result
    key has expired and no more state is available, so the result can never be
    returned.

    Subclasses :class:`ResultNotFound` so existing callers keep working; use the
    subclass to tell expiry apart from a job that never existed.
    """


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

        This function waits for the result if it's not yet available and the
        job is present. Waiting is notification based: the worker publishes a
        wake-up ping on a per-job channel in the same transaction that persists
        the result, and many waiters share a single pub/sub subscription. State
        is always re-read from redis after subscribing and after every wake-up,
        so a notification that is lost (e.g. the notification connection drops)
        or races with the subscription cannot cause a hang.

        If the job never existed (and is not currently in the queue)
        :class:`ResultNotFound` is raised. If a job that was present while we
        waited finishes without a retrievable result (its result key already
        expired, or the job was configured not to keep one) :class:`ResultExpired`
        is raised instead. ``ResultExpired`` is a subclass of ``ResultNotFound``.

        Clients that are not an :class:`~arq.connections.ArqRedis` fall back to
        polling every ``poll_delay`` seconds.

        :param timeout: maximum time to wait for the job result before raising ``TimeoutError``, will wait forever
        :param poll_delay: polling interval used only when notification based waiting is unavailable
        :param pole_delay: deprecated, use poll_delay instead
        """
        if pole_delay is not None:
            warnings.warn(
                '"pole_delay" is deprecated, use the correct spelling "poll_delay" instead', DeprecationWarning
            )
            poll_delay = pole_delay

        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        # whether we have personally observed the job as queued/running: that is
        # what lets us distinguish "expired / result not kept" from "never existed"
        seen_present = False

        notifier = getattr(self._redis, 'result_notifier', None)

        while True:
            v, score = await self._fetch_state()
            if v:
                info = deserialize_result(v, deserializer=self._deserializer)
                if info.success:
                    return info.result
                elif isinstance(info.result, (Exception, asyncio.CancelledError)):
                    raise info.result
                else:
                    raise SerializationError(info.result)
            if score is not None:
                seen_present = True
            elif seen_present:
                raise ResultExpired(
                    'Job result is not available: the job finished but its result was not kept '
                    'or has already expired.'
                )
            else:
                raise ResultNotFound(
                    'Not waiting for job result because the job is not in queue. '
                    'Is the worker function configured to keep result?'
                )

            remaining = None if deadline is None else deadline - asyncio.get_running_loop().time()
            if remaining is not None and remaining <= 0:
                raise asyncio.TimeoutError()

            if notifier is None:
                await asyncio.sleep(min(poll_delay, remaining) if remaining is not None else poll_delay)
                continue

            # a fresh waiter per iteration: a disconnect-induced wake-up
            # resolves the old future, and looping back must register a new one
            # (shared with any other waiters) rather than sleeping on an
            # already-resolved future
            waiter = notifier.wait_for(self.job_id)
            try:
                await waiter.subscribe()
                # mandatory re-check between the SUBSCRIBE being confirmed and
                # going to sleep; the waiter was registered before the
                # SUBSCRIBE, so a completion landing in this gap is caught
                # either here or by the notification waking the registered future
                v, score = await self._fetch_state()
                if v is not None or score is None:
                    continue
                seen_present = True
                woken = await waiter.wait(remaining)
                if not woken:
                    raise asyncio.TimeoutError()
            finally:
                await waiter.release()

    async def _fetch_state(self) -> tuple[Optional[bytes], Optional[float]]:
        async with self._redis.pipeline(transaction=True) as tr:
            tr.get(result_key_prefix + self.job_id)
            tr.zscore(self._queue_name, self.job_id)
            v, score = await tr.execute()
        return v, score

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

    async def abort(self, *, timeout: Optional[float] = None, poll_delay: float = 0.5) -> bool:
        """
        Abort the job.

        :param timeout: maximum time to wait for the job result before raising ``TimeoutError``,
            will wait forever on None
        :param poll_delay: how often to poll redis for the job result
        :return: True if the job aborted properly, False otherwise
        """
        job_info = await self.info()
        if job_info and job_info.score and job_info.score > timestamp_ms():
            async with self._redis.pipeline(transaction=True) as tr:
                tr.zrem(self._queue_name, self.job_id)
                tr.zadd(self._queue_name, {self.job_id: 1})
                await tr.execute()

        await self._redis.zadd(abort_jobs_ss, {self.job_id: timestamp_ms()})

        try:
            await self.result(timeout=timeout, poll_delay=poll_delay)
        except asyncio.CancelledError:
            return True
        except ResultNotFound:
            # We do not know if the job was cancelled or not
            return False
        else:
            return False

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
