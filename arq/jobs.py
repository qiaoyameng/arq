import asyncio
import logging
import pickle
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional

from redis.exceptions import RedisError
from redis.asyncio import Redis

from .constants import (
    abort_jobs_ss,
    default_queue_name,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    result_notification_channel_prefix,
)
from .utils import ms_to_datetime, timestamp_ms

if TYPE_CHECKING:
    from redis.asyncio.client import PubSub

logger = logging.getLogger('arq.jobs')

Serializer = Callable[[dict[str, Any]], bytes]
Deserializer = Callable[[bytes], dict[str, Any]]


class ResultNotFound(RuntimeError):
    pass


class ResultExpired(ResultNotFound):
    pass


ResultExpiredError = ResultExpired


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


@dataclass
class _JobResultState:
    result: Optional[bytes]
    score: Optional[float]
    in_progress: bool


_result_pending = object()


class ResultNotifications:
    def __init__(self, redis: 'Redis[bytes]', reconnect_delay: float = 0.5):
        self._redis = redis
        self._reconnect_delay = reconnect_delay
        self._pubsub: Optional['PubSub'] = None
        self._reader: Optional[asyncio.Task[None]] = None
        self._startup: Optional[asyncio.Future[bool]] = None
        self._lock = asyncio.Lock()
        self._waiters: dict[str, set['asyncio.Queue[bytes]']] = defaultdict(set)
        self._queue_names: dict[str, str] = {}

    async def _create_reader(self) -> None:
        pubsub = self._redis.pubsub()
        task = asyncio.get_running_loop().create_task(self._read(pubsub))
        startup: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pubsub = pubsub
        self._reader = task
        self._startup = startup

        try:
            await pubsub.psubscribe(result_notification_channel_prefix + '*')
            if pubsub.connection is not None:
                pubsub.connection.health_check_interval = self._reconnect_delay
            await asyncio.wait_for(startup, timeout=5)
        except BaseException:
            self._pubsub = None
            self._reader = None
            self._startup = None
            task.cancel()
            try:
                await task
            except BaseException:
                pass
            await pubsub.aclose()
            raise

    async def subscribe(self, job_id: str, queue_name: str) -> '_ResultWaiter':
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
        async with self._lock:
            self._waiters[job_id].add(queue)
            self._queue_names[job_id] = queue_name
            try:
                if self._reader is None or self._reader.done():
                    await self._create_reader()
            except BaseException:
                self._unsubscribe(job_id, queue)
                raise
        return _ResultWaiter(self, job_id, queue)

    def _unsubscribe(self, job_id: str, queue: 'asyncio.Queue[bytes]') -> None:
        waiters = self._waiters.get(job_id)
        if waiters is not None:
            waiters.discard(queue)
            if not waiters:
                self._waiters.pop(job_id, None)
                self._queue_names.pop(job_id, None)

    def _wake(self, job_id: Optional[str] = None, data: bytes = b'') -> None:
        if job_id is None:
            waiters = [queue for queues in self._waiters.values() for queue in queues]
        else:
            waiters = list(self._waiters.get(job_id, ()))

        for queue in waiters:
            if queue.empty():
                queue.put_nowait(data)

    async def _recheck_waiting_jobs(self) -> None:
        jobs = list(self._waiters)
        if not jobs:
            return

        async with self._redis.pipeline(transaction=True) as pipe:
            for job_id in jobs:
                queue_name = self._queue_names.get(job_id, default_queue_name)
                pipe.get(result_key_prefix + job_id)
                pipe.zscore(queue_name, job_id)
                pipe.exists(in_progress_key_prefix + job_id)
            values = await pipe.execute()

        for index, job_id in enumerate(jobs):
            result, score, in_progress = values[index * 3 : index * 3 + 3]
            if result or (score is None and not in_progress):
                self._wake(job_id)

    async def _read(self, pubsub: 'PubSub') -> None:
        pattern = result_notification_channel_prefix.encode()
        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=False, timeout=self._reconnect_delay)
                if message is None:
                    await self._recheck_waiting_jobs()
                    continue

                message_type = message['type']
                if message_type in {'psubscribe', 'pmessage'}:
                    if message_type == 'psubscribe' and self._startup is not None and not self._startup.done():
                        self._startup.set_result(True)
                    elif message_type == 'psubscribe':
                        self._wake()
                    if message_type == 'pmessage':
                        channel = message['channel']
                        data = message['data']
                        if isinstance(channel, bytes) and channel.startswith(pattern):
                            data = data if isinstance(data, bytes) else b''
                            self._wake(channel[len(pattern) :].decode(), data)
                        elif isinstance(channel, str) and channel.startswith(result_notification_channel_prefix):
                            data = data if isinstance(data, bytes) else b''
                            self._wake(channel[len(result_notification_channel_prefix) :], data)
                elif message['type'] != 'pong':
                    self._wake()
            except asyncio.CancelledError:
                raise
            except BaseException:
                self._wake()
                await asyncio.sleep(self._reconnect_delay)

    async def aclose(self) -> None:
        if self._reader is not None:
            self._reader.cancel()
            try:
                await self._reader
            except (asyncio.CancelledError, Exception):
                pass
        if self._pubsub is not None:
            await self._pubsub.aclose()
        self._reader = None
        self._pubsub = None
        self._startup = None
        self._wake()


class _ResultWaiter:
    __slots__ = '_notifications', '_job_id', '_queue'

    def __init__(self, notifications: ResultNotifications, job_id: str, queue: 'asyncio.Queue[bytes]') -> None:
        self._notifications = notifications
        self._job_id = job_id
        self._queue = queue

    async def wait(self, timeout: Optional[float] = None) -> bytes:
        return await asyncio.wait_for(self._queue.get(), timeout=timeout)

    def close(self) -> None:
        self._notifications._unsubscribe(self._job_id, self._queue)


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
        self,
        timeout: Optional[float] = None,
        *,
        poll_delay: float = 0.5,
        pole_delay: Optional[float] = None,
    ) -> Any:
        """
        Get the result of the job or, if the job raised an exception, reraise it.

        This function waits for the result if it's not yet available and the job is
        present in the queue. Otherwise ``ResultNotFound`` is raised.

        Waiting is event-driven: waiters share a Redis Pub/Sub connection, and the
        result is rechecked before and after establishing the subscription. Pub/Sub
        messages only wake a waiter; the persisted result remains the source of truth.

        :param timeout: maximum time to wait for the job result before raising ``TimeoutError``, will wait forever
        :param poll_delay: fallback delay when the notification connection is unavailable
        :param pole_delay: deprecated, use poll_delay instead
        """
        if pole_delay is not None:
            warnings.warn(
                '"pole_delay" is deprecated, use the correct spelling "poll_delay" instead', DeprecationWarning
            )
            poll_delay = pole_delay

        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        saw_job = False
        notifications = getattr(self._redis, '_arq_result_notifications', None)
        if notifications is None:
            notifications = ResultNotifications(self._redis)
            try:
                setattr(self._redis, '_arq_result_notifications', notifications)
            except (AttributeError, TypeError):
                pass

        while True:
            state = await self._result_state()
            result = self._deserialize_result_state(state)
            if result is not _result_pending:
                return result

            if state.score is not None or state.in_progress:
                saw_job = True
            elif saw_job:
                raise ResultExpired(
                    'Job result expired before it was read. '
                    'Increase keep_result or use keep_result_forever to retain it for longer.'
                )
            else:
                raise ResultNotFound(
                    'Not waiting for job result because the job is not in queue. '
                    'Is the worker function configured to keep result?'
                )

            if deadline is not None:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise asyncio.TimeoutError()
            else:
                remaining = None

            try:
                subscription = await notifications.subscribe(self.job_id, self._queue_name)
            except (RedisError, OSError):
                await asyncio.sleep(poll_delay)
                continue

            try:
                state = await self._result_state()
                result = self._deserialize_result_state(state)
                if result is not _result_pending:
                    return result

                if state.score is None and not state.in_progress:
                    if saw_job and notification != b'none':
                        raise ResultExpired(
                            'Job result expired before it was read. '
                            'Increase keep_result or use keep_result_forever to retain it for longer.'
                        )
                    raise ResultNotFound(
                        'Not waiting for job result because the job is not in queue. '
                        'Is the worker function configured to keep result?'
                    )

                if deadline is not None:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError()

                notification = await subscription.wait(remaining)
            except asyncio.TimeoutError:
                raise
            except (RedisError, OSError):
                await asyncio.sleep(poll_delay)
            finally:
                subscription.close()

    async def _result_state(self) -> _JobResultState:
        async with self._redis.pipeline(transaction=True) as tr:
            tr.get(result_key_prefix + self.job_id)
            tr.zscore(self._queue_name, self.job_id)
            tr.exists(in_progress_key_prefix + self.job_id)
            result, score, in_progress = await tr.execute()
        return _JobResultState(result, score, bool(in_progress))

    def _deserialize_result_state(self, state: _JobResultState) -> Any:
        if not state.result:
            return _result_pending

        info = deserialize_result(state.result, deserializer=self._deserializer)
        if info.success:
            return info.result
        if isinstance(info.result, (Exception, asyncio.CancelledError)):
            raise info.result
        raise SerializationError(info.result)

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
