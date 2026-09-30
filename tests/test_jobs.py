import asyncio
import pickle

import pytest
from dirty_equals import IsNow, IsStr

from arq import Worker, func
from arq.connections import ArqRedis, RedisSettings, create_pool
from arq.constants import (
    default_queue_name,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    result_notification_channel_prefix,
)
from arq.jobs import (
    DeserializationError,
    Job,
    JobResult,
    JobStatus,
    ResultExpired,
    ResultNotFound,
    deserialize_job_raw,
    serialize_result,
)


async def test_job_in_progress(arq_redis: ArqRedis):
    await arq_redis.set(in_progress_key_prefix + 'foobar', b'1')
    j = Job('foobar', arq_redis)
    assert JobStatus.in_progress == await j.status()
    assert str(j) == '<arq job foobar>'


async def test_unknown(arq_redis: ArqRedis):
    j = Job('foobar', arq_redis)
    assert JobStatus.not_found == await j.status()
    info = await j.info()
    assert info is None


async def test_result_timeout(arq_redis: ArqRedis):
    j = await arq_redis.enqueue_job('foobar')
    with pytest.raises(asyncio.TimeoutError):
        await j.result(0.1, poll_delay=0)


async def test_result_not_found(arq_redis: ArqRedis):
    j = Job('foobar', arq_redis)
    with pytest.raises(ResultNotFound):
        await j.result()


async def test_result_completes_before_subscription(arq_redis: ArqRedis, worker, monkeypatch):
    async def foobar(ctx):
        return 42

    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    j = await arq_redis.enqueue_job('foobar')
    notifications = arq_redis._arq_result_notifications
    pubsub_factory = arq_redis.pubsub
    subscription_sent = asyncio.Event()
    allow_subscription_ack = asyncio.Event()

    class ControlledPubSub:
        def __new__(cls, *args, **kwargs):
            pubsub = pubsub_factory(*args, **kwargs)
            get_message = pubsub.get_message

            async def controlled_get_message(*message_args, **message_kwargs):
                message = await get_message(*message_args, **message_kwargs)
                if message is not None and message['type'] == 'psubscribe':
                    subscription_sent.set()
                    await allow_subscription_ack.wait()
                return message

            pubsub.get_message = controlled_get_message
            return pubsub

    monkeypatch.setattr(arq_redis, 'pubsub', ControlledPubSub)
    result = asyncio.create_task(j.result(poll_delay=0.01))
    await subscription_sent.wait()

    await worker.main()
    allow_subscription_ack.set()

    assert await asyncio.wait_for(result, timeout=5) == 42


async def test_result_completes_between_subscription_and_recheck(arq_redis: ArqRedis, worker, monkeypatch):
    async def foobar(ctx):
        return 42

    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    j = await arq_redis.enqueue_job('foobar')
    result_state = j._result_state
    subscription_confirmed = asyncio.Event()
    allow_recheck = asyncio.Event()
    state_calls = 0

    async def controlled_result_state():
        nonlocal state_calls
        state_calls += 1
        if state_calls == 2:
            subscription_confirmed.set()
            await allow_recheck.wait()
        return await result_state()

    monkeypatch.setattr(j, '_result_state', controlled_result_state)
    result = asyncio.create_task(j.result(poll_delay=0.01))
    await subscription_confirmed.wait()

    await worker.main()
    allow_recheck.set()

    assert await asyncio.wait_for(result, timeout=5) == 42


async def test_result_completes_after_recheck_uses_notification(arq_redis: ArqRedis, worker, monkeypatch):
    async def foobar(ctx):
        return 42

    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    j = await arq_redis.enqueue_job('foobar')
    notifications = arq_redis._arq_result_notifications
    subscribe = notifications.subscribe
    subscribed = asyncio.Event()

    async def subscribed_subscribe(job_id, queue_name):
        waiter = await subscribe(job_id)
        subscribed.set()
        return waiter

    monkeypatch.setattr(notifications, 'subscribe', subscribed_subscribe)
    result = asyncio.create_task(j.result(poll_delay=10))
    await subscribed.wait()
    while j.job_id not in notifications._waiters:
        await asyncio.sleep(0.01)

    await worker.main()

    assert await asyncio.wait_for(result, timeout=5) == 42


async def test_result_rechecks_after_notification_disconnect(arq_redis: ArqRedis, worker, monkeypatch):
    async def foobar(ctx):
        return 42

    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    j = await arq_redis.enqueue_job('foobar')
    notifications = arq_redis._arq_result_notifications
    subscribe = notifications.subscribe
    subscribed = asyncio.Event()

    async def subscribed_subscribe(job_id, queue_name):
        waiter = await subscribe(job_id)
        subscribed.set()
        return waiter

    monkeypatch.setattr(notifications, 'subscribe', subscribed_subscribe)
    result = asyncio.create_task(j.result(poll_delay=0.01))
    await subscribed.wait()

    notifications._pubsub.connection.disconnect()
    await worker.main()

    assert await asyncio.wait_for(result, timeout=5) == 42


async def test_result_waiters_share_subscription_and_cancel_independently(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    j = await arq_redis.enqueue_job('foobar')
    notifications = arq_redis._arq_result_notifications
    first_result = asyncio.create_task(j.result(poll_delay=10))
    second_result = asyncio.create_task(j.result(poll_delay=10))

    while len(notifications._waiters[j.job_id]) < 2:
        await asyncio.sleep(0.01)

    pubsub = notifications._pubsub
    first_result.cancel()
    cancelled = await asyncio.gather(first_result, return_exceptions=True)
    assert isinstance(cancelled[0], asyncio.CancelledError)
    assert len(notifications._waiters[j.job_id]) == 1

    await worker.main()

    assert await asyncio.wait_for(second_result, timeout=5) == 42
    assert notifications._pubsub is pubsub


async def test_result_expired_is_distinct_from_unknown_job(arq_redis: ArqRedis):
    job_id = 'previously-known-job'
    j = Job(job_id, arq_redis)
    await arq_redis.zadd(default_queue_name, {job_id: 1})

    result = asyncio.create_task(j.result(timeout=2, poll_delay=0.01))
    while not arq_redis._arq_result_notifications._waiters.get(job_id):
        await asyncio.sleep(0.01)

    await arq_redis.zrem(default_queue_name, job_id)
    await arq_redis.publish(result_notification_channel_prefix + job_id, b'')

    with pytest.raises(ResultExpired):
        await result


async def test_result_when_job_does_not_keep_result(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        pass

    worker: Worker = worker(functions=[func(foobar, name='foobar', keep_result=0)])
    j = await arq_redis.enqueue_job('foobar')

    result_call = asyncio.Task(j.result())

    _, pending = await asyncio.wait([result_call], timeout=0.1)
    assert pending == {result_call}

    await worker.main()

    with pytest.raises(ResultNotFound):
        # Job has completed and did not store any result
        await asyncio.wait_for(result_call, timeout=5)


async def test_enqueue_job(arq_redis: ArqRedis, worker, queue_name=default_queue_name):
    async def foobar(ctx, *args, **kwargs):
        return 42

    j = await arq_redis.enqueue_job('foobar', 1, 2, c=3, _queue_name=queue_name)
    assert isinstance(j, Job)
    assert JobStatus.queued == await j.status()
    worker: Worker = worker(functions=[func(foobar, name='foobar')], queue_name=queue_name)
    await worker.main()
    r = await j.result(poll_delay=0)
    assert r == 42
    assert JobStatus.complete == await j.status()
    info = await j.info()
    expected_queue_name = queue_name or arq_redis.default_queue_name
    assert info == JobResult(
        job_try=1,
        function='foobar',
        args=(1, 2),
        kwargs={'c': 3},
        enqueue_time=IsNow(tz='UTC'),
        success=True,
        result=42,
        start_time=IsNow(tz='UTC'),
        finish_time=IsNow(tz='UTC'),
        score=None,
        queue_name=expected_queue_name,
        job_id=IsStr(),
    )
    results = await arq_redis.all_job_results()
    assert results == [
        JobResult(
            function='foobar',
            args=(1, 2),
            kwargs={'c': 3},
            job_try=1,
            enqueue_time=IsNow(tz='UTC'),
            success=True,
            result=42,
            start_time=IsNow(tz='UTC'),
            finish_time=IsNow(tz='UTC'),
            score=None,
            queue_name=expected_queue_name,
            job_id=j.job_id,
        )
    ]


async def test_enqueue_job_alt_queue(arq_redis: ArqRedis, worker):
    await test_enqueue_job(arq_redis, worker, queue_name='custom_queue')


async def test_enqueue_job_nondefault_queue(test_redis_settings: RedisSettings, worker):
    """Test initializing arq_redis with a queue name, and the worker using it."""
    arq_redis = await create_pool(test_redis_settings, default_queue_name='test_queue')
    await test_enqueue_job(
        arq_redis,
        lambda functions, **_: worker(functions=functions, arq_redis=arq_redis, queue_name=None),
        queue_name=None,
    )


async def test_cant_unpickle_at_all():
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    r1 = serialize_result('foobar', (1,), {}, 1, 123, True, Foobar(), 123, 123, 'testing', 'test-queue', 'job_1')
    assert isinstance(r1, bytes)
    r2 = serialize_result('foobar', (Foobar(),), {}, 1, 123, True, Foobar(), 123, 123, 'testing', 'test-queue', 'job_1')
    assert r2 is None


async def test_custom_serializer():
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    def custom_serializer(x):
        return b'0123456789'

    r1 = serialize_result(
        'foobar',
        (1,),
        {},
        1,
        123,
        True,
        Foobar(),
        123,
        123,
        'testing',
        'test-queue',
        'job_1',
        serializer=custom_serializer,
    )
    assert r1 == b'0123456789'
    r2 = serialize_result(
        'foobar',
        (Foobar(),),
        {},
        1,
        123,
        True,
        Foobar(),
        123,
        123,
        'testing',
        'test-queue',
        'job_1',
        serializer=custom_serializer,
    )
    assert r2 == b'0123456789'


async def test_deserialize_result(arq_redis: ArqRedis, worker):
    async def foobar(ctx, a, b):
        return a + b

    j = await arq_redis.enqueue_job('foobar', 1, 2)
    assert JobStatus.queued == await j.status()
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.run_check()
    assert await j.result(poll_delay=0) == 3
    assert await j.result(poll_delay=0) == 3
    info = await j.info()
    assert info.args == (1, 2)
    await arq_redis.set(result_key_prefix + j.job_id, b'invalid pickle data')
    with pytest.raises(DeserializationError, match='unable to deserialize job result'):
        assert await j.result(poll_delay=0) == 3


async def test_deserialize_info(arq_redis: ArqRedis):
    j = await arq_redis.enqueue_job('foobar', 1, 2)
    assert JobStatus.queued == await j.status()
    await arq_redis.set(job_key_prefix + j.job_id, b'invalid pickle data')

    with pytest.raises(DeserializationError, match='unable to deserialize job'):
        assert await j.info()


async def test_deserialize_job_raw():
    assert deserialize_job_raw(pickle.dumps({'f': 1, 'a': 2, 'k': 3, 't': 4, 'et': 5})) == (1, 2, 3, 4, 5)
    with pytest.raises(DeserializationError, match='unable to deserialize job'):
        deserialize_job_raw(b'123')


async def test_get_job_result(arq_redis: ArqRedis):
    with pytest.raises(KeyError, match='job "foobar" not found'):
        await arq_redis._get_job_result(b'foobar')


async def test_result_pole_delay_dep(arq_redis: ArqRedis):
    j = Job('foobar', arq_redis)
    r = serialize_result('foobar', (1,), {}, 1, 123, True, 42, 123, 123, 'testing', 'test-queue', 'job_1')
    await arq_redis.set(result_key_prefix + j.job_id, r)
    with pytest.warns(
        DeprecationWarning, match='"pole_delay" is deprecated, use the correct spelling "poll_delay" instead'
    ):
        assert await j.result(pole_delay=0) == 42
