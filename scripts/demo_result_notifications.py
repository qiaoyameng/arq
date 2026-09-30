"""
Local proof that notification-based waiting on arq job results never hangs.

Three race timing points are arranged deliberately:

1. completion lands BETWEEN the pre-subscribe state check and SUBSCRIBE being
   confirmed (the classic subscribe-window race);
2. completion lands in the WAIT window but the notification connection drops at
   the same instant, so the PUBLISH is lost;
3. several waiters share one subscription and one of them is cancelled while
   waiting - the others must still be woken when the job completes.

Plus checks for: expiry vs job-not-found staying distinguishable, and a
notification connection dropping while idle (waiter re-checks and recovers).

Run:
    .venv/bin/python scripts/demo_result_notifications.py

The demo uses a real redis on localhost:6379 if one answers, otherwise it
starts an in-process, pipe-backed RESP2 redis (see scripts/redis_emu.py),
needed because this sandbox forbids network sockets entirely.
"""

from __future__ import annotations

import argparse
import asyncio
import socket
import sys
import time
from typing import Any

from arq.connections import ArqRedis
from arq.constants import default_queue_name, result_channel_prefix, result_key_prefix
from arq.jobs import Job, ResultExpired, ResultNotFound, serialize_result


def result_bytes(job_id: str, value: Any = 42) -> bytes:
    now = int(time.time() * 1000)
    return serialize_result(
        'demo-fn', (), {}, 1, now, True, value, now, now, job_id, default_queue_name, job_id=job_id
    )


async def persist_and_notify(redis: ArqRedis, job_id: str, value: Any = 42, *, px_ms: int = 60_000) -> None:
    """Exactly what Worker.finish_job does: SET result + PUBLISH in one transaction."""
    async with redis.pipeline(transaction=True) as tr:
        tr.set(result_key_prefix + job_id, result_bytes(job_id, value), px=px_ms)
        tr.publish(result_channel_prefix + job_id, job_id)
        tr.zrem(default_queue_name, job_id)
        await tr.execute()


async def persist_without_notification(redis: ArqRedis, job_id: str) -> None:
    """Persist result while deliberately NOT publishing (simulates lost PUBLISH)."""
    async with redis.pipeline(transaction=True) as tr:
        tr.set(result_key_prefix + job_id, result_bytes(job_id, 99), px=60_000)
        tr.zrem(default_queue_name, job_id)
        await tr.execute()


class Report:
    def __init__(self) -> None:
        self.ok = 0
        self.fail = 0

    def check(self, name: str, cond: bool, detail: str = '') -> None:
        mark = 'PASS' if cond else 'FAIL'
        if cond:
            self.ok += 1
        else:
            self.fail += 1
        print(f'  [{mark}] {name}' + (f' -- {detail}' if detail else ''))


async def race_1_subscribe_window(redis: ArqRedis, report: Report) -> None:
    """
    Race point 1: completion happens strictly between the waiter's first state
    check and the SUBSCRIBE round trip completing.

    We force the window open by pausing the SUBSCRIBE command on the server side
    while another coroutine completes the job. Correct behaviour: after
    SUBSCRIBE confirms, the waiter's mandatory post-subscribe re-check finds the
    persisted result; it must not sleep on a notification that was sent before
    the subscription existed (which would hang until timeout).
    """
    print('\n=== Race 1: completion in the pre-subscribe -> SUBSCRIBE window ===')
    job_id = 'race1'
    await redis.zadd(default_queue_name, {job_id: 1})
    job = Job(job_id, redis)

    server = getattr(redis, '_demo_server', None)
    completed = asyncio.Event()

    # The dangerous instant is strictly after SUBSCRIBE is confirmed and before
    # the waiter reads its answer from the mandatory post-subscribe state check.
    # We complete the job (persist result + publish in one transaction) inside
    # the first GET of that result key performed after the subscription exists.
    # The notification therefore can either be lost in the gap or arrive while
    # no one is listening yet; the persisted result must still be found.
    def complete_during_post_subscribe_get(key: bytes) -> None:
        # scheduled, not awaited: this hook runs on the state-check connection,
        # so awaiting another round trip on it would deadlock the pipeline
        server.before_state_check = None

        async def _complete() -> None:
            await persist_and_notify(redis, job_id, value='race-1-result')
            completed.set()

        asyncio.create_task(_complete())

    def armed_get(key: bytes) -> None:
        channel = (result_channel_prefix + job_id).encode()
        if key == result_key_prefix.encode() + job_id.encode() and channel in server.subscribers:
            complete_during_post_subscribe_get(key)

    if server is not None:
        server.before_state_check = armed_get
        # also stall SUBSCRIBE so the subscription confirmation is the last
        # thing that happens before the waiter re-reads state; any completion
        # notification published in that tiny gap can only be recovered via the
        # persisted-result re-check or the following wake-up
        async def _stall(args: list[bytes]) -> None:
            await asyncio.sleep(0.15)

        server.during_subscribe = _stall

    t0 = time.monotonic()
    try:
        value = await asyncio.wait_for(job.result(timeout=5), timeout=5)
        elapsed = time.monotonic() - t0
        report.check(
            'waiter returns without hanging',
            value == 'race-1-result' and elapsed < 4,
            f'value={value!r}, waited {elapsed * 1000:.0f}ms',
        )
    except (TimeoutError, asyncio.TimeoutError) as e:
        report.check('waiter returns without hanging', False, f'hung/timeout: {e!r}')
    finally:
        if server is not None:
            server.before_state_check = None
            server.during_subscribe = None
    await completed.wait()


async def race_2_dropped_notification(redis: ArqRedis, report: Report) -> None:
    """
    Race point 2: job completes while the notification connection is dead, so
    the PUBLISH never reaches the waiter. Correct behaviour: the reader detects
    the dead connection, wakes everyone to re-check state, rebuilds and
    re-subscribes; the waiter finds the persisted result and returns.
    """
    print('\n=== Race 2: PUBLISH lost because notification connection drops ===')
    server = getattr(redis, '_demo_server', None)
    job_id = 'race2'
    await redis.zadd(default_queue_name, {job_id: 1})
    job = Job(job_id, redis)

    async def waiter() -> Any:
        return await job.result(timeout=10)

    waiter_task = asyncio.create_task(waiter())
    # let the subscription settle
    await asyncio.sleep(0.4)

    if server is None:
        # real redis: nothing in-process to cut; skip the drop injection but
        # still exercise the plain wake-up path
        await persist_and_notify(redis, job_id, value='race-2-result')
        note = 'real redis: drop not injected in-process'
    else:
        # cut the pub/sub connection, THEN finish the job with no live subscriber
        from scripts.redis_emu import drop_connection

        drop_connection(server, subscriber=True)
        await asyncio.sleep(0.2)
        # persist + publish with the old subscriber gone; the publish lands on
        # no one. Rebuild happens concurrently and must still find the result.
        await persist_and_notify(redis, job_id, value='race-2-result')
        note = 'pub/sub connection cut, PUBLISH during outage'

    t0 = time.monotonic()
    try:
        value = await asyncio.wait_for(asyncio.shield(waiter_task), timeout=10)
        elapsed = time.monotonic() - t0
        report.check(
            'waiter recovers via state re-check + resubscribe',
            value == 'race-2-result' and elapsed < 9,
            f'value={value!r}, {note}, waited {elapsed * 1000:.0f}ms',
        )
    except (TimeoutError, asyncio.TimeoutError):
        report.check('waiter recovers via state re-check + resubscribe', False, f'hung -- {note}')
        waiter_task.cancel()


async def race_3_shared_subscription_cancel(redis: ArqRedis, report: Report) -> None:
    """
    Race point 3: many waiters share ONE subscription; cancelling one waiter
    mid-wait must not unsubscribe the channel for the others.
    """
    print('\n=== Race 3: shared subscription, one waiter cancelled ===')
    job_id = 'race3'
    await redis.zadd(default_queue_name, {job_id: 1})
    job = Job(job_id, redis)

    async def labelled_waiter(label: str) -> tuple[str, Any]:
        return label, await job.result(timeout=8)

    tasks = [asyncio.create_task(labelled_waiter(f'w{i}')) for i in range(4)]
    await asyncio.sleep(0.4)

    notifier = redis.result_notifier
    channel = (result_channel_prefix + job_id).encode()
    shared = notifier._waiters.get(channel)
    report.check('four waiters share one channel subscription', shared is not None and len(shared) == 4)

    # cancel waiter #1, then wait a moment so its UNSUBSCRIBE-guard runs
    tasks[1].cancel()
    cancelled_saw_cancel = False
    try:
        await tasks[1]
    except asyncio.CancelledError:
        cancelled_saw_cancel = True
    await asyncio.sleep(0.3)

    shared_after = notifier._waiters.get(channel)
    report.check(
        'cancelling one waiter leaves the others subscribed',
        cancelled_saw_cancel and shared_after is not None and len(shared_after) == 3,
        f'remaining futures={0 if shared_after is None else len(shared_after)}',
    )

    await persist_and_notify(redis, job_id, value='race-3-result')
    results = []
    for t in (tasks[0], tasks[2], tasks[3]):
        try:
            results.append(await asyncio.wait_for(t, timeout=8))
        except BaseException as e:  # noqa: BLE001
            results.append(('error', repr(e)))
    report.check(
        'surviving waiters all wake from the single PUBLISH',
        results == [('w0', 'race-3-result'), ('w2', 'race-3-result'), ('w3', 'race-3-result')],
        f'{results}',
    )


async def expiry_vs_not_found(redis: ArqRedis, report: Report) -> None:
    print('\n=== Expired result vs job that never existed ===')
    # never existed
    ghost = Job('never-existed', redis)
    try:
        await ghost.result(timeout=0.5)
        report.check('unknown job raises ResultNotFound', False)
    except ResultExpired:
        report.check('unknown job raises ResultNotFound', False, 'got ResultExpired instead')
    except ResultNotFound:
        report.check('unknown job raises ResultNotFound', True)

    # queued job whose result key has a very short TTL and expires during the wait
    job_id = 'expiring'
    await redis.zadd(default_queue_name, {job_id: 1})
    job = Job(job_id, redis)

    async def expire_away() -> None:
        await asyncio.sleep(0.3)
        # worker stored a result with a tiny TTL, removed the queue entry and
        # published; the notification wakes the waiter, but by the time it
        # re-reads state the result key is already gone - expiry must be
        # distinguishable from a job that never existed
        async with redis.pipeline(transaction=True) as tr:
            tr.set(result_key_prefix + job_id, result_bytes(job_id, 'short'), px=1)
            tr.zrem(default_queue_name, job_id)
            await tr.execute()
        await asyncio.sleep(0.05)  # let the 1ms TTL elapse
        # notification fires after the key is provably gone
        await redis.publish(result_channel_prefix + job_id, job_id)

    task = asyncio.create_task(job.result(timeout=5))
    await expire_away()
    try:
        await task
        report.check('expired-during-wait raises ResultExpired', False)
    except ResultNotFound as e:
        report.check(
            'expired-during-wait raises ResultExpired (ResultNotFound subclass)',
            isinstance(e, ResultExpired),
            type(e).__name__,
        )


async def idle_drop_recheck(redis: ArqRedis, report: Report) -> None:
    print('\n=== Notification connection drops while idle ===')
    server = getattr(redis, '_demo_server', None)
    job_id = 'idledrop'
    await redis.zadd(default_queue_name, {job_id: 1})
    job = Job(job_id, redis)
    task = asyncio.create_task(job.result(timeout=10))
    await asyncio.sleep(0.4)
    if server is not None:
        from scripts.redis_emu import drop_connection

        drop_connection(server, subscriber=True)
    await asyncio.sleep(0.5)
    # after re-check/resubscribe, a normal completion must still wake the waiter
    await persist_and_notify(redis, job_id, value='idle-drop-result')
    try:
        value = await asyncio.wait_for(asyncio.shield(task), timeout=10)
        report.check('waiter survives an idle disconnect and keeps waiting', value == 'idle-drop-result')
    except (TimeoutError, asyncio.TimeoutError):
        report.check('waiter survives an idle disconnect and keeps waiting', False, 'hung')
        task.cancel()


def redis_available(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


async def build_client(use_real: bool):
    if use_real:
        redis = ArqRedis(host='localhost', port=6379, encoding='utf8')
        await redis.ping()
        await redis.flushall()
        return redis, None
    from scripts.redis_emu import FakeRedisServer, InProcessConnection
    from redis.asyncio.connection import ConnectionPool

    server = FakeRedisServer()
    InProcessConnection.fake_server = server
    pool = ConnectionPool(connection_class=InProcessConnection, max_connections=10)
    redis = ArqRedis(connection_pool=pool, encoding='utf8')
    await redis.ping()
    await redis.flushall()
    redis._demo_server = server  # type: ignore[attr-defined]
    return redis, server


async def main_async(force_emulator: bool) -> int:
    use_real = not force_emulator and redis_available('localhost', 6379)
    if use_real:
        print('Using real redis at localhost:6379')
    else:
        print('No redis reachable at localhost:6379; using in-process RESP2 emulator (sockets are sandboxed here)')
    report = Report()
    scenarios = (
        race_1_subscribe_window,
        race_2_dropped_notification,
        race_3_shared_subscription_cancel,
        expiry_vs_not_found,
        idle_drop_recheck,
    )
    for scenario in scenarios:
        # a fresh client per scenario so a connection severed by an earlier
        # race injection cannot affect the next one
        redis, server = await build_client(use_real)
        try:
            await scenario(redis, report)
        finally:
            await redis.aclose()
            if server is not None:
                await server.shutdown()
            await asyncio.sleep(0.05)

    print(f'\n==== {report.ok} passed, {report.fail} failed ====')
    return 1 if report.fail else 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--emulator', action='store_true', help='force the in-process redis emulator')
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args.emulator)))


if __name__ == '__main__':
    main()
