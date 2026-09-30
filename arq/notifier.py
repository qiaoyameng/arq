import asyncio
import logging
from typing import TYPE_CHECKING, Optional

from redis.asyncio.client import PubSub
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError, TimeoutError as RedisTimeoutError

from .constants import result_channel_prefix

logger = logging.getLogger('arq.notifier')

if TYPE_CHECKING:
    from redis.asyncio import Redis


def result_channel(job_id: str) -> bytes:
    return (result_channel_prefix + job_id).encode()


class ResultNotifier:
    """
    Shared pub/sub wake-up service for jobs waiting on results.

    All waiters on a job share one subscription on a single, lazily created
    pub/sub connection. Cancelling one waiter only removes that waiter and the
    channel is unsubscribed only when its last waiter goes away.

    Notifications are purely a wake-up signal: waiters always re-read the
    persisted result (and queue score) after waking, so a notification that is
    lost or delivered before the subscription was confirmed cannot cause a
    hang - every waiter re-checks state immediately after subscribing.

    If the notification connection drops, every pending waiter is woken to
    re-check state, a fresh connection is built and all channels are
    re-subscribed; new waiters arriving during a rebuild wait for it.
    """

    def __init__(self, redis: 'Redis[bytes]') -> None:
        self._redis = redis
        self._waiters: dict[bytes, set[asyncio.Future[None]]] = {}
        # channels actually subscribed on the current pubsub connection
        self._subscribed: set[bytes] = set()
        # channels with a SUBSCRIBE in flight, so concurrent waiters on the
        # same channel wait instead of issuing duplicate subscriptions
        self._subscribing: dict[bytes, asyncio.Future[None]] = {}
        self._lock = asyncio.Lock()
        self._pubsub: Optional[PubSub] = None
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._rebuild_task: Optional[asyncio.Task[None]] = None
        self._closed = False

    def wait_for(self, job_id: str) -> '_Waiter':
        return _Waiter(self, result_channel(job_id))

    async def _subscribe(self, channel: bytes, future: asyncio.Future[None]) -> None:
        """
        Register a waiter and make sure its channel is subscribed.

        The waiter is registered before any network work and the caller always
        re-reads job state after this returns, so a completion landing between
        the pre-subscribe check and the confirmed subscription is caught either
        by the post-subscribe re-check or by the notification waking the
        already-registered future.
        """
        while not self._closed:
            async with self._lock:
                self._waiters.setdefault(channel, set()).add(future)
                if self._pubsub is None:
                    self._pubsub = self._redis.pubsub()
                pubsub = self._pubsub
                rebuild = self._rebuild_task
                already = channel in self._subscribed
                in_flight = self._subscribing.get(channel)
                if in_flight is None and not already:
                    in_flight = asyncio.get_running_loop().create_future()
                    self._subscribing[channel] = in_flight
                    do_subscribe = True
                else:
                    do_subscribe = False

            if rebuild is not None:
                # wait (outside the lock) for the dead connection to be
                # rebuilt, then loop to use the new pubsub
                await rebuild
                if future.done():
                    return
                continue

            if not do_subscribe and in_flight is not None:
                # another waiter is subscribing this channel for us
                try:
                    await in_flight
                except asyncio.CancelledError:
                    # the in-flight SUBSCRIBE was invalidated by a rebuild
                    await asyncio.sleep(0)
                continue

            if do_subscribe:
                try:
                    await pubsub.subscribe(channel)
                except (RedisConnectionError, RedisTimeoutError, RedisError, OSError):
                    async with self._lock:
                        fut = self._subscribing.pop(channel, None)
                    if fut is not None and not fut.done():
                        fut.set_exception(asyncio.CancelledError())
                    await self._start_rebuild()
                    continue
                async with self._lock:
                    if self._pubsub is not pubsub or self._rebuild_task is not None:
                        # a rebuild swapped the pubsub while we were
                        # subscribing; drop this attempt and loop
                        fut = self._subscribing.pop(channel, None)
                        if fut is not None and not fut.done():
                            fut.set_exception(asyncio.CancelledError())
                        await self._start_rebuild()
                        continue
                    self._subscribed.add(channel)
                    self._subscribing.pop(channel, None)
                    in_flight.set_result(None)
                    if self._reader_task is None or self._reader_task.done():
                        self._reader_task = asyncio.create_task(self._reader(pubsub))

            return

        raise RuntimeError('ResultNotifier is closed')

    async def _unsubscribe(self, channel: bytes, future: asyncio.Future[None]) -> None:
        """
        Remove one waiter; only the last waiter of a channel unsubscribes, so
        cancelling one waiter never disturbs the others sharing the channel.
        """
        pubsub: Optional[PubSub]

        async def _cleanup() -> None:
            nonlocal pubsub
            async with self._lock:
                waiters = self._waiters.get(channel)
                if waiters is None or future not in waiters:
                    pubsub = None
                    return
                waiters.discard(future)
                if waiters:
                    pubsub = None
                    return
                self._waiters.pop(channel, None)
                self._subscribed.discard(channel)
                pubsub = self._pubsub

        task = asyncio.create_task(_cleanup())
        pubsub = None
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # cancelling one waiter must never tear down the shared connection
            await asyncio.shield(task)
            raise
        if pubsub is not None and channel not in self._subscribed:
            try:
                await pubsub.unsubscribe(channel)
            except (RedisError, OSError):
                # connection is dead / rebuilding; with no waiters left the
                # rebuild simply never resubscribes this channel
                logger.debug('unsubscribe of last waiter on %r failed', channel, exc_info=True)

    def _wake(self, channel: object) -> None:
        key = channel if isinstance(channel, bytes) else str(channel).encode()
        for future in tuple(self._waiters.get(key, ())):
            if not future.done():
                future.set_result(None)

    def _wake_all(self) -> None:
        for waiters in self._waiters.values():
            for future in tuple(waiters):
                if not future.done():
                    future.set_result(None)

    async def _reader(self, pubsub: PubSub) -> None:
        try:
            while not self._closed:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is not None and message.get('type') == 'message':
                    self._wake(message.get('channel'))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self._closed:
                return
            logger.warning('result notification connection lost, re-checking state: %s', e)
            # wake everyone first: a notification could have been lost during
            # the drop, so waiters must re-read persisted state
            self._wake_all()
            await self._start_rebuild()
            # _rebuild spawns a fresh reader for the fresh connection

    async def _start_rebuild(self) -> None:
        async with self._lock:
            if self._rebuild_task is None:
                self._rebuild_task = asyncio.create_task(self._rebuild())
            rebuild_task = self._rebuild_task
        await rebuild_task

    async def _rebuild(self) -> None:
        delay = 0.05
        try:
            while not self._closed:
                async with self._lock:
                    channels = list(self._waiters.keys())
                    if not channels:
                        self._rebuild_task = None
                        return
                    old = self._pubsub
                # close the dead pubsub (releasing its connection back to the
                # pool) BEFORE creating a new one, otherwise the pool can hand
                # out the same dead connection
                if old is not None:
                    try:
                        await old.aclose()
                    except (RedisError, OSError):
                        pass
                pubsub = self._redis.pubsub()
                try:
                    await pubsub.subscribe(*channels)
                except (RedisError, OSError) as e:
                    await pubsub.aclose()
                    logger.warning('could not resubscribe result notifications: %s', e)
                else:
                    async with self._lock:
                        self._pubsub = pubsub
                        self._subscribed = set(channels)
                        self._subscribing.clear()
                        self._reader_task = asyncio.create_task(self._reader(pubsub))
                        # self-referential task is finishing; clear it under the
                        # lock so arriving waiters do not keep seeing a stale
                        # completed rebuild
                        self._rebuild_task = None
                    # make every waiter re-read state once the new connection is
                    # live: anything that completed during the outage has a
                    # persisted result and possibly a lost notification
                    self._wake_all()
                    return
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.0)
        finally:
            async with self._lock:
                if self._rebuild_task is not None and self._rebuild_task.done():
                    self._rebuild_task = None

    async def aclose(self) -> None:
        async with self._lock:
            self._closed = True
            for waiters in self._waiters.values():
                for future in tuple(waiters):
                    if not future.done():
                        future.set_exception(RuntimeError('ResultNotifier closed'))
            self._waiters.clear()
            self._pubsub, pubsub = None, self._pubsub
            reader = self._reader_task
            rebuild = self._rebuild_task
        if reader is not None:
            reader.cancel()
        if rebuild is not None:
            rebuild.cancel()
        if pubsub is not None:
            try:
                await pubsub.aclose()
            except (RedisError, OSError):
                pass


class _Waiter:
    """
    One waiter on one result channel.

    ``wait`` returns when the result may be available (notification or a
    disconnect-induced re-check); the caller re-reads the persisted result
    afterwards. Cancelling the coroutine using it removes only this waiter.
    """

    __slots__ = ('notifier', 'channel', 'future')

    def __init__(self, notifier: ResultNotifier, channel: bytes) -> None:
        self.notifier = notifier
        self.channel = channel
        self.future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def subscribe(self) -> None:
        await self.notifier._subscribe(self.channel, self.future)

    async def wait(self, timeout: Optional[float]) -> bool:
        """
        Wait for a wake-up. Returns ``True`` when woken, ``False`` on timeout.

        Uses ``asyncio.wait`` rather than ``wait_for``+shield so a timeout never
        cancels the shared future: doing so would force a re-registration on the
        next loop iteration and could miss the notification already in flight.
        """
        if self.future.done():
            self.future.result()
            return True
        if timeout is None:
            await self.future
            return True
        done, _ = await asyncio.wait({self.future}, timeout=timeout)
        if self.future in done:
            self.future.result()
            return True
        return False

    def detach_cancelled(self) -> None:
        """Remove a cancelled future before any late wake-up can touch it."""
        waiters = self.notifier._waiters.get(self.channel)
        if waiters is not None:
            waiters.discard(self.future)

    async def release(self) -> None:
        if self.future.done():
            # a genuine wake-up resolves every future on the channel; leave it
            # registered so the other waiters keep the shared subscription
            return
        self.future.cancel()
        self.detach_cancelled()
        await self.notifier._unsubscribe(self.channel, self.future)
