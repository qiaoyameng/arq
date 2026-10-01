import asyncio

from arq import create_pool
from arq.connections import RedisSettings


async def main():
    redis = await create_pool(RedisSettings())
    # jobs sharing a serial group name are executed one at a time, in enqueue
    # order; different groups (and jobs without one) still run in parallel
    for account_id in ('acct-1', 'acct-1', 'acct-2'):
        await redis.enqueue_job('process_account', account_id, _serial_group=account_id)


class WorkerSettings:
    functions = []  # your "process_account" coroutine
    # serial groups work alongside the normal concurrency limit;
    # do NOT set max_jobs=1, that would serialise every job
    max_jobs = 10


if __name__ == '__main__':
    asyncio.run(main())
