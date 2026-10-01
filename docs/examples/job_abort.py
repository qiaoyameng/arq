import asyncio
from arq import create_pool
from arq.connections import RedisSettings


async def do_stuff(ctx):
    print('doing stuff...')
    await asyncio.sleep(10)
    return 'stuff done'


async def main():
    redis = await create_pool(RedisSettings())
    job = await redis.enqueue_job('do_stuff')
    await asyncio.sleep(1)
    result = await job.abort(request_id='ui-42', reason='user clicked cancel')
    print(result.status, result.request_id, result.reason)


class WorkerSettings:
    functions = [do_stuff]
    allow_abort_jobs = True

    async def on_job_abort(ctx):
        print('cleaning up', ctx['abort_request_id'], ctx['abort_reason'])


if __name__ == '__main__':
    asyncio.run(main())
