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
    # every abort request can carry an id and a reason; the receipt reports what actually happened:
    # before_start / confirmed / already_finished / timeout
    receipt = await job.abort(request_id='request-1', reason='no longer needed')
    print(receipt.status, receipt.effective_reason)


class WorkerSettings:
    functions = [do_stuff]
    allow_abort_jobs = True


if __name__ == '__main__':
    asyncio.run(main())
