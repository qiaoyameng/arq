from arq import interval


async def maintenance(ctx):
    print('maintenance complete')


class WorkerSettings:
    interval_jobs = [
        interval(
            maintenance,
            interval=600,
            run_at_startup=True,
            run_after_success=True,
            run_after_failure=False,
            run_after_cancellation=False,
        )
    ]
