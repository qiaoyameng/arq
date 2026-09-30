from datetime import timedelta

from arq import interval


async def run_after_previous_finishes(ctx):
    print('run ten minutes after the previous execution finished')


class WorkerSettings:
    cron_jobs = [
        interval(
            run_after_previous_finishes,
            interval=timedelta(minutes=10),
            continue_on_failure=True,
            continue_on_cancel=True,
        )
    ]
