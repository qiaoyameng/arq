import asyncio
import json
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

import click

from .connections import RedisSettings, create_pool
from .maintenance import (
    MaintenancePlan,
    MaintenanceReport,
    apply_maintenance_plan,
    inspect_jobs,
    make_maintenance_plan,
)


@click.command('arq-doctor')
@click.option('--redis-url', default='redis://localhost:6379/0', help='Redis URL to inspect.')
@click.option('--queue', 'queue_names', multiple=True, help='Additional queue key to scan. Can be used repeatedly.')
@click.option('--no-discover-queues', is_flag=True, help='Only scan queues supplied with --queue.')
@click.option('--plan', 'plan_path', type=click.Path(dir_okay=False), help='Write a bounded repair plan as JSON.')
@click.option(
    '--apply-plan',
    'apply_plan_path',
    type=click.Path(exists=True, dir_okay=False),
    help='Apply a repair plan.',
)
@click.option('--max-actions', default=100, show_default=True, type=click.IntRange(min=1), help='Maximum plan actions.')
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of a text report.')
def doctor(
    *,
    redis_url: str,
    queue_names: tuple[str, ...],
    no_discover_queues: bool,
    plan_path: str,
    apply_plan_path: str,
    max_actions: int,
    as_json: bool,
) -> None:
    """Report arq residue and optionally prepare or apply a bounded repair plan."""
    if no_discover_queues and not queue_names:
        raise click.UsageError('--no-discover-queues requires at least one --queue.')
    if plan_path and apply_plan_path:
        raise click.UsageError('--plan and --apply-plan cannot be used together.')
    raise SystemExit(
        asyncio.run(
            _run(
                redis_url=redis_url,
                queue_names=queue_names,
                discover_queues=not no_discover_queues,
                plan_path=plan_path,
                apply_plan_path=apply_plan_path,
                max_actions=max_actions,
                as_json=as_json,
            )
        )
    )


async def _run(
    *,
    redis_url: str,
    queue_names: tuple[str, ...],
    discover_queues: bool,
    plan_path: str,
    apply_plan_path: str,
    max_actions: int,
    as_json: bool,
) -> int:
    settings = RedisSettings.from_dsn(redis_url)
    redis = await create_pool(settings)
    try:
        if apply_plan_path:
            with open(apply_plan_path) as f:
                plan = MaintenancePlan.from_dict(json.load(f))
            result = await apply_maintenance_plan(redis, plan)
            payload: Any = result
            if not as_json:
                _print_apply_result(result)
            exit_code = 1 if result.failed else 0
        else:
            report = await inspect_jobs(redis, queue_names=queue_names, discover_queues=discover_queues)
            plan = make_maintenance_plan(report, max_actions=max_actions)
            payload = {'report': report, 'plan': plan}
            if plan_path:
                with open(plan_path, 'w') as f:
                    json.dump(_to_jsonable(plan), f, indent=2, sort_keys=True)
                    f.write('\n')
            if not as_json:
                _print_report(report, plan, plan_path)
            exit_code = 0

        if as_json:
            click.echo(json.dumps(_to_jsonable(payload), indent=2, sort_keys=True))
        return exit_code
    finally:
        await redis.close(close_connection_pool=True)


def _print_report(report: MaintenanceReport, plan: MaintenancePlan, plan_path: str) -> None:
    click.echo('Scanned queues: ' + ', '.join(report.queues))
    for category, count in sorted(report.category_counts.items()):
        click.echo(f'{category}: {count}')
    for status, count in sorted(report.counts.items()):
        click.echo(f'  {status}: {count}')

    if report.issues:
        click.echo(f'issues: {len(report.issues)}')
        for issue in report.issues:
            click.echo(f'  {issue.kind}: {issue.key} ({issue.detail})')

    click.echo(f'planned actions: {len(plan.actions)}')
    for action in plan.actions:
        click.echo(f'  {action.operation}: {action.job_id}')
    if plan_path:
        click.echo(f'plan written to: {plan_path}')
    else:
        click.echo('report only; no Redis keys were modified')


def _print_apply_result(result: Any) -> None:
    click.echo(f'applied: {len(result.applied)}')
    click.echo(f'skipped: {len(result.skipped)}')
    click.echo(f'failed: {len(result.failed)}')
    for group in (result.applied, result.skipped, result.failed):
        for item in group:
            click.echo(f'  {item.operation} {item.job_id}: {item.reason}')


def _to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _to_jsonable(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item) for item in value]
    return value


if __name__ == '__main__':
    doctor()
