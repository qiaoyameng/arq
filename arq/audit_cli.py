from __future__ import annotations

import asyncio
import json
import sys

import click

from .audit import AuditConfigurationError, AuditReport, redis_settings_from_url, run_apply, run_audit
from .constants import default_queue_name


def _print_report(report: AuditReport, plan_actions: int | None = None) -> None:
    click.echo('arq audit report')
    click.echo(f'queues: {", ".join(report.queues) if report.queues else "none"}')
    for status, count in sorted(report.counts.items()):
        click.echo(f'  {status}: {count}')

    for warning in report.warnings:
        click.echo(f'warning: {warning}')

    if report.findings:
        click.echo('\nfindings:')
        for finding in report.findings:
            queues = ', '.join(f'{queue}@{score}' for queue, score in finding.queues.items()) or 'none'
            click.echo(f'  {finding.job_id} {finding.status.value} queues={queues}')
            if finding.reason:
                click.echo(f'    {finding.reason}')

    if plan_actions is not None:
        click.echo(f'\nrepair actions in plan: {plan_actions}')


@click.command('arq-audit')
@click.option('--redis-url', envvar='ARQ_REDIS_URL', help='Redis DSN, for example redis://localhost:6379/0.')
@click.option('--queue', 'queues', multiple=True, help='Queue to include; repeatable for custom queue names.')
@click.option('--queue-prefix', default=default_queue_name, show_default=True, help='Prefix used to discover queues.')
@click.option('--scan-count', default=500, show_default=True, type=click.IntRange(1), help='Redis SCAN count hint.')
@click.option('--write-plan', type=click.Path(dir_okay=False), help='Write a limited repair plan without applying it.')
@click.option('--max-items', type=click.IntRange(1), help='Maximum repair actions to place in the plan.')
@click.option('--apply-plan', type=click.Path(exists=True, dir_okay=False), help='Re-check and apply an existing plan.')
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON.')
def cli(
    *,
    redis_url: str | None,
    queues: tuple[str, ...],
    queue_prefix: str,
    scan_count: int,
    write_plan: str | None,
    max_items: int | None,
    apply_plan: str | None,
    as_json: bool,
) -> None:
    """Report arq queue residue and optionally prepare conservative repairs."""
    settings = redis_settings_from_url(redis_url)
    if apply_plan:
        try:
            result = asyncio.run(run_apply(settings, apply_plan, queue_prefix=queue_prefix, scan_count=scan_count))
        except (OSError, AuditConfigurationError, ValueError, json.JSONDecodeError) as exc:
            click.echo(f'invalid repair plan: {exc}', err=True)
            sys.exit(2)
        payload = {
            'applied': result.applied,
            'skipped': result.skipped,
            'applied_count': len(result.applied),
            'skipped_count': len(result.skipped),
        }
        if as_json:
            click.echo(json.dumps(payload, indent=2, sort_keys=True))
        else:
            click.echo(f'applied: {len(result.applied)}')
            click.echo(f'skipped: {len(result.skipped)}')
            for action in result.skipped:
                click.echo(f'  skipped {action["type"]} {action["job_id"]}: {action.get("reason", "skipped")}')
        return

    try:
        report, plan = asyncio.run(
            run_audit(
                settings,
                queues=queues,
                queue_prefix=queue_prefix,
                plan_path=write_plan,
                max_items=max_items,
                scan_count=scan_count,
            )
        )
    except (OSError, AuditConfigurationError, ValueError, json.JSONDecodeError) as exc:
        click.echo(f'audit failed: {exc}', err=True)
        sys.exit(2)
    if as_json:
        payload = report.to_dict()
        if plan is not None:
            payload['plan_path'] = write_plan
            payload['plan_action_count'] = len(plan['actions'])
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_report(report, None if plan is None else len(plan['actions']))
        if write_plan:
            click.echo(f'repair plan written to {write_plan}; no changes were made in Redis')
