from click.testing import CliRunner

from arq import audit, audit_cli
from arq.audit_cli import cli


def test_audit_cli_help():
    result = CliRunner().invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert '--write-plan' in result.output
    assert '--apply-plan' in result.output


def test_audit_cli_report(monkeypatch):
    findings = [
        audit.AuditFinding('one', audit.AuditStatus.orphan_payload),
        audit.AuditFinding('two', audit.AuditStatus.orphan_payload),
    ]
    report = audit.AuditReport(scanned_at_ms=1, queues=['arq:queue'], findings=findings)

    async def fake_run_audit(*args, **kwargs):
        return report, None

    monkeypatch.setattr(audit_cli, 'run_audit', fake_run_audit)
    result = CliRunner().invoke(cli, ['--redis-url', 'redis://localhost:6379/1'])
    assert result.exit_code == 0
    assert 'arq audit report' in result.output


def test_audit_cli_limited_plan(monkeypatch, tmp_path):
    findings = [
        audit.AuditFinding('one', audit.AuditStatus.orphan_payload),
        audit.AuditFinding('two', audit.AuditStatus.orphan_payload),
    ]
    report = audit.AuditReport(scanned_at_ms=1, queues=['arq:queue'], findings=findings)
    plan_path = tmp_path / 'plan.json'

    async def fake_run_audit(*args, **kwargs):
        assert kwargs['max_items'] == 1
        plan = audit.build_fix_plan(report, max_items=1)
        audit.write_fix_plan(kwargs['plan_path'], plan)
        return report, plan

    monkeypatch.setattr(audit_cli, 'run_audit', fake_run_audit)
    result = CliRunner().invoke(cli, ['--write-plan', str(plan_path), '--max-items', '1'])

    assert result.exit_code == 0
    assert plan_path.exists()
    assert 'repair actions in plan: 1' in result.output


def test_audit_cli_apply(monkeypatch, tmp_path):
    fix_result = audit.FixResult(skipped=[{'type': 'delete_job_key', 'job_id': 'changed'}])

    async def fake_run_apply(*args, **kwargs):
        return fix_result

    monkeypatch.setattr(audit_cli, 'run_apply', fake_run_apply)
    runner = CliRunner()
    plan_path = tmp_path / 'plan.json'
    plan_path.write_text('{"version": 1, "actions": []}')
    result = runner.invoke(cli, ['--apply-plan', str(plan_path)])

    assert result.exit_code == 0
    assert 'applied: 0' in result.output
    assert 'skipped: 1' in result.output
