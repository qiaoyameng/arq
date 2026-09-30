from click.testing import CliRunner

from arq.doctor import doctor


def test_doctor_help():
    result = CliRunner().invoke(doctor, ['--help'])

    assert result.exit_code == 0
    assert 'report arq residue' in result.output.lower()
    assert '--apply-plan' in result.output
    assert '--max-actions' in result.output


def test_doctor_requires_queue_when_discovery_is_disabled():
    result = CliRunner().invoke(doctor, ['--no-discover-queues'])

    assert result.exit_code != 0
    assert '--no-discover-queues requires at least one --queue' in result.output
