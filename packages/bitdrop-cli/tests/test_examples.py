"""The example job files shown in the documentation must keep working."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from bitdrop_cli.app import app
from bitdrop_cli.jobfile import load_job

EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
JOB_FILES = sorted(EXAMPLES.glob("*.toml"))


def test_there_are_examples_to_check() -> None:
    assert JOB_FILES, f"no example job files found in {EXAMPLES}"


@pytest.mark.parametrize("path", JOB_FILES, ids=lambda path: path.name)
class TestEveryExample:
    def test_it_is_a_valid_job_file(self, path: Path) -> None:
        job = load_job(path, environ={})

        assert job.transfer_config().job_id

    def test_it_plans(self, path: Path) -> None:
        result = CliRunner().invoke(app, ["plan", str(path)])

        assert result.exit_code == 0, result.stderr
        assert "chunks" in result.stdout
