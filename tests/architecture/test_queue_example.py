"""The queue workflow example (job file, Dockerfile, compose file) must not rot.

Docker is not needed: these check that the files agree with each other and with the code. Running
them for real is on ``docs/verify-on-your-machine.md``.
"""

import re
import tomllib
from pathlib import Path

from bitdrop_cli.jobfile import load_job

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "examples" / "queue"
DOCKERFILE = ROOT / "docker" / "Dockerfile"
ENVIRONMENT = {
    "SOURCE_URL": "postgresql://u:p@source-db/shop",
    "DESTINATION_URL": "postgresql://u:p@destination-db/shop",
    "STAGING_URL": "redis://staging:6379/0",
    "COORDINATION_URL": "redis://coordination:6379/0",
}


def test_the_job_file_is_a_valid_queue_job() -> None:
    job = load_job(EXAMPLE / "job.toml", environ=ENVIRONMENT)

    assert job.metadata is None
    assert job.require_queues().work == "redis://coordination:6379/0?bitdrop_queue=work"
    assert job.require_queues().files == "redis://coordination:6379/0?bitdrop_queue=files"
    assert job.require_markers().url == "redis://coordination:6379/0"
    assert job.consumer_config().max_attempts == 3


def test_the_job_file_reads_the_variables_the_compose_file_provides() -> None:
    wanted = set(re.findall(r"\$\{(\w+)\}", (EXAMPLE / "job.toml").read_text()))
    compose = (EXAMPLE / "docker-compose.yml").read_text()

    assert wanted == set(ENVIRONMENT)
    for name in wanted:
        assert f"{name}:" in compose, f"docker-compose.yml does not set {name}"


def test_the_compose_file_runs_every_role_with_the_commands_the_tool_has() -> None:
    compose = (EXAMPLE / "docker-compose.yml").read_text()

    for role in ("plan", "source", "destination"):
        assert re.search(rf'command: \["queue", "{role}", "/job.toml"', compose), role
    assert compose.count("--stop-when-finished") == 2


def test_the_files_the_compose_file_mounts_exist() -> None:
    compose = (EXAMPLE / "docker-compose.yml").read_text()

    for mounted in re.findall(r"- \./([\w.-]+):", compose):
        assert (EXAMPLE / mounted).is_file(), mounted


def test_the_source_table_has_an_integer_primary_key() -> None:
    sql = (EXAMPLE / "init-source.sql").read_text()

    assert "bigint PRIMARY KEY" in sql
    assert "public.orders" in sql


def test_the_image_installs_the_packages_the_example_needs_and_they_exist() -> None:
    installed = re.findall(r"\./packages/([\w-]+)", DOCKERFILE.read_text())

    for needed in (
        "bitdrop-cli",
        "bitdrop-core",
        "bitdrop-api",
        "bitdrop-queue-redis",
        "bitdrop-marker-redis",
        "bitdrop-store-redis",
        "bitdrop-connector-postgres",
    ):
        assert needed in installed, f"the image does not install {needed}"
    for package in installed:
        assert (ROOT / "packages" / package / "pyproject.toml").is_file(), package
    assert "bitdrop-tui" not in installed, "a role image needs no display toolkit"


def test_the_image_does_not_run_as_root_and_starts_the_tool() -> None:
    text = DOCKERFILE.read_text()

    assert re.search(r"^USER \d+$", text, re.MULTILINE)
    assert 'ENTRYPOINT ["bitdrop"]' in text


def test_the_installed_adapters_are_named_by_the_schemes_the_example_uses() -> None:
    """The URLs say redis:// and postgresql://; those packages must provide them."""
    schemes = {
        "bitdrop-queue-redis": "redis",
        "bitdrop-marker-redis": "redis",
        "bitdrop-store-redis": "redis",
        "bitdrop-connector-postgres": "postgresql",
    }
    for package, scheme in schemes.items():
        project = tomllib.loads((ROOT / "packages" / package / "pyproject.toml").read_text())
        entries = project["project"]["entry-points"]
        assert any(scheme in group for group in entries.values()), package
