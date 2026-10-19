"""The commands, run the way a person runs them: through the real argument parser and registry."""

import asyncio
import json
import logging
from collections.abc import Callable, Iterable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from typer.testing import CliRunner

from bitdrop_api.plan import encode_plan
from bitdrop_api.source import ChunkSpec, SourcePlan
from bitdrop_cli.app import app
from bitdrop_cli.plain import create_ui as create_plain_ui
from bitdrop_connector_memory import (
    MemorySource,
    create_destination,
    create_source,
    demo_table,
)
from bitdrop_core import registry
from bitdrop_core.progress import Snapshot, TransferEvent
from bitdrop_core.transfer import TransferReport
from bitdrop_metadata_memory import create_store as create_metadata_store
from bitdrop_store_memory import create_store as create_chunk_store

runner = CliRunner()

JOB = """
[job]
id = "orders-1"

[source]
url = "{source_url}"
table = "{source_table}"

[destination]
url = "{destination_url}"
table = "orders"

[staging]
url = "memory://"

[metadata]
url = "{metadata_url}"

[tuning]
chunk_mib = 0.1
extractors = 2
loaders = 2
"""


async def save_a_plan(metadata_url: str, job_id: str, *, table: str) -> None:
    """Leave a plan for ``table`` in the metadata store, as an earlier run of the job would have."""
    store = registry.open_metadata_store(metadata_url)
    try:
        plan = SourcePlan(table, pa.schema([pa.field("id", pa.int64())]), ())
        await store.save_plan(job_id, encode_plan(plan))
    finally:
        await store.aclose()


def job_file(
    tmp_path: Path,
    *,
    rows: int = 20_000,
    source_url: str | None = None,
    source_table: str = "demo",
    destination_url: str = "memory://",
    metadata_url: str = "memory://",
    extra: str = "",
) -> Path:
    path = tmp_path / "job.toml"
    path.write_text(
        JOB.format(
            source_url=source_url or f"memory://?demo_rows={rows}",
            source_table=source_table,
            destination_url=destination_url,
            metadata_url=metadata_url,
        )
        + extra
    )
    return path


def invoke(*args: str, env: dict[str, str] | None = None) -> Any:
    return runner.invoke(app, list(args), env=env)


class TestComponents:
    def test_lists_what_is_installed_for_every_kind(self) -> None:
        result = invoke("components")

        assert result.exit_code == 0
        by_kind = {
            line.split(None, 1)[0]: line for line in result.stdout.splitlines() if line.strip()
        }
        assert "memory" in by_kind["sources"]
        assert "memory" in by_kind["destinations"]
        assert "memory" in by_kind["staging"]
        assert "memory" in by_kind["metadata"]
        assert "memory" in by_kind["queues"]
        assert "memory" in by_kind["markers"]
        assert "plain" in by_kind["displays"]
        assert "json" in by_kind["displays"]


class TestVersion:
    def test_it_prints_the_version(self) -> None:
        result = invoke("--version")

        assert result.exit_code == 0
        assert result.stdout.startswith("bitdrop ")

    def test_without_arguments_it_explains_itself(self) -> None:
        result = invoke()

        assert "run" in result.output
        assert "plan" in result.output


class TestRun:
    def test_a_job_runs_and_the_summary_is_the_output(self, tmp_path: Path) -> None:
        result = invoke("run", str(job_file(tmp_path)), "--ui", "plain")

        assert result.exit_code == 0, result.stderr
        assert "orders-1: all" in result.stdout
        assert "20,000" in result.stdout
        assert "chunks" in result.stderr, "progress goes to the other stream"
        assert "orders-1: demo -> orders" in result.stderr

    def test_json_output_is_one_object_per_line_ending_in_the_report(self, tmp_path: Path) -> None:
        result = invoke("run", str(job_file(tmp_path)), "--ui", "json")

        assert result.exit_code == 0, result.stderr
        events = [json.loads(line) for line in result.stdout.splitlines()]
        assert events[0]["event"] == "plan"
        assert events[-1]["event"] == "report"
        assert events[-1]["succeeded"] is True
        assert events[-1]["rows_loaded"] == 20_000
        assert events[0]["chunks"] > 1

    def test_the_job_id_can_be_overridden(self, tmp_path: Path) -> None:
        result = invoke("run", str(job_file(tmp_path)), "--ui", "plain", "--job-id", "again")

        assert result.exit_code == 0, result.stderr
        assert "again: all" in result.stdout

    def test_worker_counts_can_be_overridden(self, tmp_path: Path) -> None:
        result = invoke("run", str(job_file(tmp_path)), "--ui", "json", "--extractors", "3")

        assert json.loads(result.stdout.splitlines()[0])["extractors"] == 3

    def test_without_a_terminal_auto_means_plain_text(self, tmp_path: Path) -> None:
        result = invoke("run", str(job_file(tmp_path)))

        assert result.exit_code == 0, result.stderr
        assert "orders-1: all" in result.stdout

    def test_an_unknown_table_is_refused_before_anything_moves(self, tmp_path: Path) -> None:
        result = invoke("run", str(job_file(tmp_path, source_table="nope")), "--ui", "plain")

        assert result.exit_code == 3
        assert "error:" in result.stderr
        assert "nope" in result.stderr
        assert "Traceback" not in result.stderr

    def test_a_job_id_saved_for_another_table_is_refused_with_advice(self, tmp_path: Path) -> None:
        url = f"sqlite://{tmp_path}/state.db"
        asyncio.run(save_a_plan(url, "orders-1", table="elsewhere"))

        result = invoke("run", str(job_file(tmp_path, metadata_url=url)), "--ui", "plain")

        assert result.exit_code == 3
        assert "elsewhere" in result.stderr
        assert "new job id" in result.stderr
        assert "Traceback" not in result.stderr

    def test_a_connector_that_is_not_installed_is_a_configuration_error(
        self, tmp_path: Path
    ) -> None:
        result = invoke("run", str(job_file(tmp_path, source_url="nowhere://x")), "--ui", "plain")

        assert result.exit_code == 2
        assert "nowhere" in result.stderr
        assert "memory" in result.stderr

    def test_a_bad_url_parameter_is_a_configuration_error(self, tmp_path: Path) -> None:
        result = invoke(
            "run", str(job_file(tmp_path, source_url="memory://?bogus=1")), "--ui", "plain"
        )

        assert result.exit_code == 2
        assert "bogus" in result.stderr

    def test_a_missing_job_file_is_a_usage_error(self, tmp_path: Path) -> None:
        result = invoke("run", str(tmp_path / "missing.toml"))

        assert result.exit_code == 2
        assert "missing.toml" in result.output

    def test_a_broken_job_file_lists_its_problems(self, tmp_path: Path) -> None:
        path = tmp_path / "job.toml"
        path.write_text('[job]\nid = "x"\n')

        result = invoke("run", str(path))

        assert result.exit_code == 2
        assert "source" in result.stderr
        assert "destination" in result.stderr

    def test_an_unknown_display_lists_the_installed_ones(self, tmp_path: Path) -> None:
        result = invoke("run", str(job_file(tmp_path)), "--ui", "hologram")

        assert result.exit_code == 2
        assert "hologram" in result.stderr
        assert "plain" in result.stderr

    def test_the_tui_needs_a_terminal(self, tmp_path: Path) -> None:
        result = invoke("run", str(job_file(tmp_path)), "--ui", "tui")

        assert result.exit_code == 2
        assert "terminal" in result.stderr or "tui" in result.stderr

    def test_passwords_from_the_environment_never_appear_in_the_output(
        self, tmp_path: Path
    ) -> None:
        path = job_file(tmp_path, source_url="${SRC}", source_table="nope")

        result = invoke("run", str(path), "--ui", "plain", env={"SRC": "memory://?demo_rows=10"})

        assert "?demo_rows" not in result.output
        unset = invoke("run", str(path), "--ui", "plain")
        assert unset.exit_code == 2
        assert "SRC" in unset.stderr

    def test_a_url_with_a_password_is_never_echoed_in_an_error(self, tmp_path: Path) -> None:
        path = job_file(tmp_path, source_url="nowhere://alice:s3cret@host/db")

        result = invoke("run", str(path), "--ui", "plain")

        assert result.exit_code == 2
        assert "s3cret" not in result.output
        assert "s3cret" not in result.stderr


class TestPlan:
    def test_it_describes_what_a_run_would_do(self, tmp_path: Path) -> None:
        result = invoke("plan", str(job_file(tmp_path)))

        assert result.exit_code == 0, result.stderr
        assert "demo" in result.stdout
        assert "20,000 rows" in result.stdout
        assert "customer" in result.stdout
        assert "string" in result.stdout
        assert "chunks" in result.stdout

    def test_it_never_touches_the_destination(self, tmp_path: Path) -> None:
        """A destination that cannot even be opened proves nothing was done to it."""
        result = invoke("plan", str(job_file(tmp_path, destination_url="nowhere://x")))

        assert result.exit_code == 0, result.stderr

    def test_json_output_is_one_document(self, tmp_path: Path) -> None:
        result = invoke("plan", str(job_file(tmp_path)), "--json")

        document = json.loads(result.stdout)
        assert document["job_id"] == "orders-1"
        assert document["chunks"] > 1
        assert document["estimated_rows"] == 20_000
        assert [c["name"] for c in document["columns"]][:2] == ["id", "customer"]

    def test_an_unknown_table_is_refused(self, tmp_path: Path) -> None:
        result = invoke("plan", str(job_file(tmp_path, source_table="nope")))

        assert result.exit_code == 3
        assert "nope" in result.stderr


class TestStatus:
    def test_it_reads_the_state_a_run_left_in_a_persistent_metadata_store(
        self, tmp_path: Path
    ) -> None:
        path = job_file(tmp_path, metadata_url=f"sqlite://{tmp_path}/state.db")
        assert invoke("run", str(path), "--ui", "plain").exit_code == 0

        result = invoke("status", str(path))

        assert result.exit_code == 0, result.stderr
        assert "orders-1" in result.stdout
        assert "100%" in result.stdout
        assert "loaded" in result.stdout

    def test_json_output_has_the_counts(self, tmp_path: Path) -> None:
        path = job_file(tmp_path, metadata_url=f"sqlite://{tmp_path}/state.db")
        invoke("run", str(path), "--ui", "plain")

        document = json.loads(invoke("status", str(path), "--json").stdout)

        assert document["job_id"] == "orders-1"
        assert document["chunks"]["loaded"] == document["chunks_total"] > 1
        assert document["chunks"]["failed"] == 0

    def test_a_job_the_store_has_never_seen_is_reported_as_unknown(self, tmp_path: Path) -> None:
        path = job_file(tmp_path, metadata_url=f"sqlite://{tmp_path}/empty.db")

        result = invoke("status", str(path))

        assert result.exit_code == 3
        assert "orders-1" in result.stderr


# -- exit paths that need a misbehaving component -------------------------------------------------


class FlakySource(MemorySource):
    """Every read of the plan's second chunk fails."""

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        if chunk.chunk_id == plan.chunks[1].chunk_id:
            raise ConnectionError("source went away")
        return await super().read_chunk(plan, chunk)


class StopOnFirstSnapshot:
    """A display that asks the job to stop, like a person pressing 'q'."""

    def __init__(self, context: Any) -> None:
        self.job: asyncio.Task[TransferReport] | None = None

    def __call__(self, event: TransferEvent) -> None:
        if isinstance(event, Snapshot) and self.job is not None:
            self.job.cancel()

    def log_handler(self) -> logging.Handler | None:
        return None

    async def present(self, job: asyncio.Task[TransferReport]) -> None:
        self.job = job
        await asyncio.wait({job})


def entry_point(name: str, factory: Callable[..., object]) -> SimpleNamespace:
    return SimpleNamespace(name=name, load=lambda: factory)


@pytest.fixture
def install(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    """Replace the installed adapters by exactly these entry points, per group."""

    def apply(**groups: Iterable[SimpleNamespace]) -> None:
        defaults = {
            "bitdrop_sources": [entry_point("memory", create_source)],
            "bitdrop_destinations": [entry_point("memory", create_destination)],
            "bitdrop_chunk_stores": [entry_point("memory", create_chunk_store)],
            "bitdrop_metadata_stores": [entry_point("memory", create_metadata_store)],
        }
        merged = {**defaults, **{key: list(value) for key, value in groups.items()}}

        def discover(*, group: str) -> Iterable[Any]:
            return merged.get(group.replace(".", "_"), [])

        monkeypatch.setattr(registry, "entry_points", discover)

    return apply


class TestExitPaths:
    def test_a_chunk_that_gives_up_means_exit_code_one_and_is_listed(
        self, tmp_path: Path, install: Callable[..., None]
    ) -> None:
        def flaky(url: str) -> FlakySource:
            return FlakySource({"demo": demo_table(20_000)})

        install(
            bitdrop_sources=[entry_point("flaky", flaky)],
            bitdrop_uis=[entry_point("plain", create_plain_ui)],
        )
        path = job_file(tmp_path, source_url="flaky://", extra="max_attempts = 2\n")

        result = invoke("run", str(path), "--ui", "plain")

        assert result.exit_code == 1
        assert "FAILED" in result.stdout
        assert "rows." in result.stdout
        assert "source went away" in result.stdout

    def test_a_display_that_stops_the_job_means_exit_code_130_and_a_hint_to_resume(
        self, tmp_path: Path, install: Callable[..., None]
    ) -> None:
        install(bitdrop_uis=[entry_point("stopper", StopOnFirstSnapshot)])

        result = invoke("run", str(job_file(tmp_path, rows=200_000)), "--ui", "stopper")

        assert result.exit_code == 130
        assert "stopped" in result.stderr
        assert "resume" in result.stderr
