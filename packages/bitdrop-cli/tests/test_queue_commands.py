"""``bitdrop queue plan | source | destination | status``, run the way a person runs them.

The components are real in-memory ones, shared between the commands the way a Redis server would
be: each command opens them by the URL ``shared://`` and finds the same objects.
"""

import asyncio
import json
from collections.abc import Callable, Iterable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from bitdrop_api.testing import make_rows
from bitdrop_cli.app import app
from bitdrop_connector_memory import MemoryDestination, MemorySource
from bitdrop_core import registry
from bitdrop_core.queueflow import WorkMessage, encode_work
from bitdrop_marker_memory import MemoryMarkerStore
from bitdrop_queue_memory import MemoryChunkQueue
from bitdrop_store_memory import MemoryChunkStore

runner = CliRunner()

ROWS = 1000
ROW_BYTES = make_rows(1, ROWS).nbytes // ROWS

JOB = """
[job]
id = "orders-1"

[source]
url = "shared://"
table = "people"

[destination]
url = "shared://"
table = "people_copy"

[staging]
url = "shared://"

[queues]
work = "shared://?queue=work"
files = "shared://?queue=files"

[markers]
url = "shared://"

[tuning]
chunk_mib = {chunk_mib}
max_attempts = 2
lease_seconds = 5
poll_seconds = 0.05
"""
CHUNK_MIB = ROW_BYTES * 100 / (1 << 20)


async def _keep_open() -> None:
    """Closing a shared component would empty it for the next command."""


class Shared:
    """The one set of components every command in a test opens."""

    def __init__(self, *, staging_value_limit: int | None = None) -> None:
        self.source = MemorySource({"people": make_rows(1, ROWS)})
        self.destination = MemoryDestination()
        self.staging = MemoryChunkStore(max_value_bytes=staging_value_limit)
        self.work = MemoryChunkQueue()
        self.files = MemoryChunkQueue()
        self.markers = MemoryMarkerStore()
        for component in (self.source, self.destination, self.staging, self.work, self.files):
            component.aclose = _keep_open  # type: ignore[method-assign]
        self.markers.aclose = _keep_open  # type: ignore[method-assign]


@pytest.fixture
def shared(monkeypatch: pytest.MonkeyPatch) -> Shared:
    return install(monkeypatch, Shared())


def install(monkeypatch: pytest.MonkeyPatch, parts: Shared) -> Shared:
    """Make ``shared://`` open the components of ``parts``."""

    def entry_point(factory: Callable[[str], object]) -> SimpleNamespace:
        return SimpleNamespace(name="shared", load=lambda: factory)

    def queue(url: str) -> MemoryChunkQueue:
        return parts.files if "queue=files" in url else parts.work

    groups: dict[str, list[SimpleNamespace]] = {
        "bitdrop_sources": [entry_point(lambda url: parts.source)],
        "bitdrop_destinations": [entry_point(lambda url: parts.destination)],
        "bitdrop_chunk_stores": [entry_point(lambda url: parts.staging)],
        "bitdrop_chunk_queues": [entry_point(queue)],
        "bitdrop_marker_stores": [entry_point(lambda url: parts.markers)],
    }

    def discover(*, group: str) -> Iterable[Any]:
        return groups.get(group.replace(".", "_"), [])

    monkeypatch.setattr(registry, "entry_points", discover)
    return parts


def job_file(tmp_path: Path, *, chunk_mib: float = CHUNK_MIB) -> Path:
    path = tmp_path / "job.toml"
    path.write_text(JOB.format(chunk_mib=chunk_mib))
    return path


def invoke(*args: str) -> Any:
    return runner.invoke(app, list(args))


class TestPlan:
    def test_it_publishes_the_chunk_ids_and_says_how_many(
        self, tmp_path: Path, shared: Shared
    ) -> None:
        result = invoke("queue", "plan", str(job_file(tmp_path)))

        assert result.exit_code == 0, result.output
        assert "10 chunks" in result.stdout

    def test_running_it_again_publishes_nothing_more(self, tmp_path: Path, shared: Shared) -> None:
        path = job_file(tmp_path)
        invoke("queue", "plan", str(path))

        again = invoke("queue", "plan", str(path))
        status = invoke("queue", "status", str(path), "--json")

        assert again.exit_code == 0, again.output
        assert "already planned" in again.stdout
        assert json.loads(status.stdout)["work"]["waiting"] == 10

    def test_republish_puts_every_chunk_back(self, tmp_path: Path, shared: Shared) -> None:
        path = job_file(tmp_path)
        invoke("queue", "plan", str(path))

        invoke("queue", "plan", str(path), "--republish")
        status = invoke("queue", "status", str(path), "--json")

        assert json.loads(status.stdout)["work"]["waiting"] == 20

    def test_other_chunking_for_the_same_job_is_refused_with_exit_code_3(
        self, tmp_path: Path, shared: Shared
    ) -> None:
        invoke("queue", "plan", str(job_file(tmp_path)))

        result = invoke("queue", "plan", str(job_file(tmp_path, chunk_mib=CHUNK_MIB * 2.5)))

        assert result.exit_code == 3
        assert "orders-1" in result.stderr
        assert "new job id" in result.stderr

    def test_an_unknown_table_is_refused_before_anything_is_recorded(
        self, tmp_path: Path, shared: Shared
    ) -> None:
        path = job_file(tmp_path)
        path.write_text(path.read_text().replace('table = "people"', 'table = "nope"'))

        result = invoke("queue", "plan", str(path))

        assert result.exit_code == 3
        assert "nope" in result.stderr

    def test_a_job_file_without_queues_says_which_section_is_missing(
        self, tmp_path: Path, shared: Shared
    ) -> None:
        path = job_file(tmp_path)
        path.write_text(
            path.read_text().replace(
                '[queues]\nwork = "shared://?queue=work"\nfiles = "shared://?queue=files"\n',
                '[metadata]\nurl = "shared://"\n',
            )
        )

        result = invoke("queue", "plan", str(path))

        assert result.exit_code == 2
        assert "[queues]" in result.stderr


class TestWhatTheStagingStoreCanHold:
    def test_chunks_are_cut_small_enough_for_the_value_limit_of_the_store(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The same table is cut into more, smaller chunks when the store takes small values."""
        install(monkeypatch, Shared(staging_value_limit=ROW_BYTES * 50))

        result = invoke("queue", "plan", str(job_file(tmp_path)))

        # a chunk is cut to half the value limit, 25 rows, so 1000 rows make 40 chunks, not 10
        assert "planned 40 chunks" in result.stdout


class TestStatus:
    def test_before_planning_nothing_is_known(self, tmp_path: Path, shared: Shared) -> None:
        result = invoke("queue", "status", str(job_file(tmp_path)))

        assert result.exit_code == 0
        assert "not planned" in result.stdout

    def test_after_planning_it_is_running_with_every_chunk_waiting(
        self, tmp_path: Path, shared: Shared
    ) -> None:
        path = job_file(tmp_path)
        invoke("queue", "plan", str(path))

        result = invoke("queue", "status", str(path))

        assert "running" in result.stdout
        assert "0 of 10" in result.stdout

    def test_json_has_the_state_and_both_queues(self, tmp_path: Path, shared: Shared) -> None:
        path = job_file(tmp_path)
        invoke("queue", "plan", str(path))

        document = json.loads(invoke("queue", "status", str(path), "--json").stdout)

        assert document["state"] == "running"
        assert document["chunks_total"] == 10
        assert set(document["work"]) == {"waiting", "in_flight", "dead"}
        assert set(document["files"]) == {"waiting", "in_flight", "dead"}


class TestChunksThatFail:
    def test_a_chunk_set_aside_makes_the_container_and_the_status_exit_with_1(
        self, tmp_path: Path, shared: Shared
    ) -> None:
        path = job_file(tmp_path)
        invoke("queue", "plan", str(path))
        asyncio.run(shared.work.publish(encode_work(WorkMessage("orders-1", "people", "junk"))))

        sourced = invoke("queue", "source", str(path), "--stop-when-finished")
        loaded = invoke("queue", "destination", str(path), "--stop-when-finished")
        status = invoke("queue", "status", str(path))

        assert sourced.exit_code == 1
        assert "1 set aside" in sourced.stdout
        assert loaded.exit_code == 0, "the destination had nothing wrong with its files"
        assert status.exit_code == 1
        assert "finished with failures" in status.stdout


class TestTheWholeJob:
    def test_plan_then_source_then_destination_moves_every_row_once(
        self, tmp_path: Path, shared: Shared
    ) -> None:
        path = job_file(tmp_path)
        assert invoke("queue", "plan", str(path)).exit_code == 0

        sourced = invoke("queue", "source", str(path), "--stop-when-finished")
        loaded = invoke("queue", "destination", str(path), "--stop-when-finished")
        status = invoke("queue", "status", str(path))

        assert sourced.exit_code == 0, sourced.output
        assert loaded.exit_code == 0, loaded.output
        assert "10 handled" in sourced.stdout
        assert "10 handled" in loaded.stdout
        assert "finished" in status.stdout
        assert shared.destination.contents("people_copy").sort_by("id").equals(make_rows(1, ROWS))

    def test_a_source_waits_for_the_plan_instead_of_stopping_at_once(
        self, tmp_path: Path, shared: Shared
    ) -> None:
        """Not planned yet is not the same as nothing to do."""
        path = job_file(tmp_path)

        result = invoke(
            "queue", "source", str(path), "--stop-when-finished", "--give-up-after", "0.3"
        )

        assert result.exit_code == 1
        assert "not planned" in result.stderr

    def test_status_wait_returns_when_the_job_is_over(self, tmp_path: Path, shared: Shared) -> None:
        path = job_file(tmp_path)
        invoke("queue", "plan", str(path))
        invoke("queue", "source", str(path), "--stop-when-finished")
        invoke("queue", "destination", str(path), "--stop-when-finished")

        result = invoke("queue", "status", str(path), "--wait")

        assert result.exit_code == 0
        assert "finished" in result.stdout
