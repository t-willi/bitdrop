"""The JSON-lines display: one JSON object per line on stdout, for programs to read."""

import asyncio
import io
import json
from typing import Any

from bitdrop_api.metadata_store import ChunkRecord, ChunkState, Stage
from bitdrop_cli.jsonlines import JsonLinesUi
from bitdrop_core.progress import ChunkFailed, ChunkFinished, PlanReady, Snapshot
from bitdrop_core.transfer import TransferReport
from bitdrop_core.ui import ProgressUi, UiContext

CONTEXT = UiContext(
    job_id="job-1",
    source_table="src",
    destination_table="dst",
    endpoints={"source": "a://"},
)


def plan() -> PlanReady:
    return PlanReady(
        job_id="job-1",
        source_table="src",
        destination_table="dst",
        chunk_ids=("c0", "c1", "c2", "c3"),
        columns=(("id", "int64"), ("name", "string")),
        estimated_rows=400,
        estimated_row_bytes=10,
        notes=("one chunk only",),
        extractors=2,
        loaders=1,
        max_attempts=3,
    )


def snapshot(elapsed: float, *, loaded: int = 0, rows: int = 0) -> Snapshot:
    counts = dict.fromkeys(ChunkState, 0)
    counts[ChunkState.LOADED] = loaded
    counts[ChunkState.PENDING] = 4 - loaded
    return Snapshot("job-1", elapsed, counts, rows, rows, 0, 123, 50, 100)


def make(interval: float = 1.0) -> tuple[JsonLinesUi, io.StringIO]:
    out = io.StringIO()
    return JsonLinesUi(CONTEXT, out=out, interval=interval), out


def lines(out: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in out.getvalue().splitlines()]


def test_it_is_a_progress_ui() -> None:
    ui, _ = make()

    assert isinstance(ui, ProgressUi)
    assert ui.log_handler() is None


def test_every_line_is_one_complete_json_object() -> None:
    ui, out = make()

    ui(plan())
    ui(snapshot(0.0))

    assert out.getvalue().endswith("\n")
    assert all(isinstance(line, dict) for line in lines(out))


def test_the_plan_event_describes_the_job() -> None:
    ui, out = make()

    ui(plan())

    assert lines(out) == [
        {
            "event": "plan",
            "job_id": "job-1",
            "source_table": "src",
            "destination_table": "dst",
            "chunks": 4,
            "estimated_rows": 400,
            "estimated_row_bytes": 10,
            "columns": [{"name": "id", "type": "int64"}, {"name": "name", "type": "string"}],
            "notes": ["one chunk only"],
            "extractors": 2,
            "loaders": 1,
        }
    ]


def test_a_progress_event_has_the_counts_the_rates_and_the_staging_fill() -> None:
    ui, out = make()
    ui(plan())
    ui(snapshot(0.0))
    ui(snapshot(2.0, loaded=1, rows=200))

    progress = [line for line in lines(out) if line["event"] == "progress"]

    last = progress[-1]
    assert last["elapsed_seconds"] == 2.0
    assert last["chunks"] == {
        "pending": 3,
        "extracting": 0,
        "extracted": 0,
        "loading": 0,
        "loaded": 1,
        "failed": 0,
    }
    assert (last["rows_extracted"], last["rows_loaded"]) == (200, 200)
    assert (last["extract_rows_per_second"], last["load_rows_per_second"]) == (100.0, 100.0)
    assert (last["staging_used_bytes"], last["staging_max_bytes"]) == (50, 100)
    assert last["bytes_staged"] == 123
    assert last["eta_seconds"] == 6.0


def test_progress_events_are_throttled_but_the_last_one_always_comes() -> None:
    ui, out = make(interval=1.0)
    ui(plan())

    for elapsed in (0.0, 0.2, 0.9, 1.0, 1.5):
        ui(snapshot(elapsed))
    ui(snapshot(1.6, loaded=4))

    times = [line["elapsed_seconds"] for line in lines(out) if line["event"] == "progress"]
    assert times == [0.0, 1.0, 1.6]


def test_a_failed_attempt_is_reported_when_it_happens() -> None:
    ui, out = make()

    ui(ChunkFailed("c2", Stage.LOAD, "loader-0", "boom", failures=2, final=True))

    assert lines(out) == [
        {
            "event": "chunk_failed",
            "chunk_id": "c2",
            "stage": "load",
            "worker": "loader-0",
            "reason": "boom",
            "failures": 2,
            "final": True,
        }
    ]


def test_chunks_that_merely_progress_are_not_reported_one_by_one() -> None:
    ui, out = make()

    ui(ChunkFinished("c1", Stage.EXTRACT, "extractor-0", rows=100, bytes=10, seconds=0.1))

    assert out.getvalue() == ""


async def test_the_report_is_the_last_line_when_the_job_ends() -> None:
    ui, out = make()
    failure = ChunkRecord("job-1", "c3", ChunkState.FAILED, 3, None, "boom")

    async def job() -> TransferReport:
        return TransferReport("job-1", 4, 3, 1, 400, 300, 0, 123, 2.5, (failure,))

    await ui.present(asyncio.create_task(job()))

    assert lines(out) == [
        {
            "event": "report",
            "job_id": "job-1",
            "succeeded": False,
            "chunks_total": 4,
            "chunks_loaded": 3,
            "chunks_failed": 1,
            "rows_extracted": 400,
            "rows_loaded": 300,
            "duplicates_skipped": 0,
            "bytes_staged": 123,
            "duration_seconds": 2.5,
            "failures": [{"chunk_id": "c3", "failures": 3, "last_error": "boom"}],
        }
    ]


async def test_a_stopped_job_ends_with_a_stopped_event() -> None:
    ui, out = make()

    async def job() -> TransferReport:
        await asyncio.sleep(60)
        raise AssertionError("unreachable")

    task = asyncio.create_task(job())
    await asyncio.sleep(0)
    task.cancel()
    await ui.present(task)

    assert lines(out) == [{"event": "stopped", "job_id": "job-1"}]


async def test_a_job_that_raised_ends_with_an_error_event() -> None:
    ui, out = make()

    async def job() -> TransferReport:
        raise RuntimeError("planning failed")

    await ui.present(asyncio.create_task(job()))

    assert lines(out) == [{"event": "error", "job_id": "job-1", "message": "planning failed"}]
