"""The runner reports its progress to an observer, so a CLI or a TUI can show it live.

The observer is a plain callable that receives typed events. It is a window onto the transfer, never
part of it: whatever the observer does, the data arrives exactly as without one.
"""

import asyncio
from collections.abc import Callable

import pyarrow as pa
import pytest

from bitdrop_api.destination import ChunkWrite
from bitdrop_api.metadata_store import ChunkState, Stage
from bitdrop_api.source import ChunkSpec, SourcePlan
from bitdrop_api.testing import make_rows
from bitdrop_connector_memory import MemoryDestination, MemorySource
from bitdrop_core.progress import (
    ChunkFailed,
    ChunkFinished,
    ChunkStarted,
    PlanReady,
    Snapshot,
    TransferEvent,
)
from bitdrop_core.transfer import TransferConfig, TransferReport, run_transfer
from bitdrop_metadata_memory import MemoryMetadataStore
from bitdrop_store_memory import MemoryChunkStore

ROWS = 2000
ROWS_PER_CHUNK = 100
CHUNKS = ROWS // ROWS_PER_CHUNK
ROW_BYTES = make_rows(1, ROWS).nbytes // ROWS
TIMEOUT = 20.0


def config(**overrides: object) -> TransferConfig:
    defaults: dict[str, object] = {
        "job_id": "job-1",
        "source_table": "people",
        "destination_table": "people_copy",
        "target_chunk_bytes": ROW_BYTES * ROWS_PER_CHUNK,
        "extractors": 3,
        "loaders": 3,
        "lease_seconds": 5.0,
        "poll_interval": 0.002,
        "reaper_interval": 0.02,
        "progress_interval": 0.01,
    }
    return TransferConfig(**{**defaults, **overrides})  # type: ignore[arg-type]


class Run:
    """The outcome of one observed transfer."""

    def __init__(self, report: TransferReport, events: list[TransferEvent]) -> None:
        self.report = report
        self.events = events

    def of_type[T: TransferEvent](self, kind: type[T]) -> list[T]:
        return [event for event in self.events if isinstance(event, kind)]


async def observed_run(
    *,
    source: MemorySource | None = None,
    destination: MemoryDestination | None = None,
    chunks: MemoryChunkStore | None = None,
    cfg: TransferConfig | None = None,
    observer: Callable[[TransferEvent], None] | None = None,
) -> tuple[Run, MemoryDestination]:
    source = source or MemorySource()
    source.add_table("people", make_rows(1, ROWS))
    destination = destination or MemoryDestination()
    events: list[TransferEvent] = []
    async with asyncio.timeout(TIMEOUT):
        report = await run_transfer(
            source,
            destination,
            chunks or MemoryChunkStore(),
            MemoryMetadataStore(),
            cfg or config(),
            observer=observer or events.append,
        )
    return Run(report, events), destination


class SlowSource(MemorySource):
    """Reads slowly enough that periodic snapshots happen while the job is under way."""

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        await asyncio.sleep(0.01)
        return await super().read_chunk(plan, chunk)


class SlowDestination(MemoryDestination):
    """Loads slowly, so extracted chunks pile up in the staging tier."""

    async def write_chunk(
        self, job_id: str, chunk_id: str, table: str, data: pa.Table
    ) -> ChunkWrite:
        await asyncio.sleep(0.02)
        return await super().write_chunk(job_id, chunk_id, table, data)


class CommitThenDieDestination(MemoryDestination):
    """Commits one chunk, then fails once, as if the loader died right after the commit."""

    def __init__(self, victim: str) -> None:
        super().__init__()
        self.victim, self.struck = victim, False

    async def write_chunk(
        self, job_id: str, chunk_id: str, table: str, data: pa.Table
    ) -> ChunkWrite:
        result = await super().write_chunk(job_id, chunk_id, table, data)
        if chunk_id == self.victim and not self.struck:
            self.struck = True
            raise ConnectionError("loader died after committing")
        return result


class FlakySource(MemorySource):
    """Fails the first ``failures`` reads of ``chunk_id`` (every read if -1)."""

    def __init__(self, chunk_id: str, failures: int) -> None:
        super().__init__()
        self.chunk_id, self.failures, self.attempts = chunk_id, failures, 0

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        if chunk.chunk_id == self.chunk_id and (self.failures < 0 or self.attempts < self.failures):
            self.attempts += 1
            raise ConnectionError("source went away")
        return await super().read_chunk(plan, chunk)


class TestThePlan:
    async def test_the_first_event_describes_the_job(self) -> None:
        result, _ = await observed_run()

        first = result.events[0]
        assert isinstance(first, PlanReady)
        assert (first.job_id, first.source_table, first.destination_table) == (
            "job-1",
            "people",
            "people_copy",
        )
        assert first.chunks_total == CHUNKS
        assert first.chunk_ids == tuple(f"c{i:06d}" for i in range(CHUNKS))
        assert first.estimated_rows == ROWS
        assert [name for name, _ in first.columns] == make_rows(1, 1).schema.names
        assert (first.extractors, first.loaders) == (3, 3)

    async def test_it_is_announced_exactly_once(self) -> None:
        result, _ = await observed_run()

        assert len(result.of_type(PlanReady)) == 1


class TestChunkEvents:
    async def test_every_chunk_is_started_and_finished_in_both_stages(self) -> None:
        result, _ = await observed_run()

        for stage in Stage:
            started = sorted(e.chunk_id for e in result.of_type(ChunkStarted) if e.stage is stage)
            finished = sorted(e.chunk_id for e in result.of_type(ChunkFinished) if e.stage is stage)
            expected = [f"c{i:06d}" for i in range(CHUNKS)]
            assert started == expected, stage
            assert finished == expected, stage

    async def test_a_chunk_is_extracted_before_it_is_loaded(self) -> None:
        result, _ = await observed_run()

        position = {
            (e.chunk_id, e.stage, type(e)): index
            for index, e in enumerate(result.events)
            if isinstance(e, ChunkStarted | ChunkFinished)
        }
        for index in range(CHUNKS):
            cid = f"c{index:06d}"
            order = [
                position[(cid, Stage.EXTRACT, ChunkStarted)],
                position[(cid, Stage.EXTRACT, ChunkFinished)],
                position[(cid, Stage.LOAD, ChunkStarted)],
                position[(cid, Stage.LOAD, ChunkFinished)],
            ]
            assert order == sorted(order), cid

    async def test_finished_events_carry_rows_bytes_and_the_worker(self) -> None:
        result, _ = await observed_run()

        for stage, prefix in ((Stage.EXTRACT, "extractor-"), (Stage.LOAD, "loader-")):
            finished = [e for e in result.of_type(ChunkFinished) if e.stage is stage]
            assert sum(e.rows for e in finished) == ROWS, stage
            assert all(e.bytes > 0 and e.seconds >= 0 for e in finished), stage
            assert all(e.worker.startswith(prefix) for e in finished), stage

    async def test_staged_bytes_add_up_to_the_report(self) -> None:
        result, _ = await observed_run()

        extracted = sum(e.bytes for e in result.of_type(ChunkFinished) if e.stage is Stage.EXTRACT)
        assert extracted == result.report.bytes_staged

    async def test_a_transient_failure_is_reported_as_one_that_will_be_retried(self) -> None:
        result, destination = await observed_run(source=FlakySource("c000003", failures=1))

        failed = result.of_type(ChunkFailed)
        assert result.report.succeeded
        assert [(e.chunk_id, e.stage, e.final) for e in failed] == [
            ("c000003", Stage.EXTRACT, False)
        ]
        assert "source went away" in failed[0].reason
        assert failed[0].failures == 1
        assert destination.contents("people_copy").num_rows == ROWS

    async def test_a_chunk_the_destination_already_had_is_marked_as_a_repeat(self) -> None:
        result, _ = await observed_run(destination=CommitThenDieDestination("c000007"))

        loads = [e for e in result.of_type(ChunkFinished) if e.stage is Stage.LOAD]
        repeats = [e.chunk_id for e in loads if e.already_loaded]
        assert result.report.succeeded
        assert repeats == ["c000007"]
        assert result.of_type(Snapshot)[-1].duplicates_skipped == 1

    async def test_a_chunk_that_gives_up_is_reported_as_final(self) -> None:
        result, _ = await observed_run(
            source=FlakySource("c000003", failures=-1), cfg=config(max_attempts=2)
        )

        failed = result.of_type(ChunkFailed)
        assert [e.final for e in failed] == [False, True]
        assert [e.failures for e in failed] == [1, 2]
        assert not result.report.succeeded


class TestSnapshots:
    async def test_the_last_snapshot_agrees_with_the_report(self) -> None:
        result, _ = await observed_run()

        last = result.of_type(Snapshot)[-1]
        assert result.events[-1] is last, "nothing may be reported after the final snapshot"
        assert last.counts[ChunkState.LOADED] == result.report.chunks_loaded == CHUNKS
        assert (last.rows_extracted, last.rows_loaded) == (ROWS, ROWS)
        assert last.bytes_staged == result.report.bytes_staged
        assert last.duplicates_skipped == 0
        assert 0 < last.elapsed_seconds <= result.report.duration_seconds

    async def test_snapshots_arrive_while_the_job_runs(self) -> None:
        result, _ = await observed_run(source=SlowSource(), cfg=config(extractors=1, loaders=1))

        snapshots = result.of_type(Snapshot)
        loaded = [s.counts[ChunkState.LOADED] for s in snapshots]
        assert len(snapshots) >= 3
        assert loaded == sorted(loaded), "progress never goes backwards"
        assert any(0 < count < CHUNKS for count in loaded), "no snapshot saw the job mid-way"
        elapsed = [s.elapsed_seconds for s in snapshots]
        assert elapsed == sorted(elapsed)

    async def test_snapshots_show_how_full_the_staging_tier_is(self) -> None:
        capped = MemoryChunkStore(max_bytes=10_000_000)
        result, _ = await observed_run(
            destination=SlowDestination(), chunks=capped, cfg=config(loaders=1)
        )

        snapshots = result.of_type(Snapshot)
        assert all(s.staging_max_bytes == 10_000_000 for s in snapshots)
        assert max(s.staging_used_bytes or 0 for s in snapshots) > 0
        assert snapshots[-1].staging_used_bytes == 0, "everything is loaded and freed at the end"


class TestTheObserverIsOnlyAWindow:
    async def test_a_transfer_needs_no_observer(self) -> None:
        source = MemorySource()
        source.add_table("people", make_rows(1, ROWS))
        destination = MemoryDestination()

        report = await run_transfer(
            source, destination, MemoryChunkStore(), MemoryMetadataStore(), config()
        )

        assert report.succeeded

    async def test_an_observer_that_raises_cannot_hurt_the_transfer(self) -> None:
        calls = 0

        def broken(event: TransferEvent) -> None:
            nonlocal calls
            calls += 1
            raise RuntimeError("the display crashed")

        result, destination = await observed_run(observer=broken)

        assert result.report.succeeded
        assert destination.contents("people_copy").sort_by("id").equals(make_rows(1, ROWS))
        assert calls > CHUNKS, "a broken observer must keep being offered the events"

    async def test_an_observer_that_raises_is_logged_without_flooding(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        def broken(event: TransferEvent) -> None:
            raise RuntimeError("the display crashed")

        with caplog.at_level("WARNING", logger="bitdrop.transfer"):
            await observed_run(observer=broken)

        warnings = [r for r in caplog.records if "observer" in r.getMessage()]
        assert len(warnings) == 1
        assert "the display crashed" in caplog.text

    async def test_progress_interval_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="progress_interval"):
            config(progress_interval=0)
