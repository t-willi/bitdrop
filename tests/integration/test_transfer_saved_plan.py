"""A job's plan is saved before its chunks, and a resumed job reads the chunks it started with.

Real in-memory adapters are used; small subclasses count and order the calls the runner makes.
"""

import asyncio

import pytest

from bitdrop_api import errors
from bitdrop_api.errors import CorruptPlan, TableNotPrepared
from bitdrop_api.metadata_store import ChunkState, MetadataStore
from bitdrop_api.plan import decode_plan, encode_plan
from bitdrop_api.source import SourcePlan
from bitdrop_api.testing import make_rows
from bitdrop_connector_memory import MemoryDestination, MemorySource
from bitdrop_core.progress import PlanReady, TransferEvent
from bitdrop_core.transfer import TransferConfig, TransferReport, run_transfer
from bitdrop_metadata_memory import MemoryMetadataStore
from bitdrop_store_memory import MemoryChunkStore

JOB = "job-1"
ROWS = 1000
ROW_BYTES = make_rows(1, ROWS).nbytes // ROWS
TIMEOUT = 20.0


def config(*, rows_per_chunk: int = 100, **overrides: object) -> TransferConfig:
    defaults: dict[str, object] = {
        "job_id": JOB,
        "source_table": "people",
        "destination_table": "people_copy",
        "target_chunk_bytes": ROW_BYTES * rows_per_chunk,
        "extractors": 3,
        "loaders": 3,
        "poll_interval": 0.002,
        "reaper_interval": 0.02,
    }
    return TransferConfig(**{**defaults, **overrides})  # type: ignore[arg-type]


class CountingSource(MemorySource):
    """Counts how often the runner asks it to plan a table."""

    def __init__(self, rows: int = ROWS) -> None:
        super().__init__({"people": make_rows(1, rows), "orders": make_rows(1, 10)})
        self.plans = 0

    async def plan(
        self, table: str, *, target_chunk_bytes: int, split_column: str | None = None
    ) -> SourcePlan:
        self.plans += 1
        return await super().plan(
            table, target_chunk_bytes=target_chunk_bytes, split_column=split_column
        )


class RecordingMetadata(MemoryMetadataStore):
    """Remembers the order in which the runner saved the plan and registered chunks."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    async def save_plan(self, job_id: str, plan: bytes) -> bytes:
        self.calls.append("save_plan")
        return await super().save_plan(job_id, plan)

    async def register_chunks(self, job_id, chunk_ids, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append("register_chunks")
        return await super().register_chunks(job_id, chunk_ids, **kwargs)


class BarrierMetadata(MemoryMetadataStore):
    """Makes two masters look for a plan at the same moment, so both find none."""

    def __init__(self) -> None:
        super().__init__()
        self._barrier = asyncio.Barrier(2)

    async def load_plan(self, job_id: str) -> bytes | None:
        found = await super().load_plan(job_id)
        await self._barrier.wait()
        return found


async def run(
    source: MemorySource,
    meta: MetadataStore,
    destination: MemoryDestination,
    cfg: TransferConfig | None = None,
    *,
    chunks: MemoryChunkStore | None = None,
    observer: object = None,
) -> TransferReport:
    async with asyncio.timeout(TIMEOUT):
        return await run_transfer(
            source,
            destination,
            chunks or MemoryChunkStore(),
            meta,
            cfg or config(),
            observer=observer,  # type: ignore[arg-type]
        )


class TestSavingThePlan:
    async def test_the_saved_plan_lists_the_chunks_that_were_registered(self) -> None:
        meta, destination = MemoryMetadataStore(), MemoryDestination()

        report = await run(CountingSource(), meta, destination)

        saved = await meta.load_plan(JOB)
        assert saved is not None
        plan = decode_plan(saved)
        assert plan.table == "people"
        assert [chunk.chunk_id for chunk in plan.chunks] == [
            f"c{index:06d}" for index in range(report.chunks_total)
        ]
        assert sum((await meta.counts(JOB)).values()) == len(plan.chunks)

    async def test_the_plan_is_saved_before_the_chunks_are_registered(self) -> None:
        """A crash between the two must leave a plan and no chunks, never chunks and no plan."""
        meta = RecordingMetadata()

        await run(CountingSource(), meta, MemoryDestination())

        assert meta.calls == ["save_plan", "register_chunks"]


class TestResumingFromTheSavedPlan:
    async def test_a_second_run_of_the_job_does_not_plan_again(self) -> None:
        meta, destination = MemoryMetadataStore(), MemoryDestination()
        await run(CountingSource(), meta, destination)
        later = CountingSource()

        second = await run(later, meta, destination)

        assert later.plans == 0
        assert second.succeeded
        assert second.rows_loaded == 0

    async def test_the_saved_chunks_are_read_even_if_the_table_grew_meanwhile(self) -> None:
        """A resumed job finishes the table it started on: no new rows, no gaps, no overlaps."""
        meta, destination = MemoryMetadataStore(), MemoryDestination()
        original = CountingSource(rows=ROWS)
        await meta.save_plan(
            JOB,
            encode_plan(
                await original.plan(
                    "people", target_chunk_bytes=config().target_chunk_bytes, split_column=None
                )
            ),
        )
        grown = CountingSource(rows=ROWS + 500)

        report = await run(grown, meta, destination)

        assert report.succeeded
        assert grown.plans == 0
        assert destination.contents("people_copy").sort_by("id").equals(make_rows(1, ROWS))

    async def test_the_observer_hears_about_the_saved_plan(self) -> None:
        meta, destination = MemoryMetadataStore(), MemoryDestination()
        first = await run(CountingSource(), meta, destination)
        events: list[TransferEvent] = []

        await run(CountingSource(), meta, destination, observer=events.append)

        (ready,) = [event for event in events if isinstance(event, PlanReady)]
        assert len(ready.chunk_ids) == first.chunks_total
        assert ready.estimated_rows == ROWS

    async def test_a_job_planned_for_another_table_is_refused(self) -> None:
        meta = MemoryMetadataStore()
        await run(CountingSource(), meta, MemoryDestination())
        other_destination = MemoryDestination()
        before = dict(await meta.counts(JOB))

        with pytest.raises(errors.PlanMismatch, match=r"people.*orders|orders.*people"):
            await run(CountingSource(), meta, other_destination, config(source_table="orders"))

        assert dict(await meta.counts(JOB)) == before
        with pytest.raises(TableNotPrepared):  # nothing was set up for the refused table
            other_destination.contents("people_copy")

    async def test_a_saved_plan_that_is_garbage_stops_the_run_before_anything_moves(self) -> None:
        meta, destination = MemoryMetadataStore(), MemoryDestination()
        await meta.save_plan(JOB, b"this is not a plan")

        with pytest.raises(CorruptPlan):
            await run(CountingSource(), meta, destination)

        assert sum((await meta.counts(JOB)).values()) == 0
        with pytest.raises(TableNotPrepared):
            destination.contents("people_copy")

    async def test_the_plan_does_not_depend_on_the_chunk_size_of_the_resuming_run(self) -> None:
        meta, destination = MemoryMetadataStore(), MemoryDestination()
        first = await run(CountingSource(), meta, destination, config(rows_per_chunk=100))

        second = await run(CountingSource(), meta, destination, config(rows_per_chunk=250))

        assert second.chunks_total == first.chunks_total == ROWS // 100
        assert (await meta.counts(JOB))[ChunkState.LOADED] == first.chunks_total


class TestTwoMastersPlanningAtOnce:
    async def test_both_use_the_plan_that_was_saved_first(self) -> None:
        """Different chunk sizes would overlap or leave gaps if each master used its own plan."""
        meta, destination, chunks = BarrierMetadata(), MemoryDestination(), MemoryChunkStore()
        a, b = CountingSource(), CountingSource()

        reports = await asyncio.gather(
            run(a, meta, destination, config(rows_per_chunk=100), chunks=chunks),
            run(b, meta, destination, config(rows_per_chunk=250), chunks=chunks),
        )

        assert (a.plans, b.plans) == (1, 1)  # both really did plan: the race happened
        assert all(report.succeeded for report in reports)
        assert reports[0].chunks_total == reports[1].chunks_total
        assert destination.contents("people_copy").sort_by("id").equals(make_rows(1, ROWS))
