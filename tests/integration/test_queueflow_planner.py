"""The planner of the queue workflow: analyze and chunk the table, then publish the chunk ids.

Real in-memory adapters are used; small subclasses make the queue fail or count what is published.
"""

import asyncio
import dataclasses
import json
from collections.abc import Callable

import pytest

from bitdrop_api.errors import ChunkingMismatch, TableNotFound
from bitdrop_api.marker_store import MarkerStore
from bitdrop_api.testing import make_rows
from bitdrop_connector_memory import MemoryDestination, MemorySource
from bitdrop_core.queueflow import (
    PlannerConfig,
    WorkMessage,
    decode_work,
    planned_key,
    run_planner,
)
from bitdrop_marker_memory import MemoryMarkerStore
from bitdrop_queue_memory import MemoryChunkQueue

JOB = "job-1"
ROWS = 1000
ROW_BYTES = make_rows(1, ROWS).nbytes // ROWS


def config(*, rows_per_chunk: int = 100, **overrides: object) -> PlannerConfig:
    values: dict[str, object] = {
        "job_id": JOB,
        "source_table": "people",
        "destination_table": "people_copy",
        "target_chunk_bytes": ROW_BYTES * rows_per_chunk,
    }
    return PlannerConfig(**{**values, **overrides})  # type: ignore[arg-type]


class World:
    def __init__(self, rows: int = ROWS, queue: MemoryChunkQueue | None = None) -> None:
        self.source = MemorySource({"people": make_rows(1, rows)})
        self.destination = MemoryDestination()
        self.work = queue or MemoryChunkQueue()
        self.markers: MarkerStore = MemoryMarkerStore()

    async def plan(self, cfg: PlannerConfig | None = None, *, republish: bool = False):  # type: ignore[no-untyped-def]
        return await run_planner(
            self.source,
            self.destination,
            self.work,
            self.markers,
            cfg or config(),
            republish=republish,
        )

    async def published(self) -> list[WorkMessage]:
        """Drain the work queue; the messages are decoded."""
        messages = []
        while delivered := await self.work.receive("reader"):
            messages.append(decode_work(delivered[0].body))
        return messages


class DiesAfter(MemoryChunkQueue):
    """A queue that accepts ``limit`` messages and then fails, like a planner killed halfway."""

    def __init__(self, limit: int) -> None:
        super().__init__()
        self.limit = limit

    async def publish(self, body: bytes) -> None:
        if self.limit == 0:
            raise ConnectionError("the planner was killed")
        self.limit -= 1
        await super().publish(body)


class TestPlanning:
    async def test_every_chunk_id_is_published_once_and_names_a_readable_chunk(self) -> None:
        w = World()

        report = await w.plan()

        messages = await w.published()
        assert report.chunks_total == 10
        assert [m.chunk for m in messages] == [f"rows.{i * 100}.100" for i in range(10)]
        assert {(m.job, m.table) for m in messages} == {(JOB, "people")}
        for message in messages:  # a source container can read each one from the id alone
            assert w.source.chunk_from_id(message.table, message.chunk).chunk_id == message.chunk

    async def test_the_destination_table_is_created_before_anything_is_published(self) -> None:
        w = World()

        await w.plan()

        assert (await w.destination.count_rows("people_copy")) == 0

    async def test_the_completion_marker_comes_last_and_counts_the_chunks(self) -> None:
        w = World()

        await w.plan()

        marker = await w.markers.get(planned_key(JOB))
        assert marker is not None
        assert json.loads(marker)["chunks"] == 10

    async def test_an_empty_table_publishes_nothing_and_is_still_complete(self) -> None:
        w = World(rows=0)

        report = await w.plan()

        assert report.chunks_total == 0
        assert await w.published() == []
        marker = await w.markers.get(planned_key(JOB))
        assert marker is not None
        assert json.loads(marker)["chunks"] == 0

    async def test_an_unknown_table_fails_before_anything_is_recorded(self) -> None:
        w = World()

        with pytest.raises(TableNotFound):
            await w.plan(config(source_table="nope"))

        assert await w.published() == []
        assert await w.markers.get(planned_key(JOB)) is None
        # nothing bound the job to the failed attempt: a corrected run is accepted
        assert (await w.plan()).chunks_total == 10


class TestRunningTwice:
    async def test_a_finished_plan_is_not_published_again(self) -> None:
        w = World()
        await w.plan()
        await w.published()

        report = await w.plan()

        assert report.already_planned
        assert await w.published() == [], "chunks already loaded would be read and staged again"

    async def test_republish_puts_every_chunk_back_for_a_lost_queue(self) -> None:
        w = World()
        await w.plan()
        await w.published()

        report = await w.plan(republish=True)

        assert not report.already_planned
        assert len(await w.published()) == 10

    async def test_a_planner_killed_halfway_is_finished_by_the_next_run(self) -> None:
        queue = DiesAfter(4)
        w = World(queue=queue)
        with pytest.raises(ConnectionError):
            await w.plan()
        assert await w.markers.get(planned_key(JOB)) is None, "it never finished"

        queue.limit = 1000
        report = await w.plan()

        assert not report.already_planned
        ids = [m.chunk for m in await w.published()]
        assert set(ids) == {f"rows.{i * 100}.100" for i in range(10)}  # repeats are allowed
        assert await w.markers.get(planned_key(JOB)) is not None


class TestOneJobOneChunking:
    async def test_other_settings_are_refused_and_publish_nothing(self) -> None:
        w = World()
        await w.plan()
        await w.published()

        with pytest.raises(ChunkingMismatch, match="job-1"):
            await w.plan(config(rows_per_chunk=250))

        assert await w.published() == []

    async def test_a_table_that_changed_since_the_first_plan_is_refused(self) -> None:
        """New rows make different ids; the ledger would load the same rows twice."""
        w = World()
        await w.plan()
        w.source.add_table("people", make_rows(1, ROWS + 50))

        with pytest.raises(ChunkingMismatch):
            await w.plan()

    async def test_a_table_with_the_same_number_of_chunks_but_other_ids_is_refused(self) -> None:
        w = World()
        await w.plan()
        w.source.add_table("people", make_rows(1, 950))  # ten chunks again, the last one shorter

        with pytest.raises(ChunkingMismatch, match="chunks"):
            await w.plan()

    async def test_a_changed_schema_is_refused_even_when_the_chunks_are_the_same(self) -> None:
        w = World()
        await w.plan()
        renamed = make_rows(1, ROWS).rename_columns(
            ["id", "label", *make_rows(1, 1).column_names[2:]]
        )
        w.source.add_table("people", renamed)

        with pytest.raises(ChunkingMismatch, match="schema"):
            await w.plan()

    @pytest.mark.parametrize(
        "change",
        [
            lambda c: dataclasses.replace(c, split_column="id"),
            lambda c: dataclasses.replace(c, target_chunk_bytes=2 * 10**12),
        ],
        ids=["column", "size"],
    )
    async def test_other_settings_with_the_same_chunks_are_refused(
        self, change: Callable[[PlannerConfig], PlannerConfig]
    ) -> None:
        """A split column the source ignores, or a size that cuts the same chunks, still differs."""
        w = World()
        first = config(target_chunk_bytes=10**12)  # one chunk for the whole table
        await w.plan(first)

        with pytest.raises(ChunkingMismatch, match="settings"):
            await w.plan(change(first))

    async def test_two_planners_started_together_end_with_one_chunking(self) -> None:
        w = World()

        results = await asyncio.gather(
            w.plan(config(rows_per_chunk=100)),
            w.plan(config(rows_per_chunk=250)),
            return_exceptions=True,
        )

        winners = [r for r in results if not isinstance(r, BaseException)]
        losers = [r for r in results if isinstance(r, ChunkingMismatch)]
        assert (len(winners), len(losers)) == (1, 1)
        chunks = [m.chunk for m in await w.published()]
        assert len(chunks) == winners[0].chunks_total
        assert len(set(chunks)) == len(chunks), "the loser published chunks too"

    async def test_two_planners_with_the_same_settings_both_succeed(self) -> None:
        w = World()

        a, b = await asyncio.gather(w.plan(), w.plan())

        assert a.chunks_total == b.chunks_total == 10
        assert {m.chunk for m in await w.published()} == {f"rows.{i * 100}.100" for i in range(10)}
