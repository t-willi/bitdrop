"""Behaviour specific to the in-memory connectors (the contracts are covered by the kits)."""

import time
from typing import Any

import pyarrow.compute as pc
import pytest

from bitdrop_api.destination import Destination
from bitdrop_api.errors import InvalidChunkId, TableNotFound, TableNotPrepared
from bitdrop_api.source import Source
from bitdrop_api.testing import SAMPLE_SCHEMA, make_rows
from bitdrop_connector_memory import (
    DEMO_TABLE,
    MemoryDestination,
    MemorySource,
    create_destination,
    create_source,
)


class TestMemorySource:
    async def test_chunks_follow_the_target_size(self) -> None:
        table = make_rows(1, 1000)
        source = MemorySource({"t": table})
        row_bytes = table.nbytes // table.num_rows

        plan = await source.plan("t", target_chunk_bytes=row_bytes * 100)

        assert len(plan.chunks) == 10
        assert [chunk.estimated_rows for chunk in plan.chunks] == [100] * 10
        assert plan.estimated_rows == 1000

    async def test_the_last_chunk_takes_the_remainder(self) -> None:
        table = make_rows(1, 1050)
        source = MemorySource({"t": table})
        row_bytes = table.nbytes // table.num_rows

        plan = await source.plan("t", target_chunk_bytes=row_bytes * 100)

        assert plan.chunks[-1].estimated_rows == 50

    async def test_chunk_ids_say_which_rows_they_hold(self) -> None:
        table = make_rows(1, 250)
        source = MemorySource({"t": table})
        row_bytes = table.nbytes // table.num_rows

        plan = await source.plan("t", target_chunk_bytes=row_bytes * 100)

        assert [chunk.chunk_id for chunk in plan.chunks] == [
            "rows.0.100",
            "rows.100.100",
            "rows.200.50",
        ]

    def test_a_chunk_can_be_rebuilt_from_its_id(self) -> None:
        source = MemorySource({"t": make_rows(1, 250)})

        chunk = source.chunk_from_id("t", "rows.100.100")

        assert chunk.payload == {"offset": 100, "length": 100}

    @pytest.mark.parametrize(
        "chunk_id",
        [
            "rows.00.100",  # a second spelling of rows.0.100
            "rows.0100.100",
            "rows.0.0",  # no rows
            "rows.+1.5",
            "rows.-1.5",
            "rows.1.5.",
            "rows.200.51",  # runs past the last row
            "rows.250.1",
        ],
    )
    def test_an_id_with_another_spelling_or_outside_the_table_is_refused(
        self, chunk_id: str
    ) -> None:
        source = MemorySource({"t": make_rows(1, 250)})

        with pytest.raises(InvalidChunkId):
            source.chunk_from_id("t", chunk_id)

    def test_an_id_for_an_unknown_table_is_reported_by_table(self) -> None:
        with pytest.raises(TableNotFound):
            MemorySource().chunk_from_id("nope", "rows.0.1")

    async def test_tables_can_be_added_later(self) -> None:
        source = MemorySource()
        source.add_table("late", make_rows(1, 5))

        plan = await source.plan("late", target_chunk_bytes=1 << 20)

        assert plan.estimated_rows == 5


class TestMemoryDestination:
    async def test_contents_of_an_unprepared_table_is_an_error(self) -> None:
        with pytest.raises(TableNotPrepared):
            MemoryDestination().contents("nope")

    async def test_contents_concatenate_all_chunks_in_write_order(self) -> None:
        destination = MemoryDestination()
        await destination.prepare("t", SAMPLE_SCHEMA)
        await destination.write_chunk("j", "c1", "t", make_rows(1, 3))
        await destination.write_chunk("j", "c2", "t", make_rows(4, 3))

        assert destination.contents("t").column("id").to_pylist() == [1, 2, 3, 4, 5, 6]


class TestFactories:
    """The registry calls these with the URL from the configuration."""

    def test_bare_urls_give_working_connectors(self) -> None:
        assert isinstance(create_source("memory://"), Source)
        assert isinstance(create_destination("memory://"), Destination)

    @pytest.mark.parametrize("factory", [create_source, create_destination])
    @pytest.mark.parametrize(
        "url",
        [
            pytest.param("postgresql://", id="wrong-scheme"),
            pytest.param("memory://somehost", id="host-not-allowed"),
            pytest.param("memory:///a/path", id="path-not-allowed"),
            pytest.param("memory://?x=1", id="unknown-parameter"),
        ],
    )
    def test_bad_urls_are_rejected_with_a_clear_error(self, factory: object, url: str) -> None:
        with pytest.raises(ValueError, match="memory"):
            factory(url)  # type: ignore[operator]


class TestDemoData:
    """``memory://?demo_rows=N`` serves a generated table, so a job can run without a database."""

    async def test_the_source_serves_a_generated_table_named_demo(self) -> None:
        source = create_source("memory://?demo_rows=1000")

        plan = await source.plan(DEMO_TABLE, target_chunk_bytes=10_000)

        assert DEMO_TABLE == "demo"
        assert plan.estimated_rows == 1000
        assert plan.schema.names == ["id", "customer", "status", "amount", "created"]
        assert len(plan.chunks) > 1
        chunks = [await source.read_chunk(plan, chunk) for chunk in plan.chunks]
        assert sum(chunk.num_rows for chunk in chunks) == 1000

    async def test_ids_are_unique_and_count_up_from_one(self) -> None:
        source = create_source("memory://?demo_rows=500")
        plan = await source.plan(DEMO_TABLE, target_chunk_bytes=5_000)

        ids: list[Any] = []
        for chunk in plan.chunks:
            ids += (await source.read_chunk(plan, chunk)).column("id").to_pylist()

        assert sorted(ids) == list(range(1, 501))

    async def test_the_same_chunk_always_has_the_same_rows(self) -> None:
        source = create_source("memory://?demo_rows=300")
        plan = await source.plan(DEMO_TABLE, target_chunk_bytes=3_000)

        first = await source.read_chunk(plan, plan.chunks[1])
        second = await source.read_chunk(plan, plan.chunks[1])

        assert first.equals(second)

    async def test_the_values_are_ordinary_and_never_null(self) -> None:
        source = create_source("memory://?demo_rows=64")
        plan = await source.plan(DEMO_TABLE, target_chunk_bytes=1 << 20)

        table = await source.read_chunk(plan, plan.chunks[0])

        assert [table.column(name).null_count for name in table.schema.names] == [0] * 5
        assert set(pc.unique(table.column("status")).to_pylist()) == {
            "new",
            "paid",
            "shipped",
            "returned",
        }

    @pytest.mark.parametrize("rows", ["0", "-5", "abc", ""])
    def test_demo_rows_must_be_a_positive_integer(self, rows: str) -> None:
        with pytest.raises(ValueError, match="demo_rows"):
            create_source(f"memory://?demo_rows={rows}")

    async def test_without_demo_rows_the_source_is_empty(self) -> None:
        source = create_source("memory://")

        with pytest.raises(TableNotFound):
            await source.plan(DEMO_TABLE, target_chunk_bytes=1000)

    def test_the_destination_takes_no_demo_rows(self) -> None:
        with pytest.raises(ValueError, match="demo_rows"):
            create_destination("memory://?demo_rows=10")


class TestDelay:
    """``delay_ms`` makes every chunk take that long: to watch a job, or to fake a slow database."""

    async def test_every_chunk_read_takes_the_delay(self) -> None:
        source = create_source("memory://?demo_rows=100&delay_ms=60")
        plan = await source.plan(DEMO_TABLE, target_chunk_bytes=1 << 20)

        started = time.monotonic()
        await source.read_chunk(plan, plan.chunks[0])

        assert time.monotonic() - started >= 0.055

    async def test_every_chunk_write_takes_the_delay(self) -> None:
        destination = create_destination("memory://?delay_ms=60")
        await destination.prepare("t", SAMPLE_SCHEMA)

        started = time.monotonic()
        await destination.write_chunk("j", "c1", "t", make_rows(1, 3))

        assert time.monotonic() - started >= 0.055

    async def test_there_is_no_delay_by_default(self) -> None:
        destination = create_destination("memory://")
        await destination.prepare("t", SAMPLE_SCHEMA)

        started = time.monotonic()
        await destination.write_chunk("j", "c1", "t", make_rows(1, 3))

        assert time.monotonic() - started < 0.05

    @pytest.mark.parametrize("factory", [create_source, create_destination])
    @pytest.mark.parametrize("value", ["-1", "abc", ""])
    def test_the_delay_must_be_a_non_negative_integer(self, factory: object, value: str) -> None:
        with pytest.raises(ValueError, match="delay_ms"):
            factory(f"memory://?delay_ms={value}")  # type: ignore[operator]
