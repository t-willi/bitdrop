"""Behaviour specific to the in-memory connectors (the contracts are covered by the kits)."""

import pytest

from bitdrop_api.destination import Destination
from bitdrop_api.errors import TableNotPrepared
from bitdrop_api.source import Source
from bitdrop_api.testing import SAMPLE_SCHEMA, make_rows
from bitdrop_connector_memory import (
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
