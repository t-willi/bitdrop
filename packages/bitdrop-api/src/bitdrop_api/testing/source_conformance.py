"""Conformance tests for the :class:`~bitdrop_api.source.Source` contract.

How to use it in a connector package::

    class TestMySourceConformance(SourceConformance):
        @pytest.fixture
        async def source(self) -> AsyncIterator[Source]: ...   # loaded with `expected`

        @pytest.fixture
        def table_name(self) -> str: ...                       # the table holding `expected`

        @pytest.fixture
        def expected(self) -> pa.Table: ...                    # its exact contents

        @pytest.fixture
        def empty_table_name(self) -> str: ...                 # an existing table without rows

``expected`` must have at least a few hundred rows, and its first column must be unique (the kit
sorts by it to compare results independent of read order).
"""

import asyncio
from collections.abc import AsyncIterator

import pyarrow as pa
import pytest

from bitdrop_api.errors import TableNotFound
from bitdrop_api.keys import validate_key
from bitdrop_api.plan import decode_plan, encode_plan
from bitdrop_api.source import Source, SourcePlan

HUGE_CHUNK = 1 << 40
"""A target chunk size no table reaches: asks the source for as few chunks as it can."""


async def _read_all(source: Source, plan: SourcePlan) -> pa.Table:
    tables = [await source.read_chunk(plan, chunk) for chunk in plan.chunks]
    return pa.concat_tables(tables) if tables else plan.schema.empty_table()


def _sorted(table: pa.Table) -> pa.Table:
    return table.sort_by(table.schema.names[0])


class SourceConformance:
    """Base class of the contract tests. Subclass it and override the four fixtures."""

    # -- fixtures an adapter must override ------------------------------------------------------

    @pytest.fixture
    async def source(self) -> AsyncIterator[Source]:
        """Yield a source that holds the tables named by the other fixtures."""
        raise NotImplementedError("override the `source` fixture in your Test subclass")
        yield  # pragma: no cover  # noqa: PT022

    @pytest.fixture
    def table_name(self) -> str:
        """Name of the table whose contents are ``expected``."""
        raise NotImplementedError("override the `table_name` fixture in your Test subclass")

    @pytest.fixture
    def expected(self) -> pa.Table:
        """Exact contents of the table; first column unique."""
        raise NotImplementedError("override the `expected` fixture in your Test subclass")

    @pytest.fixture
    def empty_table_name(self) -> str:
        """Name of an existing table with no rows."""
        raise NotImplementedError("override the `empty_table_name` fixture in your Test subclass")

    # -- protocol and planning ------------------------------------------------------------------

    async def test_source_satisfies_the_protocol(self, source: Source) -> None:
        """The object structurally implements every method of ``Source``."""
        assert isinstance(source, Source)

    async def test_the_plan_describes_the_table(
        self, source: Source, table_name: str, expected: pa.Table
    ) -> None:
        """The plan names the table and reports the expected column names and types."""
        plan = await source.plan(table_name, target_chunk_bytes=HUGE_CHUNK)

        assert plan.table == table_name
        assert plan.schema.names == expected.schema.names
        assert plan.schema.types == expected.schema.types

    async def test_chunk_ids_are_unique_and_valid_store_keys(
        self, source: Source, table_name: str
    ) -> None:
        """Chunk ids end up in chunk store keys, so they must obey the key rule."""
        plan = await source.plan(table_name, target_chunk_bytes=1)

        ids = [chunk.chunk_id for chunk in plan.chunks]
        assert len(set(ids)) == len(ids)
        for chunk_id in ids:
            validate_key(chunk_id)

    async def test_a_small_target_splits_the_table_and_a_huge_one_does_not(
        self, source: Source, table_name: str
    ) -> None:
        """``target_chunk_bytes`` steers the number of chunks."""
        small = await source.plan(table_name, target_chunk_bytes=1)
        huge = await source.plan(table_name, target_chunk_bytes=HUGE_CHUNK)

        assert len(small.chunks) > 1, "a 1-byte target must produce several chunks"
        assert len(huge.chunks) <= len(small.chunks)

    # -- reading --------------------------------------------------------------------------------

    @pytest.mark.parametrize("target", [1, 10_000, HUGE_CHUNK], ids=["tiny", "medium", "huge"])
    async def test_chunks_are_disjoint_and_cover_the_table_exactly(
        self, source: Source, table_name: str, expected: pa.Table, target: int
    ) -> None:
        """Whatever the chunking, reading every chunk yields each row once and only once."""
        plan = await source.plan(table_name, target_chunk_bytes=target)

        actual = await _read_all(source, plan)

        assert actual.num_rows == expected.num_rows, "rows are missing or duplicated"
        assert _sorted(actual).equals(_sorted(expected))

    async def test_every_chunk_conforms_to_the_plan_schema(
        self, source: Source, table_name: str
    ) -> None:
        """Destinations rely on a stable schema across chunks."""
        plan = await source.plan(table_name, target_chunk_bytes=10_000)

        for chunk in plan.chunks:
            table = await source.read_chunk(plan, chunk)
            assert table.schema.names == plan.schema.names
            assert table.schema.types == plan.schema.types

    async def test_reading_a_chunk_twice_gives_the_same_rows(
        self, source: Source, table_name: str
    ) -> None:
        """Retries after a failure must see identical data."""
        plan = await source.plan(table_name, target_chunk_bytes=10_000)
        chunk = plan.chunks[len(plan.chunks) // 2]

        first = await source.read_chunk(plan, chunk)
        second = await source.read_chunk(plan, chunk)

        assert _sorted(first).equals(_sorted(second))

    async def test_chunks_can_be_read_concurrently(
        self, source: Source, table_name: str, expected: pa.Table
    ) -> None:
        """Extractors read many chunks at once."""
        plan = await source.plan(table_name, target_chunk_bytes=10_000)

        tables = await asyncio.gather(*(source.read_chunk(plan, chunk) for chunk in plan.chunks))

        assert _sorted(pa.concat_tables(tables)).equals(_sorted(expected))

    async def test_a_saved_plan_reads_the_same_table(
        self, source: Source, table_name: str, expected: pa.Table
    ) -> None:
        """A plan is saved and read back by another process; chunks read from the copy must match.

        This is why a chunk payload may hold only values JSON keeps exactly (see
        :func:`bitdrop_api.plan.encode_plan`).
        """
        plan = await source.plan(table_name, target_chunk_bytes=10_000)

        restored = decode_plan(encode_plan(plan))

        assert restored.chunks == plan.chunks
        assert _sorted(await _read_all(source, restored)).equals(_sorted(expected))

    # -- edge cases -----------------------------------------------------------------------------

    async def test_an_empty_table_yields_no_rows(
        self, source: Source, empty_table_name: str
    ) -> None:
        """No rows is not an error: the plan is valid and reading it gives nothing."""
        plan = await source.plan(empty_table_name, target_chunk_bytes=10_000)

        assert (await _read_all(source, plan)).num_rows == 0

    async def test_an_unknown_table_raises_table_not_found(self, source: Source) -> None:
        """A missing table is reported by name, before anything is read."""
        with pytest.raises(TableNotFound) as excinfo:
            await source.plan("no_such_table_anywhere", target_chunk_bytes=10_000)

        assert excinfo.value.table == "no_such_table_anywhere"
