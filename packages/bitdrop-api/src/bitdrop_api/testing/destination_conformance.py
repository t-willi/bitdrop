"""Conformance tests for the :class:`~bitdrop_api.destination.Destination` contract.

How to use it in a connector package::

    class TestMyDestinationConformance(DestinationConformance):
        @pytest.fixture
        async def destination(self) -> AsyncIterator[Destination]: ...   # fresh and empty

        @pytest.fixture
        def read_back(self) -> ReadBack: ...   # async (table) -> pa.Table, SAMPLE_SCHEMA types

The kit writes tables in :data:`~bitdrop_api.testing.sample_data.SAMPLE_SCHEMA`.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable

import pyarrow as pa
import pytest

from bitdrop_api.destination import Destination
from bitdrop_api.errors import InvalidKey, SchemaMismatch, TableNotPrepared
from bitdrop_api.testing.sample_data import SAMPLE_SCHEMA, make_rows

ReadBack = Callable[[str], Awaitable[pa.Table]]
"""Reads a destination table's full contents as a table with the types of ``SAMPLE_SCHEMA``."""

TABLE = "kit_table"
JOB = "job-1"


def _sorted(table: pa.Table) -> pa.Table:
    return table.sort_by("id")


class DestinationConformance:
    """Base class of the contract tests. Subclass it and override the two fixtures."""

    @pytest.fixture
    async def destination(self) -> AsyncIterator[Destination]:
        """Yield a fresh destination with no tables. Required: override in your subclass."""
        raise NotImplementedError("override the `destination` fixture in your Test subclass")
        yield  # pragma: no cover  # noqa: PT022

    @pytest.fixture
    def read_back(self) -> ReadBack:
        """Return a function that reads a table back. Required: override in your subclass."""
        raise NotImplementedError("override the `read_back` fixture in your Test subclass")

    # -- protocol and preparation ---------------------------------------------------------------

    async def test_destination_satisfies_the_protocol(self, destination: Destination) -> None:
        """The object structurally implements every method of ``Destination``."""
        assert isinstance(destination, Destination)

    async def test_prepare_creates_an_empty_table(self, destination: Destination) -> None:
        """A prepared table exists and has no rows."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)

        assert await destination.count_rows(TABLE) == 0

    async def test_prepare_twice_keeps_the_data(self, destination: Destination) -> None:
        """Re-running ``prepare`` on a matching table (a resumed job) loses nothing."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)
        await destination.write_chunk(JOB, "c1", TABLE, make_rows(1, 10))

        await destination.prepare(TABLE, SAMPLE_SCHEMA)

        assert await destination.count_rows(TABLE) == 10

    async def test_prepare_refuses_a_table_with_different_columns(
        self, destination: Destination
    ) -> None:
        """Appending into a table of another shape would corrupt it, so it is refused."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)
        other = pa.schema([pa.field("something_else", pa.int64())])

        with pytest.raises(SchemaMismatch):
            await destination.prepare(TABLE, other)

    async def test_writing_to_an_unprepared_table_is_an_error(
        self, destination: Destination
    ) -> None:
        """Nothing is created implicitly."""
        with pytest.raises(TableNotPrepared):
            await destination.write_chunk(JOB, "c1", "never_prepared", make_rows(1, 3))
        with pytest.raises(TableNotPrepared):
            await destination.count_rows("never_prepared")

    # -- writing --------------------------------------------------------------------------------

    async def test_a_written_chunk_reads_back_exactly(
        self, destination: Destination, read_back: ReadBack
    ) -> None:
        """Every type, including NULLs, survives the trip."""
        data = make_rows(1, 500)
        await destination.prepare(TABLE, SAMPLE_SCHEMA)

        result = await destination.write_chunk(JOB, "c1", TABLE, data)

        assert (result.rows, result.already_loaded) == (500, False)
        assert _sorted(await read_back(TABLE)).equals(_sorted(data))

    async def test_chunks_accumulate(self, destination: Destination, read_back: ReadBack) -> None:
        """Different chunks add up to the union of their rows."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)
        await destination.write_chunk(JOB, "c1", TABLE, make_rows(1, 100))
        await destination.write_chunk(JOB, "c2", TABLE, make_rows(101, 100))

        assert await destination.count_rows(TABLE) == 200
        assert _sorted(await read_back(TABLE)).equals(make_rows(1, 200))

    async def test_an_empty_chunk_is_recorded_like_any_other(
        self, destination: Destination
    ) -> None:
        """A chunk without rows is still done, and is not redone."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)

        first = await destination.write_chunk(JOB, "c1", TABLE, make_rows(1, 0))
        second = await destination.write_chunk(JOB, "c1", TABLE, make_rows(1, 0))

        assert (first.rows, first.already_loaded) == (0, False)
        assert second.already_loaded is True

    # -- idempotence: the reason the ledger exists ----------------------------------------------

    async def test_writing_a_chunk_twice_loads_it_once(self, destination: Destination) -> None:
        """The second write is recognised and changes nothing."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)
        data = make_rows(1, 100)
        await destination.write_chunk(JOB, "c1", TABLE, data)

        again = await destination.write_chunk(JOB, "c1", TABLE, data)

        assert (again.rows, again.already_loaded) == (100, True)
        assert await destination.count_rows(TABLE) == 100

    async def test_simultaneous_writes_of_one_chunk_load_it_once(
        self, destination: Destination
    ) -> None:
        """Two loaders holding the same chunk (a stale one and its replacement) race safely."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)
        data = make_rows(1, 100)

        results = await asyncio.gather(
            *(destination.write_chunk(JOB, "c1", TABLE, data) for _ in range(8))
        )

        assert sum(not result.already_loaded for result in results) == 1
        assert await destination.count_rows(TABLE) == 100

    async def test_many_different_chunks_can_be_written_concurrently(
        self, destination: Destination
    ) -> None:
        """Loaders work in parallel."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)

        await asyncio.gather(
            *(
                destination.write_chunk(JOB, f"c{i:03d}", TABLE, make_rows(i * 50 + 1, 50))
                for i in range(24)
            )
        )

        assert await destination.count_rows(TABLE) == 24 * 50

    async def test_the_ledger_is_per_job_and_per_table(self, destination: Destination) -> None:
        """The same chunk id in another job or another table is a different chunk."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)
        await destination.prepare("kit_table_2", SAMPLE_SCHEMA)
        data = make_rows(1, 10)

        await destination.write_chunk("job-a", "c1", TABLE, data)
        other_job = await destination.write_chunk("job-b", "c1", TABLE, data)
        other_table = await destination.write_chunk("job-a", "c1", "kit_table_2", data)

        assert not other_job.already_loaded
        assert not other_table.already_loaded
        assert await destination.count_rows(TABLE) == 20
        assert await destination.count_rows("kit_table_2") == 10

    @pytest.mark.parametrize("bad_id", ["", "a/b", "has space"])
    async def test_invalid_ids_are_rejected(self, destination: Destination, bad_id: str) -> None:
        """Job ids and chunk ids follow the shared key rule."""
        await destination.prepare(TABLE, SAMPLE_SCHEMA)

        with pytest.raises(InvalidKey):
            await destination.write_chunk(bad_id, "c1", TABLE, make_rows(1, 1))
        with pytest.raises(InvalidKey):
            await destination.write_chunk(JOB, bad_id, TABLE, make_rows(1, 1))
