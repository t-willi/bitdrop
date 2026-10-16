"""Source and Destination connectors keeping Arrow tables in this process's memory.

Useful for tests, demos, and as the reference implementation of the contracts. Select either with
``memory://`` (no parameters). The URL factories create *empty* instances; tests fill a
``MemorySource`` with ``add_table`` or construct it with tables directly.
"""

from collections.abc import Mapping

import pyarrow as pa

from bitdrop_api.destination import ChunkWrite
from bitdrop_api.errors import SchemaMismatch, TableNotFound, TableNotPrepared
from bitdrop_api.keys import validate_key
from bitdrop_api.source import ChunkSpec, SourcePlan
from bitdrop_api.urls import parse_url

__all__ = [
    "MemoryDestination",
    "MemorySource",
    "create_destination",
    "create_source",
]


class MemorySource:
    """A :class:`~bitdrop_api.source.Source` serving named in-memory Arrow tables.

    Chunks are row ranges (``offset``, ``length``) of the table, sized from the table's average
    bytes per row.
    """

    def __init__(self, tables: Mapping[str, pa.Table] | None = None) -> None:
        """Create a source, optionally already holding ``tables`` by name."""
        self._tables: dict[str, pa.Table] = dict(tables or {})

    def add_table(self, name: str, table: pa.Table) -> None:
        """Make ``table`` available under ``name``."""
        self._tables[name] = table

    async def plan(
        self, table: str, *, target_chunk_bytes: int, split_column: str | None = None
    ) -> SourcePlan:
        """Cut the table into row ranges; ``split_column`` is ignored."""
        data = self._table(table)
        if data.num_rows == 0:
            return SourcePlan(table=table, schema=data.schema, chunks=(), estimated_rows=0)

        row_bytes = max(1, data.nbytes // data.num_rows)
        rows_per_chunk = max(1, target_chunk_bytes // row_bytes)
        chunks = tuple(
            ChunkSpec(
                chunk_id=f"c{index:06d}",
                description=f"rows {offset}..{offset + length - 1}",
                payload={"offset": offset, "length": length},
                estimated_rows=length,
            )
            for index, offset in enumerate(range(0, data.num_rows, rows_per_chunk))
            for length in [min(rows_per_chunk, data.num_rows - offset)]
        )
        return SourcePlan(
            table=table,
            schema=data.schema,
            chunks=chunks,
            estimated_rows=data.num_rows,
            estimated_row_bytes=row_bytes,
        )

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        """Return the chunk's row range."""
        offset, length = chunk.payload["offset"], chunk.payload["length"]
        return self._table(plan.table).slice(offset, length)

    async def aclose(self) -> None:
        """Forget all tables."""
        self._tables.clear()

    def _table(self, name: str) -> pa.Table:
        try:
            return self._tables[name]
        except KeyError:
            raise TableNotFound(name) from None


class MemoryDestination:
    """A :class:`~bitdrop_api.destination.Destination` collecting Arrow tables in memory.

    The ledger is a set of ``(job_id, table, chunk_id)``. Safe under asyncio: no method awaits while
    it changes state, so every operation is atomic with respect to other tasks.
    """

    def __init__(self) -> None:
        """Create a destination with no tables."""
        self._schemas: dict[str, pa.Schema] = {}
        self._chunks: dict[str, list[pa.Table]] = {}
        self._ledger: set[tuple[str, str, str]] = set()

    async def prepare(self, table: str, schema: pa.Schema) -> None:
        """Create the table, or check that an existing one has the same columns."""
        existing = self._schemas.get(table)
        if existing is None:
            self._schemas[table] = schema
            self._chunks[table] = []
        elif existing.names != schema.names:
            raise SchemaMismatch(
                f"table {table!r} has columns {existing.names}, but {schema.names} are being loaded"
            )

    async def write_chunk(
        self, job_id: str, chunk_id: str, table: str, data: pa.Table
    ) -> ChunkWrite:
        """Append the chunk unless it was recorded before."""
        validate_key(job_id)
        validate_key(chunk_id)
        if table not in self._schemas:
            raise TableNotPrepared(table)

        entry = (job_id, table, chunk_id)
        if entry in self._ledger:
            return ChunkWrite(rows=data.num_rows, already_loaded=True)
        self._ledger.add(entry)
        self._chunks[table].append(data)
        return ChunkWrite(rows=data.num_rows, already_loaded=False)

    async def count_rows(self, table: str) -> int:
        """Return the number of rows in the table."""
        return self.contents(table).num_rows

    async def aclose(self) -> None:
        """Forget everything."""
        self._schemas.clear()
        self._chunks.clear()
        self._ledger.clear()

    def contents(self, table: str) -> pa.Table:
        """Return everything written to ``table`` so far, in write order.

        Raises:
            TableNotPrepared: ``prepare`` was never called for ``table``.
        """
        if table not in self._schemas:
            raise TableNotPrepared(table)
        chunks = self._chunks[table]
        return pa.concat_tables(chunks) if chunks else self._schemas[table].empty_table()


def create_source(url: str) -> MemorySource:
    """Build an empty source from a ``memory://`` URL. This is the plugin entry point."""
    _check_url(url)
    return MemorySource()


def create_destination(url: str) -> MemoryDestination:
    """Build an empty destination from a ``memory://`` URL. This is the plugin entry point."""
    _check_url(url)
    return MemoryDestination()


def _check_url(url: str) -> None:
    parsed = parse_url(url, scheme="memory", allowed_params=())
    if parsed.path:
        raise ValueError(f"memory:// URLs take no path, found {parsed.path!r} in {url!r}")
