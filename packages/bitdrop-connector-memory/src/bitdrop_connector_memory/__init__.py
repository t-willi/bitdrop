"""Source and Destination connectors keeping Arrow tables in this process's memory.

Useful for tests, demos, and as the reference implementation of the contracts. Select either with
``memory://``. The URL factories create *empty* instances; tests fill a ``MemorySource`` with
``add_table`` or construct it with tables directly. Two URL parameters make it usable without any
database:

* ``memory://?demo_rows=N`` (source only) serves a generated table named ``demo`` with ``N`` rows.
* ``memory://?delay_ms=D`` (source and destination) makes every chunk take ``D`` milliseconds, to
  watch a job run or to simulate a slow database.
"""

import asyncio
from collections.abc import Mapping

import pyarrow as pa
import pyarrow.compute as pc

from bitdrop_api.destination import ChunkWrite
from bitdrop_api.errors import SchemaMismatch, TableNotFound, TableNotPrepared
from bitdrop_api.keys import validate_key
from bitdrop_api.source import ChunkSpec, SourcePlan
from bitdrop_api.urls import ParsedUrl, parse_url

__all__ = [
    "DEMO_TABLE",
    "MemoryDestination",
    "MemorySource",
    "create_destination",
    "create_source",
]


DEMO_TABLE = "demo"
"""Name of the generated table served by ``memory://?demo_rows=N``."""

_DEMO_STATUSES = ("new", "paid", "shipped", "returned")
_DEMO_EPOCH_US = 1_704_067_200_000_000  # 2024-01-01T00:00:00Z


def demo_table(rows: int) -> pa.Table:
    """Return a generated table of ``rows`` rows. Values derive from the id, so it is repeatable."""
    ids = pa.array(range(1, rows + 1), pa.int64())
    # pyarrow-stubs only types this function for expressions, not for arrays and scalars.
    customer = pc.binary_join_element_wise(  # type: ignore[call-overload]
        pa.scalar("customer-"), pc.cast(ids, pa.string()), pa.scalar("")
    )
    status = pc.take(pa.array(_DEMO_STATUSES), pc.bit_wise_and(ids, pa.scalar(3, pa.int64())))
    cents = pc.cast(pc.bit_wise_and(ids, pa.scalar(0xFFFF, pa.int64())), pa.float64())
    amount = pc.divide(cents, 100)
    created = pc.cast(
        pc.add(pc.multiply(ids, 1_000_000), pa.scalar(_DEMO_EPOCH_US, pa.int64())),
        pa.timestamp("us", tz="UTC"),
    )
    return pa.table(
        {"id": ids, "customer": customer, "status": status, "amount": amount, "created": created}
    )


class MemorySource:
    """A :class:`~bitdrop_api.source.Source` serving named in-memory Arrow tables.

    Chunks are row ranges (``offset``, ``length``) of the table, sized from the table's average
    bytes per row.
    """

    def __init__(
        self, tables: Mapping[str, pa.Table] | None = None, *, delay_seconds: float = 0.0
    ) -> None:
        """Create a source, optionally already holding ``tables`` by name.

        ``delay_seconds`` is how long reading one chunk takes (0 for no delay).
        """
        self._tables: dict[str, pa.Table] = dict(tables or {})
        self._delay_seconds = delay_seconds

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
        if self._delay_seconds:
            await asyncio.sleep(self._delay_seconds)
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

    def __init__(self, *, delay_seconds: float = 0.0) -> None:
        """Create a destination with no tables.

        ``delay_seconds`` is how long writing one chunk takes (0 for no delay).
        """
        self._delay_seconds = delay_seconds
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
        if self._delay_seconds:
            await asyncio.sleep(self._delay_seconds)

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
    """Build a source from a ``memory://`` URL. This is the plugin entry point.

    Empty unless ``demo_rows`` asks for the generated ``demo`` table.
    """
    parsed = _parse(url, allowed=("demo_rows", "delay_ms"))
    source = MemorySource(delay_seconds=_delay_seconds(parsed))
    demo_rows = parsed.int_param("demo_rows")
    if demo_rows is not None:
        if demo_rows < 1:
            raise ValueError(f"memory:// parameter 'demo_rows' must be at least 1, got {demo_rows}")
        source.add_table(DEMO_TABLE, demo_table(demo_rows))
    return source


def create_destination(url: str) -> MemoryDestination:
    """Build an empty destination from a ``memory://`` URL. This is the plugin entry point."""
    parsed = _parse(url, allowed=("delay_ms",))
    return MemoryDestination(delay_seconds=_delay_seconds(parsed))


def _parse(url: str, *, allowed: tuple[str, ...]) -> ParsedUrl:
    parsed = parse_url(url, scheme="memory", allowed_params=allowed)
    if parsed.path:
        raise ValueError(f"memory:// URLs take no path, found {parsed.path!r} in {url!r}")
    return parsed


def _delay_seconds(parsed: ParsedUrl) -> float:
    delay_ms = parsed.int_param("delay_ms")
    if delay_ms is None:
        return 0.0
    if delay_ms < 0:
        raise ValueError(f"memory:// parameter 'delay_ms' must not be negative, got {delay_ms}")
    return delay_ms / 1000
