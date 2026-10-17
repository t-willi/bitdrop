"""PostgreSQL source: plans a table into primary-key ranges and reads them as Arrow.

Planning reads the catalog (column types, primary key), checks that every column can move
losslessly, estimates row size and count, and cuts the key range into chunks. Reading a chunk is a
single ``SELECT ... WHERE key BETWEEN lo AND hi`` fetched as Arrow through ADBC.

Consistency: chunks are read independently, at different moments, without a shared snapshot. Each
row is read at most once (ranges are disjoint), but rows inserted, deleted, or re-keyed while the
transfer runs may or may not be seen. A snapshot-consistent mode is future work.
"""

import math
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from bitdrop_api.errors import TableNotFound
from bitdrop_api.schema import with_native_type
from bitdrop_api.source import ChunkSpec, SourcePlan
from bitdrop_connector_postgres._chunk_ids import chunk_id_for, payload_from_id
from bitdrop_connector_postgres._connection import ConnectionPool, parse_connection_url
from bitdrop_connector_postgres._names import TableName, parse_table_name, quote_ident
from bitdrop_connector_postgres._ranges import split_range
from bitdrop_connector_postgres._types import check_supported

ENGINE = "postgresql"
ROW_SAMPLE_SIZE = 1000
"""Rows sampled to estimate the average row size."""

_INTEGER_TYPES = ("smallint", "integer", "bigint")

_COLUMNS_SQL = (
    "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull FROM pg_attribute a"
    " WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped"
    " ORDER BY a.attnum"
)
_PRIMARY_KEY_SQL = (
    "SELECT a.attname FROM pg_index i"
    " JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)"
    " WHERE i.indrelid = to_regclass($1) AND i.indisprimary"
)


class PostgresSource:
    """A :class:`~bitdrop_api.source.Source` reading PostgreSQL tables and views."""

    def __init__(self, url: str) -> None:
        """Create a source for the database at ``url`` (``postgresql://user:pw@host:port/db``).

        Connections are opened lazily. Option ``bitdrop_pool_size`` (default 4) bounds how many
        chunks are read at once.

        Raises:
            ValueError: not a PostgreSQL URL, or an unknown ``bitdrop_*`` option.
        """
        self._settings = parse_connection_url(url)
        self._pool = ConnectionPool(self._settings, autocommit=True)

    @property
    def pool_size(self) -> int:
        """How many chunk reads may run at once."""
        return self._settings.pool_size

    async def plan(
        self, table: str, *, target_chunk_bytes: int, split_column: str | None = None
    ) -> SourcePlan:
        """Inspect ``table`` and cut it into primary-key ranges of about ``target_chunk_bytes``.

        Without an integer primary key (and without a usable ``split_column``) the table is read as
        one chunk, and the plan says so in its notes.

        Raises:
            ValueError: the table name is malformed, or ``split_column`` is unusable.
            TableNotFound: the table does not exist.
            UnsupportedType: a column cannot be moved losslessly.
        """
        name = parse_table_name(table)  # validated before any SQL is built
        return await self._pool.run(
            lambda connection: _plan(connection, table, name, target_chunk_bytes, split_column)
        )

    def chunk_from_id(self, table: str, chunk_id: str) -> ChunkSpec:
        """Read the chunk out of its id (``all``, ``<column>.<first>.<last>``, or ``x.<text>``).

        Nothing is checked against the database: a column or range that does not exist fails when
        the chunk is read.

        Raises:
            InvalidChunkId: not exactly an id this source writes.
        """
        payload = payload_from_id(chunk_id)
        description = (
            f"{payload['column']} in [{payload['lo']}, {payload['hi']}]"
            if payload
            else "whole table"
        )
        return ChunkSpec(chunk_id, description, payload)

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        """Read one key range (or the whole table) as a table conforming to ``plan.schema``."""
        name = parse_table_name(plan.table)
        columns = ", ".join(quote_ident(column) for column in plan.schema.names)
        sql = f"SELECT {columns} FROM {name.sql()}"
        parameters: tuple[int, ...] = ()
        if "column" in chunk.payload:
            sql += f" WHERE {quote_ident(chunk.payload['column'])} BETWEEN $1 AND $2"
            parameters = (chunk.payload["lo"], chunk.payload["hi"])

        def work(connection: Any) -> pa.Table:
            with connection.cursor() as cursor:
                cursor.execute(sql, parameters=parameters)
                return _conform(cursor.fetch_arrow_table(), plan.schema)

        return await self._pool.run(work)

    async def aclose(self) -> None:
        """Close all connections."""
        await self._pool.aclose()


def create_source(url: str) -> PostgresSource:
    """Build a source from a ``postgresql://`` URL. This is the plugin entry point."""
    return PostgresSource(url)


# -- planning (runs in a worker thread, on one connection) -----------------------------------------


def _plan(
    connection: Any,
    table: str,
    name: TableName,
    target_chunk_bytes: int,
    split_column: str | None,
) -> SourcePlan:
    qualified = name.sql()
    with connection.cursor() as cursor:
        cursor.execute(_COLUMNS_SQL, parameters=(qualified,))
        catalog = cursor.fetchall()
        if not catalog:
            raise TableNotFound(table)

        schema = _schema(connection, table, qualified, catalog)
        key = _split_key(cursor, qualified, table, catalog, split_column)
        row_bytes = _average_row_bytes(cursor, qualified)
        estimated_rows = _estimated_rows(cursor, qualified)

        notes: list[str] = []
        chunks: tuple[ChunkSpec, ...] = ()
        if key is None:
            notes.append(
                f"{table} has no integer primary key or split_column: reading it as a single chunk"
            )
            chunks = (ChunkSpec(chunk_id_for({}), "whole table", {}, estimated_rows),)
        else:
            cursor.execute(
                f"SELECT min({quote_ident(key)}), max({quote_ident(key)}) FROM {qualified}"
            )
            ((low, high),) = cursor.fetchall()
            if low is not None:
                rows_per_chunk = max(1, target_chunk_bytes // row_bytes)
                ranges, note = split_range(
                    low, high, rows_per_chunk=rows_per_chunk, estimated_rows=estimated_rows
                )
                if note:
                    notes.append(note)
                chunks = tuple(
                    ChunkSpec(
                        chunk_id_for({"column": key, "lo": lo, "hi": hi}),
                        f"{key} in [{lo}, {hi}]",
                        {"column": key, "lo": lo, "hi": hi},
                        _chunk_rows(estimated_rows, low, high, lo, hi),
                    )
                    for lo, hi in ranges
                )

    return SourcePlan(
        table=table,
        schema=schema,
        chunks=chunks,
        estimated_rows=estimated_rows,
        estimated_row_bytes=row_bytes,
        notes=tuple(notes),
    )


def _schema(
    connection: Any, table: str, qualified: str, catalog: list[tuple[Any, ...]]
) -> pa.Schema:
    """Build the Arrow schema: driver types, numerics as decimals, native types as metadata."""
    columns = ", ".join(quote_ident(str(attname)) for attname, _, _ in catalog)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {columns} FROM {qualified} LIMIT 0")
        driver_schema = cursor.fetch_arrow_table().schema

    fields = []
    for (attname, native, not_null), driver_field in zip(catalog, driver_schema, strict=True):
        supported = check_supported(table, str(attname), str(native))
        data_type = supported.arrow_cast or driver_field.type
        field = pa.field(str(attname), data_type, nullable=not not_null)
        fields.append(with_native_type(field, ENGINE, str(native)))
    return pa.schema(fields)


def _split_key(
    cursor: Any,
    qualified: str,
    table: str,
    catalog: list[tuple[Any, ...]],
    split_column: str | None,
) -> str | None:
    """Pick the column to split on: the hint if given, else a single integer primary key."""
    by_name = {str(attname): (str(native), bool(not_null)) for attname, native, not_null in catalog}
    if split_column is not None:
        if split_column not in by_name:
            raise ValueError(f"split_column {split_column!r} is not a column of {table!r}")
        native, not_null = by_name[split_column]
        if native not in _INTEGER_TYPES or not not_null:
            raise ValueError(
                f"split_column {split_column!r} must be a NOT NULL integer column "
                f"(it is {native}{'' if not_null else ', nullable'})"
            )
        return split_column

    cursor.execute(_PRIMARY_KEY_SQL, parameters=(qualified,))
    key_columns = [str(row[0]) for row in cursor.fetchall()]
    if len(key_columns) == 1 and by_name[key_columns[0]][0] in _INTEGER_TYPES:
        return key_columns[0]
    return None


def _average_row_bytes(cursor: Any, qualified: str) -> int:
    cursor.execute(
        "SELECT avg(pg_column_size(t.*))::float8"
        f" FROM (SELECT * FROM {qualified} LIMIT {ROW_SAMPLE_SIZE}) t"
    )
    average = cursor.fetchall()[0][0]
    return max(1, math.ceil(average)) if average else 64


def _estimated_rows(cursor: Any, qualified: str) -> int:
    """Use the planner's statistics when they exist, otherwise count (exact but a full scan)."""
    cursor.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass($1)",
        parameters=(qualified,),
    )
    estimate = cursor.fetchall()[0][0]
    if estimate and estimate > 0:
        return int(estimate)
    cursor.execute(f"SELECT count(*) FROM {qualified}")
    return int(cursor.fetchall()[0][0])


def _chunk_rows(total_rows: int, low: int, high: int, lo: int, hi: int) -> int:
    return max(1, round(total_rows * (hi - lo + 1) / (high - low + 1)))


# -- reading ---------------------------------------------------------------------------------------


def _conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Turn the driver's table into one that has exactly the plan's schema."""
    columns = []
    for field in schema:
        column = table.column(field.name)
        if column.type != field.type:
            column = _cast(column, field.type)
        columns.append(column)
    return pa.Table.from_arrays(columns, schema=schema)


def _cast(column: pa.ChunkedArray[Any], target: pa.DataType) -> pa.ChunkedArray[Any]:
    """Cast a column; numerics arrive as an opaque extension whose storage is the decimal text."""
    chunks = [
        pc.cast(chunk.storage if isinstance(chunk, pa.ExtensionArray) else chunk, target)
        for chunk in column.chunks
    ]
    # The stubs only know chunked_array() for concrete type classes, not for a DataType variable.
    return pa.chunked_array(chunks, type=target)  # type: ignore[call-overload,no-any-return]
