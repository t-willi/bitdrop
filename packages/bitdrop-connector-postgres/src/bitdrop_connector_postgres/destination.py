"""PostgreSQL destination: creates tables from the source's schema and loads chunks exactly once.

Each chunk is loaded in **one transaction** that first inserts ``(job_id, table, chunk_id)`` into
the ledger table ``_bitdrop_chunks`` and then bulk-loads the rows with ``COPY`` (through ADBC). If
the ledger row already exists, the chunk was loaded before: nothing is written. Because data and
ledger row commit together, a crash can never leave one without the other.

Tables are created *without* indexes, primary keys, or foreign keys: bulk loading into a bare table
is much faster, and constraints can be added once the data is in. Column types come from the
source's native type hints when the source is also PostgreSQL (so ``numeric(12,2)``, ``jsonb``,
``varchar(10)`` survive exactly), and otherwise from the Arrow type.
"""

from typing import Any

import pyarrow as pa

from bitdrop_api.destination import ChunkWrite
from bitdrop_api.errors import SchemaMismatch, TableNotPrepared
from bitdrop_api.keys import validate_key
from bitdrop_api.schema import native_type
from bitdrop_connector_postgres._connection import ConnectionPool, parse_connection_url
from bitdrop_connector_postgres._names import TableName, parse_table_name, quote_ident
from bitdrop_connector_postgres._types import arrow_to_postgres, check_supported

ENGINE = "postgresql"
LEDGER_TABLE = "_bitdrop_chunks"

_CREATE_LEDGER = f"""
CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
    job_id     text        NOT NULL,
    table_name text        NOT NULL,
    chunk_id   text        NOT NULL,
    row_count  bigint      NOT NULL,
    loaded_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, table_name, chunk_id)
)
"""
_TABLE_EXISTS = "SELECT to_regclass($1) IS NOT NULL"
_COLUMN_NAMES = (
    "SELECT a.attname FROM pg_attribute a WHERE a.attrelid = to_regclass($1)"
    " AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
)
_RECORD_CHUNK = (
    f"INSERT INTO {LEDGER_TABLE} (job_id, table_name, chunk_id, row_count)"
    " VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING RETURNING 1"
)


class PostgresDestination:
    """A :class:`~bitdrop_api.destination.Destination` writing to PostgreSQL."""

    def __init__(self, url: str) -> None:
        """Create a destination for the database at ``url`` (``postgresql://user:pw@host:port/db``).

        Connections are opened lazily. Option ``bitdrop_pool_size`` (default 4) bounds how many
        chunks are written at once.

        Raises:
            ValueError: not a PostgreSQL URL, or an unknown ``bitdrop_*`` option.
        """
        self._settings = parse_connection_url(url)
        self._pool = ConnectionPool(self._settings, autocommit=False)

    @property
    def pool_size(self) -> int:
        """How many chunk writes may run at once."""
        return self._settings.pool_size

    async def prepare(self, table: str, schema: pa.Schema) -> None:
        """Create the schema, ledger, and table if missing; verify an existing table's columns.

        Raises:
            ValueError: the table name is malformed.
            UnsupportedType: a column type has no Postgres equivalent (nothing is created).
            SchemaMismatch: the table exists with different columns.
        """
        name = parse_table_name(table)
        column_definitions = [_column_definition(table, field) for field in schema]
        await self._pool.run(
            lambda connection: _prepare(connection, name, schema, column_definitions)
        )

    async def write_chunk(
        self, job_id: str, chunk_id: str, table: str, data: pa.Table
    ) -> ChunkWrite:
        """Load the chunk in one transaction with its ledger row, unless it was loaded before.

        Raises:
            InvalidKey: a job id or chunk id breaks the key rules.
            ValueError: the table name is malformed.
            TableNotPrepared: the table does not exist.
            ConnectorError: the database refused the data (the transaction is rolled back).
        """
        validate_key(job_id)
        validate_key(chunk_id)
        name = parse_table_name(table)
        return await self._pool.run(
            lambda connection: _write(connection, name, job_id, chunk_id, data)
        )

    async def count_rows(self, table: str) -> int:
        """Return ``count(*)`` of the table.

        Raises:
            TableNotPrepared: the table does not exist.
        """
        name = parse_table_name(table)

        def work(connection: Any) -> int:
            with connection.cursor() as cursor:
                _require_table(cursor, name)
                cursor.execute(f"SELECT count(*) FROM {name.sql()}")
                return int(cursor.fetchall()[0][0])

        return await self._pool.run(work)

    async def aclose(self) -> None:
        """Close all connections."""
        await self._pool.aclose()


def create_destination(url: str) -> PostgresDestination:
    """Build a destination from a ``postgresql://`` URL. This is the plugin entry point."""
    return PostgresDestination(url)


def _column_definition(table: str, field: pa.Field[Any]) -> str:
    """Return ``"name" type [NOT NULL]`` for one column.

    The native type hint is interpolated into DDL, so it is validated against the whitelist of
    known types first: metadata coming from another system must not be able to inject SQL.
    """
    hint = native_type(field, ENGINE)
    if hint is not None:
        sql_type = check_supported(table, field.name, hint).native
    else:
        sql_type = arrow_to_postgres(table, field.name, field.type)
    return f"{quote_ident(field.name)} {sql_type}{'' if field.nullable else ' NOT NULL'}"


def _require_table(cursor: Any, name: TableName) -> None:
    cursor.execute(_TABLE_EXISTS, parameters=(name.sql(),))
    if not cursor.fetchall()[0][0]:
        raise TableNotPrepared(str(name))


def _prepare(
    connection: Any, name: TableName, schema: pa.Schema, column_definitions: list[str]
) -> None:
    with connection.cursor() as cursor:
        if name.schema is not None:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(name.schema)}")
        cursor.execute(_CREATE_LEDGER)

        cursor.execute(_TABLE_EXISTS, parameters=(name.sql(),))
        if cursor.fetchall()[0][0]:
            cursor.execute(_COLUMN_NAMES, parameters=(name.sql(),))
            existing = [str(row[0]) for row in cursor.fetchall()]
            if existing != schema.names:
                connection.rollback()
                raise SchemaMismatch(
                    f"table {name} has columns {existing}, but {schema.names} are being loaded"
                )
        else:
            cursor.execute(f"CREATE TABLE {name.sql()} ({', '.join(column_definitions)})")
    connection.commit()


def _write(
    connection: Any, name: TableName, job_id: str, chunk_id: str, data: pa.Table
) -> ChunkWrite:
    with connection.cursor() as cursor:
        _require_table(cursor, name)
        cursor.execute(_RECORD_CHUNK, parameters=(job_id, str(name), chunk_id, data.num_rows))
        if not cursor.fetchall():  # the ledger already has this chunk: it was loaded before
            connection.rollback()
            return ChunkWrite(rows=data.num_rows, already_loaded=True)
        if data.num_rows:
            cursor.adbc_ingest(name.name, data, mode="append", db_schema_name=name.schema)
    connection.commit()
    return ChunkWrite(rows=data.num_rows, already_loaded=False)
