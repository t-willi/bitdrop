"""PostgreSQL -> PostgreSQL through the registry and the transfer runner (needs Docker).

Everything is chosen by URL, exactly as a deployment would. After each transfer a fingerprint of
both tables (row count plus an md5 over every row's full text, in key order) must be identical,
and so must the column types.

Run with ``-s`` to see the throughput printed by the big test:
``uv run pytest tests/e2e -s``.
"""

import asyncio
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from bitdrop_api.errors import UnsupportedType
from bitdrop_api.metadata_store import ChunkState
from bitdrop_connector_postgres import PostgresDestination
from bitdrop_core.registry import (
    open_chunk_store,
    open_destination,
    open_metadata_store,
    open_source,
)
from bitdrop_core.transfer import TransferConfig, TransferReport, run_transfer

if TYPE_CHECKING:
    from conftest import KvServer, PostgresClient, PostgresServer

pytestmark = pytest.mark.docker

MIB = 1024 * 1024
TIMEOUT = 180.0


@pytest.fixture
def source_url(postgres_server: PostgresServer) -> str:
    return postgres_server.create_database()


@pytest.fixture
def destination_url(postgres_server: PostgresServer) -> str:
    return postgres_server.create_database()


def fingerprint(pg: PostgresClient, url: str, table: str, key: str = "id") -> tuple[int, str]:
    """Row count and an md5 over every row's text form, in key order."""
    ((count, digest),) = pg.query(
        url, f"SELECT count(*), md5(string_agg(t::text, '|' ORDER BY t.{key})) FROM {table} t"
    )
    return int(count), str(digest)


def differing_columns(
    pg: PostgresClient, source_url: str, destination_url: str, table: str, columns: list[str]
) -> dict[str, tuple[str, str]]:
    """Per column, the values (as text, in key order) that differ between source and destination."""
    differing = {}
    for column in columns:
        query = f"SELECT string_agg(coalesce(t.{column}::text, '<NULL>'), ' | ' ORDER BY t.id) FROM {table} t"
        source_value = pg.scalar(source_url, query)
        destination_value = pg.scalar(destination_url, query)
        if source_value != destination_value:
            differing[column] = (str(source_value)[:300], str(destination_value)[:300])
    return differing


async def transfer(
    source_url: str,
    destination_url: str,
    *,
    source_table: str,
    destination_table: str,
    job_id: str = "job-1",
    chunk_store: str = "memory://",
    metadata_store: str = "memory://",
    destination: PostgresDestination | None = None,
    **config: object,
) -> TransferReport:
    source = open_source(source_url)
    sink = destination or open_destination(destination_url)
    chunks = open_chunk_store(chunk_store)
    meta = open_metadata_store(metadata_store)
    settings: dict[str, object] = {"target_chunk_bytes": MIB, "extractors": 4, "loaders": 4}
    cfg = TransferConfig(
        job_id=job_id,
        source_table=source_table,
        destination_table=destination_table,
        **{**settings, **config},  # type: ignore[arg-type]
    )
    try:
        async with asyncio.timeout(TIMEOUT):
            return await run_transfer(source, sink, chunks, meta, cfg)
    finally:
        for component in (source, sink, chunks, meta):
            await component.aclose()


# -- the big one ------------------------------------------------------------------------------------

ORDERS_DDL = """
CREATE TABLE orders (
    id bigint PRIMARY KEY, customer text NOT NULL, amount numeric(12,2), qty integer,
    ratio double precision, placed timestamptz, shipped date, paid boolean, note text,
    doc jsonb, tags text[], token uuid, blob bytea
)"""
ORDERS_FILL = """
INSERT INTO orders SELECT i, 'customer-' || (i % 5000), (i % 100000) / 100.0, i % 50, random(),
    now() - (i || ' seconds')::interval, current_date - (i % 365), i % 3 = 0,
    CASE WHEN i % 10 = 0 THEN NULL ELSE repeat('x', i % 200) END,
    jsonb_build_object('i', i, 'k', 'v' || (i % 7)), ARRAY['a', 'b' || (i % 5)],
    md5(i::text)::uuid, decode(md5(i::text), 'hex')
FROM generate_series(1, {rows}) i"""


async def test_a_wide_table_arrives_exactly(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    rows = 200_000
    pg.execute(source_url, ORDERS_DDL)
    pg.execute(source_url, ORDERS_FILL.format(rows=rows))
    pg.execute(source_url, "ANALYZE orders")

    report = await transfer(
        source_url, destination_url, source_table="orders", destination_table="orders_copy"
    )

    assert report.succeeded, report.failures
    assert report.rows_loaded == rows
    assert report.duplicates_skipped == 0
    assert report.chunks_total > 10, "the table should have been split into many chunks"
    assert fingerprint(pg, destination_url, "orders_copy") == fingerprint(pg, source_url, "orders")
    assert pg.column_types(destination_url, "orders_copy") == pg.column_types(source_url, "orders")
    megabytes = report.bytes_staged / MIB
    print(
        f"\n[e2e] {rows:,} rows in {report.chunks_total} chunks: {report.duration_seconds:.2f}s, "
        f"{rows / report.duration_seconds:,.0f} rows/s, {megabytes / report.duration_seconds:.1f} MiB/s "
        f"through the staging tier ({megabytes:.1f} MiB)"
    )


# -- every supported type ---------------------------------------------------------------------------

TYPE_COLUMNS = [
    ("c_smallint", "smallint"),
    ("c_integer", "integer"),
    ("c_bigint", "bigint"),
    ("c_real", "real"),
    ("c_double", "double precision"),
    ("c_bool", "boolean"),
    ("c_text", "text"),
    ("c_varchar", "varchar(10)"),
    ("c_char", "char(3)"),
    ("c_date", "date"),
    ("c_ts", "timestamp"),
    ("c_ts3", "timestamp(3)"),
    ("c_tstz", "timestamptz"),
    ("c_time", "time"),
    ("c_bytea", "bytea"),
    ("c_uuid", "uuid"),
    ("c_json", "json"),
    ("c_jsonb", "jsonb"),
    ("c_inet", "inet"),
    ("c_interval", "interval"),
    ("c_num_small", "numeric(12,2)"),
    ("c_num_wide", "numeric(38,10)"),
    ("c_text_arr", "text[]"),
    ("c_int_arr", "integer[]"),
    ("c_bigint_arr", "bigint[]"),
    ("c_varchar_arr", "varchar(10)[]"),
]
TYPE_ROWS = """
INSERT INTO kitchen_sink VALUES
 (1, -32768, -2147483648, -9223372036854775808, -1.5, -1e300, true, 'héllo ✓ 日本語 😀', 'abc', 'xy',
  '1900-01-01', '1900-01-01 00:00:00', '2024-02-29 23:59:59.123', '1900-01-01 00:00:00+00', '00:00:00',
  '\\x', '00000000-0000-0000-0000-000000000000', '{"a": [1, 2, {"b": null}]}', '{"a": [1, 2, {"b": null}]}',
  '10.0.0.1/8', '-1 year -2 mons -3 days -04:05:06.789', -9999999999.99, -12345678901234567890.1234567891,
  ARRAY['a', 'b', ''], ARRAY[1, 0, -3], ARRAY[-9223372036854775808, 0], ARRAY['x', 'y']),
 (2, 32767, 2147483647, 9223372036854775807, 3.4e38, 1.7e308, false, '', '', '',
  '2999-12-31', '2999-12-31 23:59:59.999999', '2999-12-31 23:59:59.999', '2999-12-31 23:59:59.999999+00', '23:59:59.999999',
  '\\x00ff00', 'ffffffff-ffff-ffff-ffff-ffffffffffff', '[]', '{}', '2001:db8::1', '1 year 2 mons 3 days 04:05:06.789',
  9999999999.99, 12345678901234567890.1234567891, ARRAY[]::text[], ARRAY[]::integer[], ARRAY[]::bigint[], ARRAY[]::varchar[]),
 (3, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL),
 (4, 1, 1, 1, 'NaN', 'Infinity', true, repeat('long ', 20000), 'ten chars!', 'ab',
  '2024-06-15', '2024-06-15 12:30:00.5', '2024-06-15 12:30:00.5', '2024-06-15 12:30:00.5+02', '12:30:00.5',
  decode(repeat('ab', 100000), 'hex'), gen_random_uuid(), '{"k": "v"}', '{"k": "v", "n": 1.50}',
  '192.168.1.5', '0 seconds', 0.00, 0.0000000001, ARRAY['one'], ARRAY[1], ARRAY[1], ARRAY['v'])
"""


async def test_every_supported_type_survives_the_trip(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in TYPE_COLUMNS)
    pg.execute(source_url, f"CREATE TABLE kitchen_sink (id integer PRIMARY KEY, {columns})")
    pg.execute(source_url, TYPE_ROWS)

    report = await transfer(
        source_url, destination_url, source_table="kitchen_sink", destination_table="kitchen_sink"
    )

    assert report.succeeded, report.failures
    differing = differing_columns(
        pg, source_url, destination_url, "kitchen_sink", [name for name, _ in TYPE_COLUMNS]
    )
    assert differing == {}, "these columns changed in transit (source, destination)"
    assert fingerprint(pg, destination_url, "kitchen_sink") == fingerprint(
        pg, source_url, "kitchen_sink"
    )
    assert pg.column_types(destination_url, "kitchen_sink") == pg.column_types(
        source_url, "kitchen_sink"
    )


async def test_an_unsupported_column_stops_the_job_before_any_data_moves(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    pg.execute(source_url, "CREATE TABLE priced (id int PRIMARY KEY, price money)")
    pg.execute(source_url, "INSERT INTO priced VALUES (1, 9.99)")

    with pytest.raises(UnsupportedType, match="price"):
        await transfer(
            source_url, destination_url, source_table="priced", destination_table="priced"
        )

    assert pg.scalar(destination_url, "SELECT to_regclass('priced') IS NULL") is True


async def test_arrays_with_null_elements_fail_loudly_instead_of_arriving_corrupted(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    """The ADBC COPY writer would store a NULL element as 0 or ''. bitdrop refuses such chunks."""
    pg.execute(source_url, "CREATE TABLE lists (id bigint PRIMARY KEY, tags text[])")
    pg.execute(
        source_url,
        "INSERT INTO lists SELECT i, CASE WHEN i = 150 THEN ARRAY['a', NULL] ELSE ARRAY['a', 'b'] END"
        " FROM generate_series(1, 300) i",
    )

    report = await transfer(
        source_url,
        destination_url,
        source_table="lists",
        destination_table="lists",
        target_chunk_bytes=2000,
        max_attempts=2,
    )

    assert not report.succeeded
    assert report.chunks_failed == 1
    assert report.failures[0].last_error is not None
    assert "NULL elements" in report.failures[0].last_error
    assert pg.scalar(destination_url, "SELECT count(*) FROM lists WHERE tags = '{a,b}'") == (
        pg.scalar(destination_url, "SELECT count(*) FROM lists")
    ), "a row with a wrongly stored array reached the destination"


# -- shapes of tables -------------------------------------------------------------------------------


async def test_schema_qualified_names_on_both_sides(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    pg.execute(source_url, "CREATE SCHEMA sales")
    pg.create_sample_table(source_url, "sales.orders", rows=5000)

    report = await transfer(
        source_url,
        destination_url,
        source_table="sales.orders",
        destination_table="warehouse.orders_copy",
        target_chunk_bytes=50_000,
    )

    assert report.succeeded
    assert fingerprint(pg, destination_url, "warehouse.orders_copy") == fingerprint(
        pg, source_url, "sales.orders"
    )


async def test_a_table_without_a_primary_key_is_copied_as_one_chunk(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    pg.execute(source_url, "CREATE TABLE loose (id int, v text)")
    pg.execute(source_url, "INSERT INTO loose SELECT i, 'v' || i FROM generate_series(1, 20000) i")

    report = await transfer(
        source_url, destination_url, source_table="loose", destination_table="loose"
    )

    assert report.succeeded
    assert report.chunks_total == 1
    assert fingerprint(pg, destination_url, "loose") == fingerprint(pg, source_url, "loose")


async def test_sparse_keys_are_copied_in_a_sensible_number_of_chunks(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    pg.execute(source_url, "CREATE TABLE sparse (id bigint PRIMARY KEY, v text)")
    pg.execute(
        source_url,
        "INSERT INTO sparse SELECT i::bigint * 1000003, repeat('s', 100) FROM generate_series(1, 5000) i",
    )
    pg.execute(source_url, "ANALYZE sparse")

    report = await transfer(
        source_url,
        destination_url,
        source_table="sparse",
        destination_table="sparse",
        target_chunk_bytes=50_000,
    )

    assert report.succeeded
    assert 5 < report.chunks_total < 500, report.chunks_total
    assert fingerprint(pg, destination_url, "sparse") == fingerprint(pg, source_url, "sparse")


async def test_an_empty_table_creates_an_empty_copy(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    pg.create_sample_table(source_url, "nothing")

    report = await transfer(
        source_url, destination_url, source_table="nothing", destination_table="nothing"
    )

    assert report.succeeded
    assert (report.chunks_total, report.rows_loaded) == (0, 0)
    assert pg.scalar(destination_url, "SELECT count(*) FROM nothing") == 0


# -- faults against the real database ---------------------------------------------------------------


class CommitThenDieDestination(PostgresDestination):
    """Really commits a chunk to PostgreSQL, then fails once, as if the loader died right after."""

    def __init__(self, url: str, victim: str) -> None:
        super().__init__(url)
        self.victim, self.struck = victim, False

    async def write_chunk(self, job_id: str, chunk_id: str, table: str, data: pa.Table):  # type: ignore[no-untyped-def]
        result = await super().write_chunk(job_id, chunk_id, table, data)
        if chunk_id == self.victim and not self.struck:
            self.struck = True
            raise ConnectionError("loader died after the commit")
        return result


async def test_a_loader_dying_after_its_commit_causes_no_duplicate_rows(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    pg.create_sample_table(source_url, "people", rows=20_000)

    report = await transfer(
        source_url,
        destination_url,
        source_table="people",
        destination_table="people",
        destination=CommitThenDieDestination(destination_url, victim="c000003"),
        target_chunk_bytes=100_000,
    )

    assert report.succeeded
    assert report.duplicates_skipped == 1
    assert pg.scalar(destination_url, "SELECT count(*) FROM people") == 20_000
    assert fingerprint(pg, destination_url, "people") == fingerprint(pg, source_url, "people")


async def test_running_the_same_job_again_changes_nothing(
    source_url: str, destination_url: str, pg: PostgresClient
) -> None:
    pg.create_sample_table(source_url, "people", rows=5000)
    first = await transfer(
        source_url,
        destination_url,
        source_table="people",
        destination_table="people",
        target_chunk_bytes=50_000,
    )

    second = await transfer(
        source_url,
        destination_url,
        source_table="people",
        destination_table="people",
        target_chunk_bytes=50_000,
    )

    assert first.rows_loaded == 5000
    assert (second.rows_loaded, second.duplicates_skipped) == (0, second.chunks_total)
    assert fingerprint(pg, destination_url, "people") == fingerprint(pg, source_url, "people")


class KillSwitchDestination(PostgresDestination):
    """Signals after ``limit`` chunks were committed, then makes every further write slow."""

    def __init__(self, url: str, limit: int, reached: asyncio.Event) -> None:
        super().__init__(url)
        self.limit, self.reached, self.committed = limit, reached, 0

    async def write_chunk(self, job_id: str, chunk_id: str, table: str, data: pa.Table):  # type: ignore[no-untyped-def]
        if self.committed >= self.limit:
            await asyncio.sleep(0.5)  # hold the rest back so the 'crash' lands mid-job
        result = await super().write_chunk(job_id, chunk_id, table, data)
        self.committed += 1
        if self.committed >= self.limit:
            self.reached.set()
        return result


@pytest.mark.parametrize("chunk_store", ["memory://", "file://{tmp}/chunks"])
async def test_a_killed_run_resumes_to_the_same_result(
    source_url: str,
    destination_url: str,
    pg: PostgresClient,
    tmp_path: Path,
    chunk_store: str,
) -> None:
    """Kill the run midway (state in SQLite, staging in memory or on disk), then run it again.

    With ``file://`` the staged chunks survive the 'crash' and are loaded as they are; with
    ``memory://`` they are lost, and the loaders notice and have the chunks extracted again.
    """
    pg.create_sample_table(source_url, "people", rows=30_000)
    common: dict[str, object] = {
        "source_table": "people",
        "destination_table": "people",
        "chunk_store": chunk_store.format(tmp=tmp_path),
        "metadata_store": f"sqlite://{tmp_path}/meta.db",
        "target_chunk_bytes": 60_000,
        "lease_seconds": 1.0,
        "reaper_interval": 0.1,
    }
    reached = asyncio.Event()
    first_run = asyncio.create_task(
        transfer(
            source_url,
            destination_url,
            destination=KillSwitchDestination(destination_url, limit=5, reached=reached),
            **common,  # type: ignore[arg-type]
        )
    )
    await asyncio.wait_for(reached.wait(), timeout=60)
    first_run.cancel()  # the 'crash': every worker vanishes mid-chunk
    with suppress(asyncio.CancelledError):
        await first_run
    partial = pg.scalar(destination_url, "SELECT count(*) FROM people")
    assert 0 < partial < 30_000, f"{partial} rows after the crash: the test proves nothing"

    resumed = await transfer(source_url, destination_url, **common)  # type: ignore[arg-type]

    assert resumed.succeeded, resumed.failures
    assert pg.scalar(destination_url, "SELECT count(*) FROM people") == 30_000
    assert fingerprint(pg, destination_url, "people") == fingerprint(pg, source_url, "people")


async def test_stores_chosen_by_url_work_with_real_databases(
    source_url: str, destination_url: str, pg: PostgresClient, tmp_path: Path
) -> None:
    pg.create_sample_table(source_url, "people", rows=10_000)

    report = await transfer(
        f"{source_url}?bitdrop_pool_size=2",
        f"{destination_url}?bitdrop_pool_size=2",
        source_table="people",
        destination_table="people",
        chunk_store=f"file://{tmp_path}/chunks",
        metadata_store=f"sqlite://{tmp_path}/meta.db",
        target_chunk_bytes=80_000,
    )

    assert report.succeeded
    assert fingerprint(pg, destination_url, "people") == fingerprint(pg, source_url, "people")
    assert ChunkState.LOADED.value == "loaded"


# -- staging in a network chunk store -----------------------------------------------------------------
# The chunks now cross a real network hop: extractor -> Redis-protocol server -> loader.


async def test_a_wide_table_through_a_network_chunk_store(
    source_url: str, destination_url: str, pg: PostgresClient, kv: KvServer
) -> None:
    rows = 200_000
    pg.execute(source_url, ORDERS_DDL)
    pg.execute(source_url, ORDERS_FILL.format(rows=rows))
    pg.execute(source_url, "ANALYZE orders")

    report = await transfer(
        source_url,
        destination_url,
        source_table="orders",
        destination_table="orders_copy",
        chunk_store=kv.url,
    )

    assert report.succeeded, report.failures
    assert report.rows_loaded == rows
    assert fingerprint(pg, destination_url, "orders_copy") == fingerprint(pg, source_url, "orders")
    assert kv.client().dbsize() == 0, "chunks were left behind in the staging server"
    megabytes = report.bytes_staged / MIB
    print(
        f"\n[e2e] staging in {kv.flavor}: {rows:,} rows in {report.chunks_total} chunks: "
        f"{report.duration_seconds:.2f}s, {rows / report.duration_seconds:,.0f} rows/s, "
        f"{megabytes / report.duration_seconds:.1f} MiB/s through the staging tier"
    )


class WipeStagingDestination(PostgresDestination):
    """Wipes the staging server after ``after`` chunks were loaded, like a crash or restart would."""

    def __init__(self, url: str, wipe: Callable[[], None], after: int) -> None:
        super().__init__(url)
        self.wipe, self.after, self.committed = wipe, after, 0

    async def write_chunk(self, job_id: str, chunk_id: str, table: str, data: pa.Table):  # type: ignore[no-untyped-def]
        await asyncio.sleep(0.02)  # let extraction run ahead so many chunks are staged
        result = await super().write_chunk(job_id, chunk_id, table, data)
        self.committed += 1
        if self.committed == self.after:
            self.wipe()
        return result


async def test_wiping_the_staging_server_mid_job_loses_no_row_and_duplicates_none(
    source_url: str, destination_url: str, pg: PostgresClient, kv: KvServer
) -> None:
    """RAM is volatile by design: lost chunks are simply extracted again from the source."""
    pg.create_sample_table(source_url, "people", rows=30_000)

    report = await transfer(
        source_url,
        destination_url,
        source_table="people",
        destination_table="people",
        chunk_store=kv.url,
        target_chunk_bytes=60_000,
        destination=WipeStagingDestination(destination_url, kv.client().flushall, after=5),
    )

    assert report.succeeded, report.failures
    assert report.rows_extracted > 30_000, "nothing was lost, so the test proves nothing"
    assert pg.scalar(destination_url, "SELECT count(*) FROM people") == 30_000
    assert fingerprint(pg, destination_url, "people") == fingerprint(pg, source_url, "people")


async def test_a_killed_run_resumes_with_the_chunks_still_staged_in_the_server(
    source_url: str, destination_url: str, pg: PostgresClient, kv: KvServer, tmp_path: Path
) -> None:
    """The staging server outlives the crashed workers, so the resumed run loads what is there."""
    pg.create_sample_table(source_url, "people", rows=30_000)
    common: dict[str, object] = {
        "source_table": "people",
        "destination_table": "people",
        "chunk_store": kv.url,
        "metadata_store": f"sqlite://{tmp_path}/meta.db",
        "target_chunk_bytes": 60_000,
        "lease_seconds": 1.0,
        "reaper_interval": 0.1,
    }
    reached = asyncio.Event()
    first_run = asyncio.create_task(
        transfer(
            source_url,
            destination_url,
            destination=KillSwitchDestination(destination_url, limit=5, reached=reached),
            **common,  # type: ignore[arg-type]
        )
    )
    await asyncio.wait_for(reached.wait(), timeout=60)
    first_run.cancel()
    with suppress(asyncio.CancelledError):
        await first_run
    assert kv.client().dbsize() > 0, "no chunk survived in the server: the test proves nothing"

    resumed = await transfer(source_url, destination_url, **common)  # type: ignore[arg-type]

    assert resumed.succeeded, resumed.failures
    assert fingerprint(pg, destination_url, "people") == fingerprint(pg, source_url, "people")
    assert kv.client().dbsize() == 0
