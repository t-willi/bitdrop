"""PostgreSQL-specific destination behaviour: DDL, ledger, atomicity, safety (needs Docker)."""

from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest

from bitdrop_api.errors import ConnectorError, SchemaMismatch, UnsupportedType
from bitdrop_api.schema import with_native_type
from bitdrop_api.testing import SAMPLE_SCHEMA, make_rows
from bitdrop_connector_postgres import PostgresDestination, create_destination

if TYPE_CHECKING:
    from conftest import PostgresClient

pytestmark = pytest.mark.docker


def schema_of(*fields: pa.Field[Any]) -> pa.Schema:
    return pa.schema(list(fields))


def hinted(
    name: str, data_type: pa.DataType, native: str, *, nullable: bool = True
) -> pa.Field[Any]:
    return with_native_type(pa.field(name, data_type, nullable=nullable), "postgresql", native)


class TestTableCreation:
    async def test_native_type_hints_recreate_the_columns_exactly(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        schema = schema_of(
            hinted("id", pa.int64(), "bigint", nullable=False),
            hinted("price", pa.decimal128(12, 2), "numeric(12,2)"),
            hinted("doc", pa.string(), "jsonb"),
            hinted("code", pa.string(), "character varying(10)"),
            hinted("tags", pa.list_(pa.string()), "text[]"),
            hinted("at", pa.timestamp("us"), "timestamp(3) without time zone"),
        )
        destination = PostgresDestination(postgres_url)

        await destination.prepare("t", schema)

        assert pg.column_types(postgres_url, "t") == [
            ("id", "bigint"),
            ("price", "numeric(12,2)"),
            ("doc", "jsonb"),
            ("code", "character varying(10)"),
            ("tags", "text[]"),
            ("at", "timestamp(3) without time zone"),
        ]
        await destination.aclose()

    async def test_arrow_types_alone_map_to_natural_postgres_types(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        destination = PostgresDestination(postgres_url)

        await destination.prepare("t", SAMPLE_SCHEMA)

        assert pg.column_types(postgres_url, "t") == [
            ("id", "bigint"),
            ("name", "text"),
            ("score", "double precision"),
            ("amount", "numeric(12,2)"),
            ("active", "boolean"),
            ("born", "date"),
            ("seen", "timestamp with time zone"),
            ("raw", "bytea"),
        ]
        await destination.aclose()

    async def test_not_null_follows_the_schema(self, postgres_url: str, pg: PostgresClient) -> None:
        destination = PostgresDestination(postgres_url)

        await destination.prepare("t", SAMPLE_SCHEMA)

        nullable = dict(
            pg.query(
                postgres_url,
                "SELECT attname, NOT attnotnull FROM pg_attribute"
                " WHERE attrelid = 't'::regclass AND attnum > 0",
            )
        )
        assert nullable["id"] is False
        assert nullable["name"] is True
        await destination.aclose()

    async def test_hints_from_another_engine_are_ignored(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        field = with_native_type(pa.field("flag", pa.bool_()), "mysql", "tinyint(1)")
        destination = PostgresDestination(postgres_url)

        await destination.prepare("t", pa.schema([field]))

        assert pg.column_types(postgres_url, "t") == [("flag", "boolean")]
        await destination.aclose()

    async def test_a_missing_schema_is_created(self, postgres_url: str, pg: PostgresClient) -> None:
        destination = PostgresDestination(postgres_url)

        await destination.prepare("reports.sales", SAMPLE_SCHEMA)
        await destination.write_chunk("j", "c1", "reports.sales", make_rows(1, 5))

        assert await destination.count_rows("reports.sales") == 5
        assert pg.scalar(postgres_url, "SELECT count(*) FROM reports.sales") == 5
        await destination.aclose()

    async def test_hostile_column_names_are_quoted(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        schema = schema_of(pa.field("select", pa.int64()), pa.field('we"ird', pa.string()))
        data = pa.table({"select": [1, 2], 'we"ird': ["a", "b"]}, schema=schema)
        destination = PostgresDestination(postgres_url)

        await destination.prepare("odd", schema)
        await destination.write_chunk("j", "c1", "odd", data)

        assert pg.scalar(postgres_url, "SELECT count(*) FROM odd") == 2
        await destination.aclose()

    async def test_an_existing_matching_table_is_used_as_it_is(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.create_sample_table(postgres_url, "mine")
        pg.execute(postgres_url, "CREATE INDEX mine_name ON mine (name)")
        destination = PostgresDestination(postgres_url)

        await destination.prepare("mine", SAMPLE_SCHEMA)
        await destination.write_chunk("j", "c1", "mine", make_rows(1, 10))

        assert pg.scalar(postgres_url, "SELECT count(*) FROM mine") == 10
        assert (
            pg.scalar(postgres_url, "SELECT count(*) FROM pg_indexes WHERE tablename='mine'") == 2
        )
        await destination.aclose()

    async def test_the_same_columns_in_another_order_are_a_mismatch(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.execute(postgres_url, "CREATE TABLE swapped (b text, a text)")
        destination = PostgresDestination(postgres_url)

        with pytest.raises(SchemaMismatch, match="swapped"):
            await destination.prepare(
                "swapped", pa.schema([pa.field("a", pa.string()), pa.field("b", pa.string())])
            )
        await destination.aclose()

    async def test_an_arrow_type_without_a_postgres_equivalent_creates_nothing(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        schema = schema_of(pa.field("ok", pa.int32()), pa.field("bad", pa.duration("us")))
        destination = PostgresDestination(postgres_url)

        with pytest.raises(UnsupportedType):
            await destination.prepare("t", schema)

        assert pg.scalar(postgres_url, "SELECT to_regclass('t') IS NULL") is True
        await destination.aclose()


class TestLedger:
    async def test_every_written_chunk_is_recorded(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        destination = PostgresDestination(postgres_url)
        await destination.prepare("t", SAMPLE_SCHEMA)

        await destination.write_chunk("job-1", "c1", "t", make_rows(1, 10))
        await destination.write_chunk("job-1", "c2", "t", make_rows(11, 5))

        assert pg.query(
            postgres_url,
            "SELECT job_id, table_name, chunk_id, row_count FROM _bitdrop_chunks ORDER BY chunk_id",
        ) == [("job-1", "t", "c1", 10), ("job-1", "t", "c2", 5)]
        await destination.aclose()

    async def test_the_ledger_survives_a_new_destination_object(self, postgres_url: str) -> None:
        """A restarted process must still recognise chunks that an earlier one loaded."""
        first = PostgresDestination(postgres_url)
        await first.prepare("t", SAMPLE_SCHEMA)
        await first.write_chunk("job-1", "c1", "t", make_rows(1, 10))
        await first.aclose()

        second = PostgresDestination(postgres_url)
        await second.prepare("t", SAMPLE_SCHEMA)
        again = await second.write_chunk("job-1", "c1", "t", make_rows(1, 10))

        assert again.already_loaded is True
        assert await second.count_rows("t") == 10
        await second.aclose()

    async def test_a_failed_write_leaves_no_trace_and_does_not_poison_the_chunk_id(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        """Data and ledger row are one transaction: a failure undoes both."""
        destination = PostgresDestination(postgres_url)
        await destination.prepare("t", SAMPLE_SCHEMA)
        bad = make_rows(1, 5).set_column(
            0,
            SAMPLE_SCHEMA.field("id").with_nullable(True),
            pa.array([1, None, 3, 4, 5], pa.int64()),
        )

        with pytest.raises(ConnectorError):
            await destination.write_chunk("job-1", "c1", "t", bad)  # NULL in a NOT NULL column

        assert pg.scalar(postgres_url, "SELECT count(*) FROM _bitdrop_chunks") == 0
        assert await destination.count_rows("t") == 0
        retry = await destination.write_chunk("job-1", "c1", "t", make_rows(1, 5))
        assert (retry.rows, retry.already_loaded) == (5, False)
        assert await destination.count_rows("t") == 5
        await destination.aclose()

    async def test_data_with_unexpected_columns_is_rejected_without_a_trace(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        destination = PostgresDestination(postgres_url)
        await destination.prepare("t", SAMPLE_SCHEMA)
        wrong = make_rows(1, 3).append_column("surprise", pa.array([1, 2, 3]))

        with pytest.raises(ConnectorError):
            await destination.write_chunk("job-1", "c1", "t", wrong)

        assert pg.scalar(postgres_url, "SELECT count(*) FROM _bitdrop_chunks") == 0
        await destination.aclose()


class TestArrays:
    """The installed ADBC COPY writer turns NULL array *elements* into 0 or '' without an error.
    A destination must store a value exactly or refuse it, so such chunks are refused."""

    SCHEMA = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            hinted("tags", pa.list_(pa.string()), "text[]"),
            hinted("nums", pa.list_(pa.int32()), "integer[]"),
        ]
    )

    def table(self, tags: list[object], nums: list[object]) -> pa.Table:
        ids = pa.array(range(1, len(tags) + 1), pa.int64())
        return pa.table(
            {
                "id": ids,
                "tags": pa.array(tags, pa.list_(pa.string())),
                "nums": pa.array(nums, pa.list_(pa.int32())),
            },
            schema=self.SCHEMA,
        )

    async def test_arrays_that_are_empty_missing_or_full_load_exactly(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        destination = PostgresDestination(postgres_url)
        await destination.prepare("t", self.SCHEMA)
        data = self.table([["a", "", "c"], [], None, ["only"]], [[1, -2, 3], [], None, [0]])

        await destination.write_chunk("j", "c1", "t", data)

        assert pg.query(postgres_url, "SELECT tags::text, nums::text FROM t ORDER BY id") == [
            ('{a,"",c}', "{1,-2,3}"),
            ("{}", "{}"),
            (None, None),
            ("{only}", "{0}"),
        ]
        await destination.aclose()

    @pytest.mark.parametrize(
        ("tags", "nums"),
        [
            ([["a", None]], [[1]]),
            ([["a"]], [[1, None]]),
        ],
        ids=["null-text-element", "null-int-element"],
    )
    async def test_null_array_elements_are_refused_not_corrupted(
        self, postgres_url: str, pg: PostgresClient, tags: list[object], nums: list[object]
    ) -> None:
        destination = PostgresDestination(postgres_url)
        await destination.prepare("t", self.SCHEMA)

        with pytest.raises(ConnectorError, match="NULL elements"):
            await destination.write_chunk("j", "c1", "t", self.table(tags, nums))

        assert pg.scalar(postgres_url, "SELECT count(*) FROM t") == 0
        assert pg.scalar(postgres_url, "SELECT count(*) FROM _bitdrop_chunks") == 0
        await destination.aclose()

    async def test_a_chunk_refused_for_null_elements_does_not_poison_the_destination(
        self, postgres_url: str
    ) -> None:
        destination = PostgresDestination(postgres_url)
        await destination.prepare("t", self.SCHEMA)
        with pytest.raises(ConnectorError):
            await destination.write_chunk("j", "bad", "t", self.table([["a", None]], [[1]]))

        good = await destination.write_chunk("j", "good", "t", self.table([["a"]], [[1]]))

        assert (good.rows, good.already_loaded) == (1, False)
        assert await destination.count_rows("t") == 1
        await destination.aclose()


class TestSafety:
    @pytest.mark.parametrize("name", ["t; DROP TABLE victim", 't"', "a.b.c", "", "1t"])
    async def test_malicious_or_malformed_table_names_are_refused_before_any_sql(
        self, postgres_url: str, pg: PostgresClient, name: str
    ) -> None:
        pg.execute(postgres_url, "CREATE TABLE victim (id int)")
        destination = PostgresDestination(postgres_url)

        with pytest.raises(ValueError, match="table name"):
            await destination.prepare(name, SAMPLE_SCHEMA)
        with pytest.raises(ValueError, match="table name"):
            await destination.write_chunk("j", "c1", name, make_rows(1, 1))

        assert pg.scalar(postgres_url, "SELECT to_regclass('victim') IS NOT NULL") is True
        await destination.aclose()

    async def test_a_hostile_native_type_hint_cannot_inject_sql(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        """Hints come from another system's metadata and end up in DDL, so they are whitelisted."""
        pg.execute(postgres_url, "CREATE TABLE victim (id int)")
        evil = hinted("x", pa.string(), "text); DROP TABLE victim; --")
        destination = PostgresDestination(postgres_url)

        with pytest.raises(UnsupportedType):
            await destination.prepare("t", pa.schema([evil]))

        assert pg.scalar(postgres_url, "SELECT to_regclass('victim') IS NOT NULL") is True
        await destination.aclose()

    async def test_a_connection_failure_does_not_leak_the_password(self) -> None:
        destination = create_destination("postgresql://alice:hunter2@127.0.0.1:1/nowhere")

        with pytest.raises(ConnectorError) as excinfo:
            await destination.prepare("t", SAMPLE_SCHEMA)

        assert "hunter2" not in str(excinfo.value)
        assert "alice:***@127.0.0.1:1" in str(excinfo.value)
        await destination.aclose()

    @pytest.mark.parametrize("url", ["mysql://h/db", "postgresql://h/db?bitdrop_nope=1"])
    def test_bad_urls_are_rejected_when_the_destination_is_created(self, url: str) -> None:
        with pytest.raises(ValueError, match=r"postgresql|bitdrop_nope"):
            create_destination(url)


class TestVolume:
    async def test_a_large_chunk_loads_in_one_go(self, postgres_url: str) -> None:
        destination = PostgresDestination(postgres_url)
        await destination.prepare("t", SAMPLE_SCHEMA)

        result = await destination.write_chunk("j", "c1", "t", make_rows(1, 100_000))

        assert result.rows == 100_000
        assert await destination.count_rows("t") == 100_000
        await destination.aclose()
