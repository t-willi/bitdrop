"""PostgreSQL-specific source behaviour: splitting, types, naming, safety (needs Docker)."""

import itertools
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from bitdrop_api.errors import ConnectorError, TableNotFound, UnsupportedType
from bitdrop_api.schema import native_type
from bitdrop_api.testing import make_rows
from bitdrop_connector_postgres import PostgresSource, create_source

if TYPE_CHECKING:
    from conftest import PostgresClient

pytestmark = pytest.mark.docker

MIB = 1024 * 1024


async def read_all(source: PostgresSource, table: str, target: int = MIB) -> pa.Table:
    plan = await source.plan(table, target_chunk_bytes=target)
    tables = [await source.read_chunk(plan, chunk) for chunk in plan.chunks]
    return pa.concat_tables(tables) if tables else plan.schema.empty_table()


class TestSplitting:
    async def test_an_integer_primary_key_is_split_into_contiguous_ranges(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.create_sample_table(postgres_url, "t", rows=2000)
        source = PostgresSource(postgres_url)

        plan = await source.plan("t", target_chunk_bytes=50_000)

        ranges = [(c.payload["lo"], c.payload["hi"]) for c in plan.chunks]
        assert len(ranges) > 3
        assert ranges[0][0] == 1
        assert ranges[-1][1] == 2000
        assert all(prev[1] + 1 == nxt[0] for prev, nxt in itertools.pairwise(ranges))
        assert [c.chunk_id for c in plan.chunks] == [f"c{i:06d}" for i in range(len(ranges))]
        await source.aclose()

    async def test_bigger_targets_make_fewer_chunks(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.create_sample_table(postgres_url, "t", rows=2000)
        source = PostgresSource(postgres_url)

        small = await source.plan("t", target_chunk_bytes=20_000)
        large = await source.plan("t", target_chunk_bytes=200_000)

        assert len(large.chunks) < len(small.chunks)
        await source.aclose()

    async def test_row_estimates_are_reported(self, postgres_url: str, pg: PostgresClient) -> None:
        pg.create_sample_table(postgres_url, "t", rows=1000)
        pg.execute(postgres_url, "ANALYZE t")
        source = PostgresSource(postgres_url)

        plan = await source.plan("t", target_chunk_bytes=MIB)

        assert plan.estimated_rows == 1000
        assert plan.estimated_row_bytes is not None
        assert plan.estimated_row_bytes > 10
        await source.aclose()

    async def test_sparse_keys_do_not_explode_into_empty_chunks(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        """100 rows spread over 10 million ids must not become 10 million tiny ranges."""
        pg.execute(postgres_url, "CREATE TABLE sparse (id bigint PRIMARY KEY, v text)")
        pg.execute(
            postgres_url,
            "INSERT INTO sparse SELECT i * 100000, 'x' FROM generate_series(1, 100) i",
        )
        source = PostgresSource(postgres_url)

        plan = await source.plan("sparse", target_chunk_bytes=2000)

        assert 1 < len(plan.chunks) <= 100
        assert (await read_all(source, "sparse", 2000)).num_rows == 100
        await source.aclose()

    async def test_the_extremes_of_bigint_do_not_overflow(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.execute(postgres_url, "CREATE TABLE wide (id bigint PRIMARY KEY)")
        pg.execute(
            postgres_url,
            "INSERT INTO wide VALUES (-9223372036854775808), (-1), (0), (1), (9223372036854775807)",
        )
        source = PostgresSource(postgres_url)

        table = await read_all(source, "wide", 1)

        assert sorted(cast(list[int], table.column("id").to_pylist())) == [
            -9223372036854775808,
            -1,
            0,
            1,
            9223372036854775807,
        ]
        await source.aclose()

    async def test_a_table_without_a_primary_key_is_read_as_one_chunk_and_says_so(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.execute(postgres_url, "CREATE TABLE loose (a int, b text)")
        pg.execute(postgres_url, "INSERT INTO loose SELECT i, 'x' FROM generate_series(1, 500) i")
        source = PostgresSource(postgres_url)

        plan = await source.plan("loose", target_chunk_bytes=1)

        assert len(plan.chunks) == 1
        assert any("single chunk" in note for note in plan.notes)
        assert (await read_all(source, "loose")).num_rows == 500
        await source.aclose()

    async def test_a_composite_primary_key_falls_back_to_one_chunk(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.execute(postgres_url, "CREATE TABLE pair (a int, b int, PRIMARY KEY (a, b))")
        pg.execute(postgres_url, "INSERT INTO pair SELECT i, i FROM generate_series(1, 50) i")
        source = PostgresSource(postgres_url)

        plan = await source.plan("pair", target_chunk_bytes=1)

        assert len(plan.chunks) == 1
        await source.aclose()

    async def test_a_split_column_hint_overrides_the_choice(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.execute(postgres_url, "CREATE TABLE hinted (a int, b int NOT NULL)")
        pg.execute(postgres_url, "INSERT INTO hinted SELECT i, i FROM generate_series(1, 300) i")
        source = PostgresSource(postgres_url)

        plan = await source.plan("hinted", target_chunk_bytes=100, split_column="b")

        assert len(plan.chunks) > 1
        assert plan.chunks[0].payload["column"] == "b"
        assert (await read_all(source, "hinted", 100)).num_rows == 300
        await source.aclose()

    @pytest.mark.parametrize(
        ("ddl", "hint"),
        [
            ("CREATE TABLE h (a int, b int)", "b"),  # nullable: NULL rows would be skipped
            ("CREATE TABLE h (a int NOT NULL, b text NOT NULL)", "b"),  # not an integer
            ("CREATE TABLE h (a int NOT NULL)", "missing"),
        ],
    )
    async def test_unusable_split_hints_are_rejected(
        self, postgres_url: str, pg: PostgresClient, ddl: str, hint: str
    ) -> None:
        pg.execute(postgres_url, ddl)
        source = PostgresSource(postgres_url)

        with pytest.raises(ValueError, match="split_column"):
            await source.plan("h", target_chunk_bytes=100, split_column=hint)
        await source.aclose()


class TestSchemaAndTypes:
    async def test_native_types_are_attached_to_every_field(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.create_sample_table(postgres_url, "t", rows=10)
        source = PostgresSource(postgres_url)

        plan = await source.plan("t", target_chunk_bytes=MIB)

        hints = {f.name: native_type(f, "postgresql") for f in plan.schema}
        assert hints["amount"] == "numeric(12,2)"
        assert hints["id"] == "bigint"
        assert hints["seen"] == "timestamp with time zone"
        await source.aclose()

    async def test_nullability_follows_the_catalog(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.create_sample_table(postgres_url, "t", rows=1)
        source = PostgresSource(postgres_url)

        plan = await source.plan("t", target_chunk_bytes=MIB)

        assert plan.schema.field("id").nullable is False
        assert plan.schema.field("name").nullable is True
        await source.aclose()

    async def test_bounded_numerics_are_read_as_exact_decimals(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.create_sample_table(postgres_url, "t", rows=100)
        source = PostgresSource(postgres_url)

        table = await read_all(source, "t")

        assert table.schema.field("amount").type == pa.decimal128(12, 2)
        assert table.sort_by("id").equals(make_rows(1, 100))
        await source.aclose()

    @pytest.mark.parametrize(
        ("column_type", "native"),
        [
            ("numeric", "numeric"),
            ("numeric(40,2)", "numeric(40,2)"),
            ("money", "money"),
            ("xml", "xml"),
            ("int4range", "int4range"),
        ],
    )
    async def test_types_that_cannot_move_losslessly_fail_the_plan_loudly(
        self, postgres_url: str, pg: PostgresClient, column_type: str, native: str
    ) -> None:
        pg.execute(postgres_url, f"CREATE TABLE bad (id int PRIMARY KEY, price {column_type})")
        source = PostgresSource(postgres_url)

        with pytest.raises(UnsupportedType) as excinfo:
            await source.plan("bad", target_chunk_bytes=MIB)

        assert (excinfo.value.column, excinfo.value.native_type) == ("price", native)
        await source.aclose()


class TestNamesAndSafety:
    async def test_schema_qualified_tables_work(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.execute(postgres_url, "CREATE SCHEMA sales")
        pg.create_sample_table(postgres_url, "sales.orders", rows=50)
        source = PostgresSource(postgres_url)

        assert (await read_all(source, "sales.orders")).num_rows == 50
        await source.aclose()

    async def test_case_sensitive_names_are_matched_exactly(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.execute(postgres_url, 'CREATE TABLE "Orders" (id int PRIMARY KEY)')
        source = PostgresSource(postgres_url)

        assert (await source.plan("Orders", target_chunk_bytes=MIB)).table == "Orders"
        with pytest.raises(TableNotFound):
            await source.plan("orders", target_chunk_bytes=MIB)
        await source.aclose()

    async def test_hostile_column_names_are_quoted(
        self, postgres_url: str, pg: PostgresClient
    ) -> None:
        pg.execute(
            postgres_url,
            'CREATE TABLE odd ("select" int PRIMARY KEY, "we""ird" text, "Mixed Case" text)',
        )
        pg.execute(postgres_url, "INSERT INTO odd VALUES (1, 'a', 'b'), (2, NULL, 'c')")
        source = PostgresSource(postgres_url)

        table = await read_all(source, "odd")

        assert table.schema.names == ["select", 'we"ird', "Mixed Case"]
        assert table.num_rows == 2
        await source.aclose()

    @pytest.mark.parametrize("name", ["t; DROP TABLE victim", 't"', "a.b.c", "", "1t"])
    async def test_malicious_or_malformed_table_names_are_refused_before_any_sql(
        self, postgres_url: str, pg: PostgresClient, name: str
    ) -> None:
        pg.execute(postgres_url, "CREATE TABLE victim (id int)")
        source = PostgresSource(postgres_url)

        with pytest.raises(ValueError, match="table name"):
            await source.plan(name, target_chunk_bytes=MIB)

        assert pg.scalar(postgres_url, "SELECT to_regclass('victim') IS NOT NULL") is True
        await source.aclose()

    async def test_a_view_can_be_transferred(self, postgres_url: str, pg: PostgresClient) -> None:
        pg.create_sample_table(postgres_url, "base", rows=30)
        pg.execute(postgres_url, "CREATE VIEW v AS SELECT id, name FROM base WHERE id <= 20")
        source = PostgresSource(postgres_url)

        plan = await source.plan("v", target_chunk_bytes=MIB, split_column=None)

        assert (await read_all(source, "v")).num_rows == 20
        assert plan.schema.names == ["id", "name"]
        await source.aclose()


class TestConnection:
    async def test_a_connection_failure_is_a_connector_error_without_the_password(self) -> None:
        source = create_source("postgresql://alice:hunter2@127.0.0.1:1/nowhere")

        with pytest.raises(ConnectorError) as excinfo:
            await source.plan("t", target_chunk_bytes=MIB)

        assert "hunter2" not in str(excinfo.value)
        assert "alice:***@127.0.0.1:1" in str(excinfo.value)
        await source.aclose()

    @pytest.mark.parametrize("url", ["mysql://h/db", "postgresql://h/db?bitdrop_nope=1"])
    def test_bad_urls_are_rejected_when_the_source_is_created(self, url: str) -> None:
        with pytest.raises(ValueError, match=r"postgresql|bitdrop_nope"):
            create_source(url)

    async def test_the_pool_size_option_is_honoured(self, postgres_url: str) -> None:
        source = create_source(f"{postgres_url}?bitdrop_pool_size=2")

        assert isinstance(source, PostgresSource)
        assert source.pool_size == 2
        await source.aclose()
