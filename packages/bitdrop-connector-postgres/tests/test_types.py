"""Which Postgres types move losslessly, and how Arrow types map back to Postgres DDL."""

import pyarrow as pa
import pytest

from bitdrop_api.errors import UnsupportedType
from bitdrop_connector_postgres._types import arrow_to_postgres, check_supported


class TestCheckSupported:
    @pytest.mark.parametrize(
        "native",
        [
            "smallint",
            "integer",
            "bigint",
            "real",
            "double precision",
            "boolean",
            "text",
            "character varying",
            "character varying(10)",
            "character(3)",
            "date",
            "timestamp without time zone",
            "timestamp(3) without time zone",
            "timestamp with time zone",
            "time without time zone",
            "bytea",
            "uuid",
            "json",
            "jsonb",
            "inet",
            "interval",
            "text[]",
            "integer[]",
            "bigint[]",
            "character varying(10)[]",
        ],
    )
    def test_supported_types_pass_through_unchanged(self, native: str) -> None:
        result = check_supported("t", "c", native)

        assert result.native == native
        assert result.arrow_cast is None

    @pytest.mark.parametrize(
        ("native", "expected"),
        [
            ("numeric(12,2)", pa.decimal128(12, 2)),
            ("numeric(38,10)", pa.decimal128(38, 10)),
            ("numeric(10,0)", pa.decimal128(10, 0)),
            ("numeric(5,5)", pa.decimal128(5, 5)),
        ],
    )
    def test_bounded_numerics_become_decimals(self, native: str, expected: pa.DataType) -> None:
        assert check_supported("t", "c", native).arrow_cast == expected

    @pytest.mark.parametrize(
        "native",
        [
            "numeric",  # unconstrained: no lossless Arrow type
            "numeric(39,2)",  # wider than decimal128
            "money",
            "bit(3)",
            "xml",
            "tsvector",
            "int4range",
            "point",
            "a_user_defined_enum",
            "numeric(12,2)[]",
            "timestamp with time zone[]",
        ],
    )
    def test_everything_else_is_refused_loudly_before_any_data_moves(self, native: str) -> None:
        with pytest.raises(UnsupportedType) as excinfo:
            check_supported("orders", "price", native)

        assert (excinfo.value.table, excinfo.value.column, excinfo.value.native_type) == (
            "orders",
            "price",
            native,
        )

    def test_the_error_suggests_what_to_do(self) -> None:
        with pytest.raises(UnsupportedType, match="Cast the column in a view"):
            check_supported("orders", "price", "money")


class TestArrowToPostgres:
    @pytest.mark.parametrize(
        ("arrow_type", "expected"),
        [
            (pa.int8(), "smallint"),
            (pa.int16(), "smallint"),
            (pa.int32(), "integer"),
            (pa.int64(), "bigint"),
            (pa.float32(), "real"),
            (pa.float64(), "double precision"),
            (pa.bool_(), "boolean"),
            (pa.string(), "text"),
            (pa.large_string(), "text"),
            (pa.binary(), "bytea"),
            (pa.large_binary(), "bytea"),
            (pa.date32(), "date"),
            (pa.timestamp("us"), "timestamp without time zone"),
            (pa.timestamp("us", tz="UTC"), "timestamp with time zone"),
            (pa.time64("us"), "time without time zone"),
            (pa.decimal128(12, 2), "numeric(12,2)"),
        ],
    )
    def test_common_types_map_to_their_natural_column_type(
        self, arrow_type: pa.DataType, expected: str
    ) -> None:
        assert arrow_to_postgres("t", "c", arrow_type) == expected

    @pytest.mark.parametrize(
        "arrow_type",
        [
            pa.list_(pa.int32()),
            pa.struct([pa.field("a", pa.int32())]),
            pa.duration("us"),
            pa.null(),
            pa.timestamp("ns"),  # Postgres keeps microseconds: nanoseconds would be cut silently
            pa.uint64(),
        ],
    )
    def test_types_without_a_faithful_mapping_are_refused(self, arrow_type: pa.DataType) -> None:
        with pytest.raises(UnsupportedType):
            arrow_to_postgres("t", "c", arrow_type)
