"""Which Postgres types move losslessly, and how Arrow types map back to Postgres DDL.

The rule is: **never degrade silently**. A column whose type the connector cannot carry exactly
makes planning fail with ``UnsupportedType`` before any data moves. Every type listed as supported
here is covered by the round-trip test in ``tests/test_types_roundtrip.py``.
"""

import re
from dataclasses import dataclass

import pyarrow as pa

from bitdrop_api.errors import UnsupportedType

SCALAR_TYPES = frozenset(
    {
        "smallint",
        "integer",
        "bigint",
        "real",
        "double precision",
        "boolean",
        "text",
        "character varying",
        "character",
        "date",
        "timestamp without time zone",
        "timestamp with time zone",
        "time without time zone",
        "bytea",
        "uuid",
        "json",
        "jsonb",
        "inet",
        "interval",
    }
)
"""Base types (length and precision modifiers removed) that pass through unchanged."""

ARRAY_ELEMENT_TYPES = frozenset({"text", "character varying", "integer", "bigint"})
"""Element types of the arrays that pass through unchanged."""

MAX_DECIMAL_PRECISION = 38
"""Arrow's decimal128 holds at most 38 digits."""

_MODIFIERS = re.compile(r"\(\d+(?:,\d+)?\)")
_NUMERIC = re.compile(r"numeric\((\d+),(\d+)\)")

_HINT = (
    "Cast the column in a view (for example col::text AS col) and transfer the view. "
    "Supported types: integers, real/double precision, boolean, text/varchar/char, date, "
    "time/timestamp, bytea, uuid, json/jsonb, inet, interval, numeric(p,s) with p <= 38, "
    "and arrays of text/varchar/integer/bigint."
)


@dataclass(frozen=True, slots=True)
class ColumnType:
    """A supported column type and what the connector must do with it after reading."""

    native: str
    """The type as ``format_type`` prints it, e.g. ``numeric(12,2)``."""

    arrow_cast: pa.DataType | None
    """If set, the value read from the driver must be cast to this Arrow type."""


def check_supported(table: str, column: str, native: str) -> ColumnType:
    """Return how to handle a column of type ``native``.

    Raises:
        UnsupportedType: the type cannot be moved losslessly.
    """
    if match := _NUMERIC.fullmatch(native):
        precision, scale = int(match[1]), int(match[2])
        if 0 < precision <= MAX_DECIMAL_PRECISION and scale <= precision:
            return ColumnType(native, pa.decimal128(precision, scale))
        raise UnsupportedType(table, column, native, _HINT)

    if native.endswith("[]"):
        element = _MODIFIERS.sub("", native.removesuffix("[]")).strip()
        if element in ARRAY_ELEMENT_TYPES:
            return ColumnType(native, None)
        raise UnsupportedType(table, column, native, _HINT)

    if _MODIFIERS.sub("", native).strip() in SCALAR_TYPES:
        return ColumnType(native, None)
    raise UnsupportedType(table, column, native, _HINT)


def arrow_to_postgres(table: str, column: str, data_type: pa.DataType) -> str:
    """Return the Postgres column type for an Arrow type, used when the source gave no native hint.

    Raises:
        UnsupportedType: the Arrow type has no faithful Postgres equivalent.
    """
    types = pa.types
    if types.is_int8(data_type) or types.is_int16(data_type):
        return "smallint"
    if types.is_int32(data_type):
        return "integer"
    if types.is_int64(data_type):
        return "bigint"
    if types.is_float32(data_type):
        return "real"
    if types.is_float64(data_type):
        return "double precision"
    if types.is_boolean(data_type):
        return "boolean"
    if types.is_string(data_type) or types.is_large_string(data_type):
        return "text"
    if types.is_binary(data_type) or types.is_large_binary(data_type):
        return "bytea"
    if types.is_date32(data_type):
        return "date"
    if types.is_timestamp(data_type) and data_type.unit != "ns":
        return "timestamp with time zone" if data_type.tz else "timestamp without time zone"
    if types.is_time64(data_type) and data_type.unit == "us":
        return "time without time zone"
    if types.is_decimal128(data_type):
        return f"numeric({data_type.precision},{data_type.scale})"
    raise UnsupportedType(table, column, str(data_type), "No Postgres column type represents it.")
