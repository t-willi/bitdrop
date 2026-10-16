"""Deterministic sample tables for conformance tests and examples."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa

_FIELDS: list[pa.Field[Any]] = [
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string()),
    pa.field("score", pa.float64()),
    pa.field("amount", pa.decimal128(12, 2)),
    pa.field("active", pa.bool_()),
    pa.field("born", pa.date32()),
    pa.field("seen", pa.timestamp("us", tz="UTC")),
    pa.field("raw", pa.binary()),
]
SAMPLE_SCHEMA = pa.schema(_FIELDS)
"""A schema with the types every connector must handle, nullable columns included."""


def make_rows(start: int, count: int) -> pa.Table:
    """Return ``count`` rows with ids ``start .. start + count - 1`` in :data:`SAMPLE_SCHEMA`.

    Values derive from the id, so any slice of the data can be regenerated and compared. Some
    values are ``NULL`` (``name`` where ``id % 7 == 0``, ``active`` where ``id % 11 == 0``).
    """
    ids = range(start, start + count)
    epoch = datetime(2024, 1, 1, tzinfo=UTC)
    return pa.table(
        {
            "id": list(ids),
            "name": [None if i % 7 == 0 else f"name-{i}" for i in ids],
            "score": [i / 3.0 for i in ids],
            "amount": [Decimal(f"{i % 10000}.{i % 100:02d}") for i in ids],
            "active": [None if i % 11 == 0 else i % 2 == 0 for i in ids],
            "born": [date(2000, 1, 1) + timedelta(days=i % 5000) for i in ids],
            "seen": [epoch + timedelta(seconds=i) for i in ids],
            "raw": [i.to_bytes(8, "big") for i in ids],
        },
        schema=SAMPLE_SCHEMA,
    )
