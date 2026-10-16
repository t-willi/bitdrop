"""A source plan can be written to bytes and read back unchanged, so another process can use it."""

import datetime
import json
from typing import Any

import pyarrow as pa
import pytest

from bitdrop_api.errors import CorruptPlan
from bitdrop_api.plan import decode_plan, encode_plan
from bitdrop_api.schema import native_type, with_native_type
from bitdrop_api.source import ChunkSpec, SourcePlan


def a_schema() -> pa.Schema:
    """A schema with the awkward parts: native-type hints, nested types, schema-level metadata."""
    return pa.schema(
        [
            with_native_type(pa.field("id", pa.int64(), nullable=False), "postgresql", "bigint"),
            with_native_type(
                pa.field("price", pa.decimal128(12, 2)), "postgresql", "numeric(12,2)"
            ),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("point", pa.struct([("x", pa.float64()), ("y", pa.float64())])),
            pa.field("seen", pa.timestamp("us", tz="UTC")),
            pa.field("blob", pa.binary()),
        ],
        metadata={"origin": "unit test"},
    )


def a_plan() -> SourcePlan:
    return SourcePlan(
        table="public.orders",
        schema=a_schema(),
        chunks=(
            ChunkSpec(
                "c-0",
                "id in [1, 5)",
                {"column": "id", "low": 1, "high": 5, "nested": {"keys": ["a", "b"], "flag": True}},
                estimated_rows=4,
            ),
            ChunkSpec("c-1", "id in [5, 9)", {"column": "id", "low": 5, "high": None}),
            ChunkSpec("c-2", "the rest", {}),
        ),
        estimated_rows=8,
        estimated_row_bytes=120,
        notes=("no integer primary key", "second note"),
    )


def same_plan(left: SourcePlan, right: SourcePlan) -> None:
    """Assert two plans are identical, including the schema metadata that ``==`` ignores."""
    assert left.table == right.table
    assert left.schema.equals(right.schema, check_metadata=True)
    assert left.chunks == right.chunks
    assert left.estimated_rows == right.estimated_rows
    assert left.estimated_row_bytes == right.estimated_row_bytes
    assert left.notes == right.notes


class TestRoundTrip:
    def test_a_plan_comes_back_unchanged(self) -> None:
        plan = a_plan()

        same_plan(decode_plan(encode_plan(plan)), plan)

    def test_the_native_type_hints_survive(self) -> None:
        restored = decode_plan(encode_plan(a_plan()))

        assert native_type(restored.schema.field("price"), "postgresql") == "numeric(12,2)"

    def test_nullability_and_schema_metadata_survive(self) -> None:
        restored = decode_plan(encode_plan(a_plan()))

        assert restored.schema.field("id").nullable is False
        assert restored.schema.metadata == {b"origin": b"unit test"}

    def test_the_order_of_the_chunks_is_kept(self) -> None:
        restored = decode_plan(encode_plan(a_plan()))

        assert [chunk.chunk_id for chunk in restored.chunks] == ["c-0", "c-1", "c-2"]

    def test_unknown_estimates_stay_unknown(self) -> None:
        plan = SourcePlan("t", pa.schema([pa.field("x", pa.int64())]), ())

        restored = decode_plan(encode_plan(plan))

        assert (restored.estimated_rows, restored.estimated_row_bytes) == (None, None)
        assert restored.chunks == ()
        assert restored.notes == ()

    def test_the_bytes_are_json(self) -> None:
        """Anyone can read a saved plan, not only this library."""
        document = json.loads(encode_plan(a_plan()))

        assert document["table"] == "public.orders"
        assert [chunk["chunk_id"] for chunk in document["chunks"]] == ["c-0", "c-1", "c-2"]


class TestEncodingRefusesWhatItCannotStoreExactly:
    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param({"when": datetime.date(2026, 10, 16)}, id="a date"),
            pytest.param({"range": (1, 5)}, id="a tuple, which JSON would turn into a list"),
            pytest.param({1: "one"}, id="a key that is not a string"),
            pytest.param({"x": float("nan")}, id="not a number"),
            pytest.param({"ids": {1, 2}}, id="a set"),
        ],
    )
    def test_a_payload_that_json_cannot_keep_exactly_is_refused(
        self, payload: dict[Any, Any]
    ) -> None:
        plan = SourcePlan("t", a_schema(), (ChunkSpec("c-0", "one", payload),))

        with pytest.raises(ValueError, match="c-0"):
            encode_plan(plan)


class TestDecodingRefusesGarbage:
    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(b"", id="empty"),
            pytest.param(b"not json at all", id="not json"),
            pytest.param(b"\xff\xfe\x00", id="not text"),
            pytest.param(b"[1, 2, 3]", id="a list instead of an object"),
            pytest.param(b"{}", id="an object without the plan in it"),
        ],
    )
    def test_bytes_that_are_not_a_plan_are_corrupt(self, data: bytes) -> None:
        with pytest.raises(CorruptPlan):
            decode_plan(data)

    def test_a_plan_from_the_future_is_refused_by_name(self) -> None:
        document = json.loads(encode_plan(a_plan()))
        document["format"] = 999

        with pytest.raises(CorruptPlan, match="999"):
            decode_plan(json.dumps(document).encode())

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            pytest.param("table", 7, id="table is not text"),
            pytest.param("schema", "%%% not base64 %%%", id="schema is not base64"),
            pytest.param("schema", "AAAA", id="schema is not an Arrow schema"),
            pytest.param("chunks", "c-0", id="chunks is not a list"),
            pytest.param("chunks", [{"chunk_id": "c-0"}], id="a chunk without its fields"),
            pytest.param(
                "chunks", [{"chunk_id": "a/b", "description": "", "payload": {}}], id="bad id"
            ),
            pytest.param("notes", "just text", id="notes is not a list"),
            pytest.param("estimated_rows", "many", id="estimate is not a number"),
            pytest.param("estimated_rows", True, id="estimate is a boolean"),
            pytest.param(
                "chunks",
                [{"chunk_id": "c-0", "description": "", "payload": [1, 2]}],
                id="a payload that is not an object",
            ),
        ],
    )
    def test_a_field_of_the_wrong_kind_is_corrupt(self, field: str, value: Any) -> None:
        document = json.loads(encode_plan(a_plan()))
        document[field] = value

        with pytest.raises(CorruptPlan):
            decode_plan(json.dumps(document).encode())

    def test_a_chunk_id_that_appears_twice_is_corrupt(self) -> None:
        document = json.loads(encode_plan(a_plan()))
        document["chunks"].append(document["chunks"][0])

        with pytest.raises(CorruptPlan, match="c-0"):
            decode_plan(json.dumps(document).encode())

    def test_the_error_never_repeats_the_input(self) -> None:
        secret = "hunter2-hunter2"

        with pytest.raises(CorruptPlan) as caught:
            decode_plan(f'{{"format": 1, "table": "{secret}", "chunks": 5}}'.encode())

        assert secret not in str(caught.value)
