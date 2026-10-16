"""Native type hints travel with the data as Arrow field metadata."""

import pyarrow as pa

from bitdrop_api.schema import native_type, with_native_type


class TestNativeTypeMetadata:
    def test_roundtrip(self) -> None:
        field = with_native_type(
            pa.field("amount", pa.decimal128(12, 2)), "postgresql", "numeric(12,2)"
        )

        assert native_type(field, "postgresql") == "numeric(12,2)"

    def test_the_hint_is_only_valid_for_the_engine_that_wrote_it(self) -> None:
        field = with_native_type(
            pa.field("amount", pa.decimal128(12, 2)), "postgresql", "numeric(12,2)"
        )

        assert native_type(field, "mysql") is None

    def test_fields_without_a_hint_have_none(self) -> None:
        assert native_type(pa.field("x", pa.int64()), "postgresql") is None

    def test_existing_metadata_is_kept(self) -> None:
        field = pa.field("x", pa.int64(), metadata={"comment": "keep me"})

        hinted = with_native_type(field, "postgresql", "bigint")

        assert hinted.metadata is not None
        assert hinted.metadata[b"comment"] == b"keep me"

    def test_name_nullability_and_type_are_untouched(self) -> None:
        field = pa.field("x", pa.int32(), nullable=False)

        hinted = with_native_type(field, "postgresql", "integer")

        assert (hinted.name, hinted.type, hinted.nullable) == ("x", pa.int32(), False)

    def test_the_hint_survives_arrow_ipc(self) -> None:
        schema = pa.schema([with_native_type(pa.field("j", pa.string()), "postgresql", "jsonb")])
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_table(pa.table({"j": ["{}"]}, schema=schema))

        restored = pa.ipc.open_stream(sink.getvalue()).read_all().schema

        assert native_type(restored.field("j"), "postgresql") == "jsonb"
