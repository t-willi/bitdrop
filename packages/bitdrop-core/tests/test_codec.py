"""The default chunk codec: Arrow IPC streams, optionally compressed."""

import pyarrow as pa
import pytest

from bitdrop_api.codec import ChunkCodec
from bitdrop_api.errors import CorruptChunk
from bitdrop_api.schema import native_type, with_native_type
from bitdrop_api.testing import SAMPLE_SCHEMA, make_rows
from bitdrop_core.codec import ArrowIpcCodec


@pytest.fixture(params=["none", "lz4", "zstd"])
def codec(request: pytest.FixtureRequest) -> ArrowIpcCodec:
    return ArrowIpcCodec(compression=request.param)


class TestRoundTrip:
    def test_is_a_chunk_codec(self, codec: ArrowIpcCodec) -> None:
        assert isinstance(codec, ChunkCodec)

    def test_data_survives_exactly(self, codec: ArrowIpcCodec) -> None:
        table = make_rows(1, 2000)

        assert codec.decode(codec.encode(table)).equals(table)

    def test_an_empty_table_survives(self, codec: ArrowIpcCodec) -> None:
        table = SAMPLE_SCHEMA.empty_table()

        restored = codec.decode(codec.encode(table))

        assert restored.num_rows == 0
        assert restored.schema.equals(SAMPLE_SCHEMA)

    def test_native_type_hints_survive(self, codec: ArrowIpcCodec) -> None:
        """The destination needs them to recreate columns exactly."""
        schema = pa.schema(
            [
                with_native_type(
                    pa.field("amount", pa.decimal128(12, 2)), "postgresql", "numeric(12,2)"
                )
            ]
        )
        table = pa.table({"amount": pa.array([], pa.decimal128(12, 2))}, schema=schema)

        restored = codec.decode(codec.encode(table))

        assert native_type(restored.schema.field("amount"), "postgresql") == "numeric(12,2)"

    def test_nulls_and_extension_free_types_survive(self, codec: ArrowIpcCodec) -> None:
        table = pa.table(
            {"a": pa.array([1, None, 3], pa.int32()), "b": pa.array([None, "x", None], pa.string())}
        )

        assert codec.decode(codec.encode(table)).equals(table)

    def test_a_table_of_several_batches_survives(self, codec: ArrowIpcCodec) -> None:
        table = pa.concat_tables([make_rows(1, 100), make_rows(101, 100)])

        assert codec.decode(codec.encode(table)).equals(table)


class TestCompression:
    def test_compression_shrinks_repetitive_data(self) -> None:
        table = pa.table({"x": ["same value every time"] * 10_000})

        plain = ArrowIpcCodec(compression="none").encode(table)

        assert len(ArrowIpcCodec(compression="lz4").encode(table)) < len(plain) / 4
        assert len(ArrowIpcCodec(compression="zstd").encode(table)) < len(plain) / 4

    def test_any_codec_can_read_any_other_codecs_output(self) -> None:
        """Compression is recorded in the stream, so readers need no configuration."""
        table = make_rows(1, 500)
        compressed = ArrowIpcCodec(compression="zstd").encode(table)

        assert ArrowIpcCodec(compression="none").decode(compressed).equals(table)

    def test_unknown_compression_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="compression"):
            ArrowIpcCodec(compression="gzip")  # type: ignore[arg-type]


class TestCorruption:
    @pytest.mark.parametrize("garbage", [b"", b"not arrow at all", b"\x00" * 64])
    def test_garbage_raises_corrupt_chunk(self, codec: ArrowIpcCodec, garbage: bytes) -> None:
        with pytest.raises(CorruptChunk):
            codec.decode(garbage)

    def test_a_truncated_chunk_raises_corrupt_chunk(self, codec: ArrowIpcCodec) -> None:
        encoded = codec.encode(make_rows(1, 1000))

        with pytest.raises(CorruptChunk):
            codec.decode(encoded[: len(encoded) // 2])
