"""The Source, Destination, and ChunkCodec contracts: value objects and protocols."""

import dataclasses

import pyarrow as pa
import pytest

from bitdrop_api.codec import ChunkCodec
from bitdrop_api.destination import ChunkWrite, Destination
from bitdrop_api.errors import (
    BitdropError,
    ConnectorError,
    InvalidKey,
    SchemaMismatch,
    TableNotFound,
    TableNotPrepared,
    UnsupportedType,
)
from bitdrop_api.source import ChunkSpec, Source, SourcePlan


class TestChunkSpec:
    def test_the_id_follows_the_shared_key_rule(self) -> None:
        with pytest.raises(InvalidKey):
            ChunkSpec(chunk_id="has/slash", description="x", payload={})

    def test_is_immutable(self) -> None:
        spec = ChunkSpec(chunk_id="c000001", description="ids 1..10", payload={"lo": 1, "hi": 11})

        with pytest.raises(dataclasses.FrozenInstanceError):
            spec.chunk_id = "other"  # type: ignore[misc]

    def test_estimated_rows_are_optional(self) -> None:
        spec = ChunkSpec(chunk_id="c1", description="all", payload={})

        assert spec.estimated_rows is None


class TestSourcePlan:
    def test_chunk_ids_must_be_unique(self) -> None:
        chunks = (
            ChunkSpec(chunk_id="c1", description="a", payload={}),
            ChunkSpec(chunk_id="c1", description="b", payload={}),
        )

        with pytest.raises(ValueError, match="duplicate chunk id"):
            SourcePlan(table="t", schema=pa.schema([]), chunks=chunks)

    def test_an_empty_plan_is_valid(self) -> None:
        plan = SourcePlan(table="t", schema=pa.schema([pa.field("id", pa.int64())]), chunks=())

        assert plan.chunks == ()
        assert plan.estimated_rows is None


class TestChunkWrite:
    def test_describes_whether_the_chunk_was_new(self) -> None:
        assert ChunkWrite(rows=10, already_loaded=False).rows == 10
        assert ChunkWrite(rows=0, already_loaded=True).already_loaded is True


class TestErrors:
    @pytest.mark.parametrize("error_type", [TableNotFound, UnsupportedType, SchemaMismatch])
    def test_connector_errors_share_a_base(self, error_type: type[BitdropError]) -> None:
        assert issubclass(error_type, ConnectorError)
        assert issubclass(error_type, BitdropError)

    def test_table_not_prepared_is_a_connector_error(self) -> None:
        assert issubclass(TableNotPrepared, ConnectorError)

    def test_unsupported_type_names_the_column_and_the_type(self) -> None:
        error = UnsupportedType("orders", "price", "money", "cast it to numeric(12,2)")

        assert "price" in str(error)
        assert "money" in str(error)
        assert "cast it to numeric(12,2)" in str(error)
        assert (error.table, error.column, error.native_type) == ("orders", "price", "money")

    def test_table_not_found_names_the_table(self) -> None:
        assert "orders" in str(TableNotFound("orders"))


class TestProtocols:
    def test_complete_source_is_recognised(self) -> None:
        class Complete:
            async def plan(self, *args: object, **kwargs: object) -> SourcePlan:
                raise NotImplementedError

            async def read_chunk(self, *args: object, **kwargs: object) -> pa.Table:
                raise NotImplementedError

            async def aclose(self) -> None: ...

        assert isinstance(Complete(), Source)
        assert not isinstance(object(), Source)

    def test_complete_destination_is_recognised(self) -> None:
        class Complete:
            async def prepare(self, *args: object, **kwargs: object) -> None: ...

            async def write_chunk(self, *args: object, **kwargs: object) -> ChunkWrite:
                raise NotImplementedError

            async def count_rows(self, *args: object, **kwargs: object) -> int:
                return 0

            async def aclose(self) -> None: ...

        assert isinstance(Complete(), Destination)
        assert not isinstance(object(), Destination)

    def test_complete_codec_is_recognised(self) -> None:
        class Complete:
            def encode(self, table: pa.Table) -> bytes:
                return b""

            def decode(self, data: bytes) -> pa.Table:
                raise NotImplementedError

        assert isinstance(Complete(), ChunkCodec)
        assert not isinstance(object(), ChunkCodec)
