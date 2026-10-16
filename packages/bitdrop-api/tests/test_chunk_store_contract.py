"""The ChunkStore contract: capability/stat value objects and the protocol itself."""

import dataclasses

import pytest

from bitdrop_api.chunk_store import ChunkStore, Locality, StoreCapabilities, StoreStats


class TestStoreCapabilities:
    def test_defaults_describe_the_most_conservative_store(self) -> None:
        capabilities = StoreCapabilities()

        assert capabilities.max_value_bytes is None
        assert capabilities.locality is Locality.CLUSTER
        assert capabilities.memory_stats is False

    @pytest.mark.parametrize("limit", [0, -1])
    def test_value_limit_must_be_positive(self, limit: int) -> None:
        with pytest.raises(ValueError, match="max_value_bytes"):
            StoreCapabilities(max_value_bytes=limit)

    def test_is_immutable(self) -> None:
        capabilities = StoreCapabilities()

        with pytest.raises(dataclasses.FrozenInstanceError):
            capabilities.memory_stats = True  # type: ignore[misc]


class TestStoreStats:
    def test_max_bytes_is_optional(self) -> None:
        assert StoreStats(used_bytes=10).max_bytes is None

    def test_used_bytes_cannot_be_negative(self) -> None:
        with pytest.raises(ValueError, match="used_bytes"):
            StoreStats(used_bytes=-1)

    @pytest.mark.parametrize("limit", [0, -5])
    def test_max_bytes_must_be_positive_when_given(self, limit: int) -> None:
        with pytest.raises(ValueError, match="max_bytes"):
            StoreStats(used_bytes=0, max_bytes=limit)


class TestLocality:
    def test_values_are_stable_strings_usable_in_config(self) -> None:
        assert [locality.value for locality in Locality] == ["pod", "node", "cluster"]


class TestChunkStoreProtocol:
    def test_object_with_all_methods_is_a_chunk_store(self) -> None:
        class Complete:
            async def put(self, key: str, value: bytes) -> None: ...
            async def get(self, key: str) -> bytes:
                return b""

            async def delete(self, key: str) -> None: ...
            def capabilities(self) -> StoreCapabilities:
                return StoreCapabilities()

            async def stats(self) -> StoreStats | None:
                return None

            async def aclose(self) -> None: ...

        assert isinstance(Complete(), ChunkStore)

    def test_object_missing_a_method_is_not_a_chunk_store(self) -> None:
        class MissingGet:
            async def put(self, key: str, value: bytes) -> None: ...
            async def delete(self, key: str) -> None: ...

        assert not isinstance(MissingGet(), ChunkStore)
