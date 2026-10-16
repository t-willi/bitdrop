"""The in-memory adapter must pass the shared ChunkStore conformance kit."""

from collections.abc import AsyncIterator

import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.testing import MIB, ChunkStoreConformance
from bitdrop_store_memory import MemoryChunkStore


class TestMemoryStoreConformance(ChunkStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[ChunkStore]:
        store = MemoryChunkStore()
        yield store
        await store.aclose()

    @pytest.fixture
    async def capped_store(self) -> AsyncIterator[ChunkStore]:
        store = MemoryChunkStore(max_bytes=4 * MIB)
        yield store
        await store.aclose()


class TestMemoryStoreWithValueLimitConformance(ChunkStoreConformance):
    """Same kit, configured so the 'declared value limit' checks actually run."""

    @pytest.fixture
    async def store(self) -> AsyncIterator[ChunkStore]:
        store = MemoryChunkStore(max_value_bytes=MIB)
        yield store
        await store.aclose()
