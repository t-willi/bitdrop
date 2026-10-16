"""The filesystem adapter must pass the shared ChunkStore conformance kit."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.testing import MIB, ChunkStoreConformance
from bitdrop_store_file import FileChunkStore


class TestFileStoreConformance(ChunkStoreConformance):
    @pytest.fixture
    async def store(self, tmp_path: Path) -> AsyncIterator[ChunkStore]:
        store = FileChunkStore(tmp_path / "chunks")
        yield store
        await store.aclose()


class TestFileStoreWithValueLimitConformance(ChunkStoreConformance):
    """Same kit, configured so the 'declared value limit' checks actually run."""

    @pytest.fixture
    async def store(self, tmp_path: Path) -> AsyncIterator[ChunkStore]:
        store = FileChunkStore(tmp_path / "chunks", max_value_bytes=MIB)
        yield store
        await store.aclose()
