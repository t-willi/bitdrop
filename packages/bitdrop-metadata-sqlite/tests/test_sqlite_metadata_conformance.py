"""The SQLite adapter must pass the shared MetadataStore conformance kit."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from bitdrop_api.metadata_store import MetadataStore
from bitdrop_api.testing import MetadataStoreConformance
from bitdrop_metadata_sqlite import SqliteMetadataStore


class TestSqliteFileConformance(MetadataStoreConformance):
    @pytest.fixture
    async def store(self, tmp_path: Path) -> AsyncIterator[MetadataStore]:
        store = SqliteMetadataStore(tmp_path / "metadata.db")
        yield store
        await store.aclose()


class TestSqliteInMemoryConformance(MetadataStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MetadataStore]:
        store = SqliteMetadataStore()
        yield store
        await store.aclose()
