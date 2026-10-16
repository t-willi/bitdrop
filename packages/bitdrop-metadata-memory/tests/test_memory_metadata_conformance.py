"""The in-memory adapter must pass the shared MetadataStore conformance kit."""

from collections.abc import AsyncIterator

import pytest

from bitdrop_api.metadata_store import MetadataStore
from bitdrop_api.testing import MetadataStoreConformance
from bitdrop_metadata_memory import MemoryMetadataStore


class TestMemoryMetadataStoreConformance(MetadataStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MetadataStore]:
        store = MemoryMetadataStore()
        yield store
        await store.aclose()
