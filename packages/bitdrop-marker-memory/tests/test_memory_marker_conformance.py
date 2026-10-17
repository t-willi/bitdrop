"""The in-memory adapter must pass the shared MarkerStore conformance kit."""

from collections.abc import AsyncIterator

import pytest

from bitdrop_api.marker_store import MarkerStore
from bitdrop_api.testing import MarkerStoreConformance
from bitdrop_marker_memory import MemoryMarkerStore


class TestMemoryMarkerStoreConformance(MarkerStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MarkerStore]:
        store = MemoryMarkerStore()
        yield store
        await store.aclose()
