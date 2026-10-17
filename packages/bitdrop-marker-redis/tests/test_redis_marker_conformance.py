"""The Redis marker store must pass the shared MarkerStore kit on every server (Docker)."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest

from bitdrop_api.marker_store import MarkerStore
from bitdrop_api.testing import MarkerStoreConformance
from bitdrop_marker_redis import RedisMarkerStore

if TYPE_CHECKING:
    from conftest import KvServer

pytestmark = pytest.mark.docker


class TestRedisMarkerStoreConformance(MarkerStoreConformance):
    @pytest.fixture
    async def store(self, kv: KvServer) -> AsyncIterator[MarkerStore]:
        """A store with a prefix of its own, so tests on one server cannot see each other's."""
        store = RedisMarkerStore(f"{kv.url}?bitdrop_prefix=t{uuid.uuid4().hex}:")
        yield store
        await store.aclose()
