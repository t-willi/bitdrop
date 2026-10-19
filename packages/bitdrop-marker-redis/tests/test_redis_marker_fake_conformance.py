"""The Redis marker store passes the shared kit against an in-process fake server (no Docker).

This proves the adapter's logic. ``test_redis_marker_conformance.py`` runs the same kit on the real
servers (Docker).
"""

from collections.abc import AsyncIterator

import fakeredis
import pytest
from fakeredis import aioredis as fake_aioredis

from bitdrop_api.marker_store import MarkerStore
from bitdrop_api.testing import MarkerStoreConformance
from bitdrop_marker_redis import RedisMarkerStore


class TestRedisMarkerStoreAgainstAFakeServer(MarkerStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MarkerStore]:
        client = fake_aioredis.FakeRedis(server=fakeredis.FakeServer())
        store = RedisMarkerStore("redis://fake/0", client=client)
        yield store
        await store.aclose()
