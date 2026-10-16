"""The Redis-protocol chunk store must pass the shared ChunkStore kit on every server (Docker)."""

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.testing import MIB, ChunkStoreConformance
from bitdrop_store_redis import RedisChunkStore

if TYPE_CHECKING:
    from conftest import KvServer

pytestmark = pytest.mark.docker


class TestRedisStoreConformance(ChunkStoreConformance):
    @pytest.fixture
    async def store(self, kv: KvServer) -> AsyncIterator[ChunkStore]:
        store = RedisChunkStore(kv.url)
        yield store
        await store.aclose()

    @pytest.fixture
    async def capped_store(self, kv: KvServer) -> AsyncIterator[ChunkStore]:
        """The server itself is limited to 8 MB of memory: it refuses writes beyond that."""
        client = kv.client()
        original = client.config_get("maxmemory")["maxmemory"]
        client.config_set("maxmemory", "8mb")
        store = RedisChunkStore(kv.url)
        yield store
        await store.aclose()
        client.config_set("maxmemory", original)
        client.flushall()


class TestRedisStoreWithValueLimitConformance(ChunkStoreConformance):
    """Same kit with a 1 MiB item limit (like memcached's), so the limit checks actually run."""

    @pytest.fixture
    async def store(self, kv: KvServer) -> AsyncIterator[ChunkStore]:
        store = RedisChunkStore(f"{kv.url}?bitdrop_max_value_bytes={MIB}")
        yield store
        await store.aclose()
