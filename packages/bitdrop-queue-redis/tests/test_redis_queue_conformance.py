"""The Redis Streams queue must pass the shared ChunkQueue kit on every server (Docker)."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest

from bitdrop_api.chunk_queue import ChunkQueue
from bitdrop_api.testing import ChunkQueueConformance
from bitdrop_queue_redis import RedisChunkQueue

if TYPE_CHECKING:
    from conftest import KvServer

pytestmark = pytest.mark.docker


class TestRedisChunkQueueConformance(ChunkQueueConformance):
    @pytest.fixture
    async def queue(self, kv: KvServer) -> AsyncIterator[ChunkQueue]:
        """A queue with a name of its own, so tests on one server cannot see each other's."""
        queue = RedisChunkQueue(f"{kv.url}?bitdrop_queue=t{uuid.uuid4().hex}")
        yield queue
        await queue.aclose()
