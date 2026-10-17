"""The in-memory adapter must pass the shared ChunkQueue conformance kit."""

from collections.abc import AsyncIterator

import pytest

from bitdrop_api.chunk_queue import ChunkQueue
from bitdrop_api.testing import ChunkQueueConformance
from bitdrop_queue_memory import MemoryChunkQueue


class TestMemoryChunkQueueConformance(ChunkQueueConformance):
    @pytest.fixture
    async def queue(self) -> AsyncIterator[ChunkQueue]:
        queue = MemoryChunkQueue()
        yield queue
        await queue.aclose()
