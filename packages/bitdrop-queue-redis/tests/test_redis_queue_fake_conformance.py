"""The Redis Streams queue passes the shared kit against an in-process fake server (no Docker).

This proves the adapter's logic. It does not prove that Redis, Valkey, and Dragonfly agree with
the fake: ``test_redis_queue_conformance.py`` runs the same kit on the real servers (Docker).
"""

from collections.abc import AsyncIterator

import fakeredis
import pytest
from fakeredis import aioredis as fake_aioredis

from bitdrop_api.chunk_queue import ChunkQueue
from bitdrop_api.testing import ChunkQueueConformance
from bitdrop_queue_redis import RedisChunkQueue


class TestRedisChunkQueueAgainstAFakeServer(ChunkQueueConformance):
    @pytest.fixture
    async def queue(self) -> AsyncIterator[ChunkQueue]:
        client = fake_aioredis.FakeRedis(server=fakeredis.FakeServer())
        queue = RedisChunkQueue("redis://fake/0", client=client)
        yield queue
        await queue.aclose()

    # fakeredis answers a blocking XREADGROUP at once instead of waiting (plain XREAD does wait),
    # so the two kit tests that depend on waiting run only against the real servers.

    @pytest.mark.skip(reason="fakeredis does not block XREADGROUP; see the Docker run")
    async def test_receiving_can_wait_for_a_message_that_arrives(self, queue: ChunkQueue) -> None:
        """Skipped on the fake server."""

    @pytest.mark.skip(reason="fakeredis does not block XREADGROUP; see the Docker run")
    async def test_waiting_for_nothing_gives_up_after_the_time(self, queue: ChunkQueue) -> None:
        """Skipped on the fake server."""
