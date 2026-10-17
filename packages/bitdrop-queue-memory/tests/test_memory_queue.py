"""Behaviour specific to the in-memory queue (the contract is covered by the kit)."""

import asyncio

import pytest

from bitdrop_queue_memory import MemoryChunkQueue, create_queue


class TestTheClock:
    async def test_idle_time_follows_the_injected_clock(self) -> None:
        now = [100.0]
        queue = MemoryChunkQueue(clock=lambda: now[0])
        await queue.publish(b"one")
        await queue.receive("consumer-1")

        now[0] += 9.9
        assert await queue.reclaim("consumer-2", idle_seconds=10.0) == []
        now[0] += 0.1
        assert len(await queue.reclaim("consumer-2", idle_seconds=10.0)) == 1


class TestWaiting:
    async def test_a_cancelled_wait_leaves_nothing_behind(self) -> None:
        queue = MemoryChunkQueue()
        waiting = asyncio.create_task(queue.receive("consumer-1", block_seconds=30.0))
        await asyncio.sleep(0.05)

        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        await queue.publish(b"one")
        assert len(await queue.receive("consumer-2")) == 1

    async def test_finished_waits_are_forgotten(self) -> None:
        queue = MemoryChunkQueue()
        await queue.receive("consumer-1", block_seconds=0.05)  # times out
        waiting = asyncio.create_task(queue.receive("consumer-1", block_seconds=5.0))
        await asyncio.sleep(0.02)
        await queue.publish(b"one")  # wakes it
        await waiting

        assert queue._sleepers == []

    async def test_every_waiting_consumer_wakes_but_only_one_gets_the_message(self) -> None:
        queue = MemoryChunkQueue()
        waiters = [
            asyncio.create_task(queue.receive(f"consumer-{i}", block_seconds=0.5)) for i in range(3)
        ]
        await asyncio.sleep(0.05)

        await queue.publish(b"one")
        results = await asyncio.gather(*waiters)

        assert sorted(len(delivered) for delivered in results) == [0, 0, 1]

    async def test_bad_arguments_are_refused(self) -> None:
        queue = MemoryChunkQueue()

        with pytest.raises(ValueError, match="max_messages"):
            await queue.receive("consumer-1", max_messages=0)
        with pytest.raises(ValueError, match="block_seconds"):
            await queue.receive("consumer-1", block_seconds=-1)


class TestTheUrl:
    def test_memory_gives_an_empty_queue(self) -> None:
        assert isinstance(create_queue("memory://"), MemoryChunkQueue)

    @pytest.mark.parametrize("url", ["memory:///somewhere", "memory://?x=1", "file://"])
    def test_anything_else_is_refused(self, url: str) -> None:
        with pytest.raises(ValueError, match="memory"):
            create_queue(url)
