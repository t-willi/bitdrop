"""The consumer loop shared by source and destination containers: retry, give up, hand back, hold.

Real in-memory queues are used; the handlers are small functions that fail on purpose.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable

import pytest

from bitdrop_core.queueflow import ConsumerConfig, ConsumerStats, Defer, GiveUp, Handler, consume
from bitdrop_queue_memory import MemoryChunkQueue

TIMEOUT = 10.0


def config(**overrides: object) -> ConsumerConfig:
    values: dict[str, object] = {
        "max_attempts": 3,
        "poll_interval": 0.01,
        "idle_seconds": 5.0,
        "retry_delay": 0.001,
        "defer_delay": 0.01,
    }
    return ConsumerConfig(**{**values, **overrides})  # type: ignore[arg-type]


async def until(condition: Callable[[], Awaitable[bool]], seconds: float = TIMEOUT) -> None:
    """Wait for ``condition``; fail the test instead of hanging it."""
    deadline = time.monotonic() + seconds
    while not await condition():
        if time.monotonic() > deadline:
            pytest.fail("the condition was not reached in time")
        await asyncio.sleep(0.005)


class Run:
    """A consumer running in the background until stopped."""

    def __init__(
        self,
        queue: MemoryChunkQueue,
        handler: Handler,
        cfg: ConsumerConfig,
        name: str = "c-1",
    ) -> None:
        self.stop = asyncio.Event()
        self.task = asyncio.create_task(consume(queue, name, handler, cfg, self.stop))

    async def finish(self) -> ConsumerStats:
        self.stop.set()
        return await asyncio.wait_for(self.task, TIMEOUT)


async def settled(queue: MemoryChunkQueue) -> bool:
    stats = await queue.stats()
    return stats.waiting == 0 and stats.in_flight == 0


class TestHandling:
    async def test_every_message_is_handled_once_and_acknowledged(self) -> None:
        queue = MemoryChunkQueue()
        seen: list[bytes] = []

        async def handler(body: bytes) -> None:
            seen.append(body)

        for i in range(20):
            await queue.publish(f"m{i}".encode())
        run = Run(queue, handler, config())
        await until(lambda: settled(queue))
        stats = await run.finish()

        assert sorted(seen) == sorted(f"m{i}".encode() for i in range(20))
        assert (stats.handled, stats.dead, stats.retried) == (20, 0, 0)

    async def test_a_consumer_stops_when_asked_even_with_nothing_to_do(self) -> None:
        run = Run(MemoryChunkQueue(), lambda body: asyncio.sleep(0), config())
        await asyncio.sleep(0.05)

        stats = await run.finish()

        assert stats.handled == 0


class TestCleaningUp:
    async def test_the_cleanup_a_handler_returns_runs_after_the_acknowledgement(self) -> None:
        queue = MemoryChunkQueue()
        in_flight_when_cleaning: list[int] = []

        async def handler(body: bytes) -> Callable[[], Awaitable[None]]:
            async def clean_up() -> None:
                in_flight_when_cleaning.append((await queue.stats()).in_flight)

            return clean_up

        await queue.publish(b"one")
        run = Run(queue, handler, config())
        await until(lambda: settled(queue))
        await asyncio.sleep(0.05)
        await run.finish()

        assert in_flight_when_cleaning == [0], "the message was still unacknowledged during cleanup"

    async def test_a_cleanup_that_fails_does_not_undo_the_work(self) -> None:
        queue = MemoryChunkQueue()

        async def handler(body: bytes) -> Callable[[], Awaitable[None]]:
            async def clean_up() -> None:
                raise ConnectionError("the store went away")

            return clean_up

        await queue.publish(b"one")
        run = Run(queue, handler, config())
        await until(lambda: settled(queue))
        stats = await run.finish()

        assert (stats.handled, stats.dead, stats.retried) == (1, 0, 0)
        assert (await queue.stats()).dead == 0


class TestFailures:
    async def test_a_message_that_fails_is_tried_again_and_counted(self) -> None:
        queue = MemoryChunkQueue()
        calls = 0

        async def flaky(body: bytes) -> None:
            nonlocal calls
            calls += 1
            if calls <= 2:
                raise ConnectionError("source went away")

        await queue.publish(b"one")
        run = Run(queue, flaky, config())
        await until(lambda: settled(queue))
        stats = await run.finish()

        assert calls == 3
        assert (stats.handled, stats.retried, stats.dead) == (1, 2, 0)

    async def test_a_message_that_always_fails_is_dead_lettered_after_the_attempts(self) -> None:
        queue = MemoryChunkQueue()
        calls = 0

        async def broken(body: bytes) -> None:
            nonlocal calls
            calls += 1
            raise ConnectionError("always")

        await queue.publish(b"one")
        run = Run(queue, broken, config(max_attempts=4))
        await until(lambda: settled(queue))
        stats = await run.finish()

        assert calls == 4
        assert (stats.handled, stats.retried, stats.dead) == (0, 3, 1)
        assert (await queue.stats()).dead == 1

    async def test_giving_up_dead_letters_at_once_without_another_try(self) -> None:
        queue = MemoryChunkQueue()
        calls = 0

        async def hopeless(body: bytes) -> None:
            nonlocal calls
            calls += 1
            raise GiveUp("not a message of this job")

        await queue.publish(b"one")
        run = Run(queue, hopeless, config())
        await until(lambda: settled(queue))
        stats = await run.finish()

        assert calls == 1
        assert stats.dead == 1

    async def test_deferring_hands_the_message_back_without_using_an_attempt(self) -> None:
        queue = MemoryChunkQueue()
        calls = 0

        async def busy(body: bytes) -> None:
            nonlocal calls
            calls += 1
            if calls <= 5:  # more than max_attempts: deferring must not count
                raise Defer

        await queue.publish(b"one")
        run = Run(queue, busy, config(max_attempts=2))
        await until(lambda: settled(queue))
        stats = await run.finish()

        assert calls == 6
        assert (stats.handled, stats.deferred, stats.dead) == (1, 5, 0)

    async def test_attempts_used_by_earlier_deliveries_are_not_given_again(self) -> None:
        queue = MemoryChunkQueue()
        await queue.publish(b"one")
        await queue.receive("victim")  # delivery 1: its holder died
        calls = 0

        async def broken(body: bytes) -> None:
            nonlocal calls
            calls += 1
            raise ConnectionError("always")

        run = Run(queue, broken, config(max_attempts=3, idle_seconds=0.03))
        await until(lambda: settled(queue))
        await run.finish()

        assert calls == 2, "three attempts in all, one of them used by the dead holder"

    async def test_a_message_that_kept_killing_consumers_is_dead_lettered_unseen(self) -> None:
        """Delivered more often than the attempts allow: it is not worth another consumer."""
        queue = MemoryChunkQueue()
        await queue.publish(b"poison")
        await queue.receive("victim-1")
        for victim in ("victim-2", "victim-3", "victim-4"):  # three takeovers: delivery 4
            await asyncio.sleep(0.03)
            await queue.reclaim(victim, idle_seconds=0.01)
        calls = 0

        async def handler(body: bytes) -> None:
            nonlocal calls
            calls += 1

        run = Run(queue, handler, config(max_attempts=3, idle_seconds=0.02))
        await until(lambda: settled(queue))
        stats = await run.finish()

        assert calls == 0
        assert stats.dead == 1


class TestSurvivingDeath:
    async def test_a_cancelled_consumer_hands_its_message_back(self) -> None:
        queue = MemoryChunkQueue()
        started = asyncio.Event()

        async def stuck(body: bytes) -> None:
            started.set()
            await asyncio.sleep(3600)

        await queue.publish(b"one")
        run = Run(queue, stuck, config())
        await started.wait()

        run.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run.task

        stats = await queue.stats()
        assert (stats.waiting, stats.in_flight, stats.dead) == (1, 0, 0)
        (again,) = await queue.receive("other")
        assert again.deliveries == 1, "stopping on purpose must not use an attempt"

    async def test_a_message_held_by_a_silent_consumer_is_taken_over(self) -> None:
        queue = MemoryChunkQueue()
        await queue.publish(b"one")
        await queue.receive("zombie")  # takes it and never answers
        seen: list[bytes] = []

        async def handler(body: bytes) -> None:
            seen.append(body)

        run = Run(queue, handler, config(idle_seconds=0.05))
        await until(lambda: settled(queue))
        stats = await run.finish()

        assert seen == [b"one"]
        assert stats.handled == 1

    async def test_a_slow_handler_keeps_its_message_by_touching_it(self) -> None:
        queue = MemoryChunkQueue()
        calls: list[str] = []

        def slow(name: str) -> Callable[[bytes], Awaitable[None]]:
            async def handler(body: bytes) -> None:
                calls.append(name)
                await asyncio.sleep(0.4)  # several times the idle time

            return handler

        await queue.publish(b"one")
        first = Run(queue, slow("first"), config(idle_seconds=0.1), "c-1")
        await asyncio.sleep(0.05)
        second = Run(queue, slow("second"), config(idle_seconds=0.1), "c-2")
        await until(lambda: settled(queue))
        await first.finish()
        await second.finish()

        assert calls == ["first"], (
            "the second consumer took over a message that was being worked on"
        )
