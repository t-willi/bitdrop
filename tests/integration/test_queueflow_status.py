"""Is the queue workflow's job finished? Read from the markers and the two queues, never changed.

Real in-memory adapters are used; the containers are the real ones from the pipeline.
"""

import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from bitdrop_api.chunk_queue import QueueStats
from bitdrop_api.testing import make_rows
from bitdrop_connector_memory import MemoryDestination, MemorySource
from bitdrop_core.queueflow import (
    ConsumerConfig,
    DestinationWorkerConfig,
    FileMessage,
    JobState,
    PlannerConfig,
    QueueflowStatus,
    SourceWorkerConfig,
    WorkMessage,
    chunking_key,
    encode_file,
    encode_work,
    queueflow_status,
    run_destination_worker,
    run_planner,
    run_source_worker,
    wait_until_finished,
)
from bitdrop_marker_memory import MemoryMarkerStore
from bitdrop_queue_memory import MemoryChunkQueue
from bitdrop_store_memory import MemoryChunkStore

JOB = "job-1"
ROWS = 1000
CHUNKS = 10
ROW_BYTES = make_rows(1, ROWS).nbytes // ROWS
FAST = ConsumerConfig(
    max_attempts=2, poll_interval=0.01, idle_seconds=5.0, retry_delay=0.001, defer_delay=0.01
)


class World:
    def __init__(self) -> None:
        self.source = MemorySource({"people": make_rows(1, ROWS)})
        self.destination = MemoryDestination()
        self.staging = MemoryChunkStore()
        self.work = MemoryChunkQueue()
        self.files = MemoryChunkQueue()
        self.markers = MemoryMarkerStore()
        self.stop = asyncio.Event()
        self.tasks: list[asyncio.Task[Any]] = []

    async def plan(self) -> None:
        await run_planner(
            self.source,
            self.destination,
            self.work,
            self.markers,
            PlannerConfig(JOB, "people", "people_copy", ROW_BYTES * 100),
        )

    def start(self, *, sources: int = 1, destinations: int = 1) -> None:
        for i in range(sources):
            self.tasks.append(
                asyncio.create_task(
                    run_source_worker(
                        self.source,
                        self.staging,
                        self.work,
                        self.files,
                        self.markers,
                        SourceWorkerConfig(JOB, FAST),
                        self.stop,
                        name=f"source-{i}",
                    )
                )
            )
        for i in range(destinations):
            self.tasks.append(
                asyncio.create_task(
                    run_destination_worker(
                        self.destination,
                        self.staging,
                        self.files,
                        DestinationWorkerConfig(JOB, "people_copy", FAST),
                        self.stop,
                        name=f"destination-{i}",
                    )
                )
            )

    async def status(self) -> QueueflowStatus:
        return await queueflow_status(self.markers, self.work, self.files, JOB)

    async def stop_all(self) -> None:
        self.stop.set()
        await asyncio.gather(*self.tasks)


class TestStates:
    async def test_before_the_planner_has_run_nothing_is_known(self) -> None:
        status = await World().status()

        assert status.state is JobState.NOT_PLANNED
        assert status.chunks_total is None

    async def test_a_planner_that_has_not_finished_publishing_is_still_planning(self) -> None:
        w = World()
        await w.plan()
        # take the completion marker away, as if the planner had died before writing it
        await w.markers.aclose()
        await w.markers.set_if_absent(chunking_key(JOB), b'{"chunks":10,"format":1,"sha256":"x"}')

        status = await w.status()

        assert status.state is JobState.PLANNING
        assert status.chunks_total == CHUNKS

    async def test_a_planned_job_nobody_works_on_is_running_with_everything_waiting(self) -> None:
        w = World()
        await w.plan()

        status = await w.status()

        assert status.state is JobState.RUNNING
        assert (status.chunks_total, status.extracted, status.loaded) == (CHUNKS, 0, 0)
        assert status.work.waiting == CHUNKS

    async def test_a_job_whose_chunks_are_all_loaded_is_finished(self) -> None:
        w = World()
        await w.plan()
        w.start(sources=3, destinations=3)

        status = await wait_until_finished(
            w.markers, w.work, w.files, JOB, poll_interval=0.01, give_up_after=20
        )
        await w.stop_all()

        assert status.state is JobState.FINISHED
        assert (status.chunks_total, status.extracted, status.loaded) == (CHUNKS, CHUNKS, CHUNKS)
        assert (await w.destination.count_rows("people_copy")) == ROWS

    async def test_a_job_with_a_set_aside_chunk_finishes_with_failures(self) -> None:
        w = World()
        await w.plan()
        await w.work.publish(encode_work(WorkMessage(JOB, "people", "not-a-chunk")))
        w.start()

        status = await wait_until_finished(
            w.markers, w.work, w.files, JOB, poll_interval=0.01, give_up_after=20
        )
        await w.stop_all()

        assert status.state is JobState.FINISHED_WITH_FAILURES
        assert status.work.dead == 1
        assert status.failed == 1

    async def test_a_job_extracted_but_not_loaded_is_still_running(self) -> None:
        """Nothing in the work queue is not enough: the staged chunks still wait for a loader."""
        w = World()
        await w.plan()
        w.start(sources=2, destinations=0)
        deadline = time.monotonic() + 20
        while (await w.status()).files.waiting < CHUNKS:
            assert time.monotonic() < deadline, "the sources did not stage every chunk"
            await asyncio.sleep(0.01)

        status = await w.status()
        await w.stop_all()

        assert status.state is JobState.RUNNING
        assert (status.extracted, status.loaded) == (CHUNKS, 0)

    async def test_a_staged_chunk_that_was_set_aside_counts_as_a_failure(self) -> None:
        w = World()
        await w.plan()
        w.start()
        await wait_until_finished(
            w.markers, w.work, w.files, JOB, poll_interval=0.01, give_up_after=20
        )
        await w.files.publish(encode_file(FileMessage("job-2", "people", "c", "job-2:c", 1, 1)))

        status = await wait_until_finished(
            w.markers, w.work, w.files, JOB, poll_interval=0.01, give_up_after=20
        )
        await w.stop_all()

        assert status.state is JobState.FINISHED_WITH_FAILURES
        assert (status.files.dead, status.work.dead, status.failed) == (1, 0, 1)

    async def test_a_finished_job_stays_finished_when_asked_again(self) -> None:
        w = World()
        await w.plan()
        w.start()
        await wait_until_finished(
            w.markers, w.work, w.files, JOB, poll_interval=0.01, give_up_after=20
        )
        await w.stop_all()

        assert (await w.status()).state is JobState.FINISHED


class TestAskingIsHarmless:
    async def test_looking_changes_nothing(self) -> None:
        w = World()
        await w.plan()
        before = (await w.work.stats(), await w.files.stats())

        for _ in range(3):
            await w.status()

        assert (await w.work.stats(), await w.files.stats()) == before


class Hooked(MemoryChunkQueue):
    """Runs ``hook`` once, right after the first ``stats`` call of either hooked queue returned."""

    fired = False
    hook: Callable[[], Coroutine[Any, Any, None]]

    async def stats(self) -> QueueStats:
        result = await super().stats()
        if not Hooked.fired:
            Hooked.fired = True
            await Hooked.hook()
        return result


class TestTheOrderOfReading:
    async def test_a_chunk_moving_between_the_queues_while_looking_is_never_missed(self) -> None:
        """Counted in the files queue first, a chunk handed over between the two reads vanishes."""
        Hooked.fired = False
        work, files = Hooked(), Hooked()
        w = World()
        w.work, w.files = work, files
        await w.plan()
        for delivery in await work.receive("source", max_messages=CHUNKS):
            await work.ack(delivery)  # every chunk is through, except the one published next
        await work.publish(encode_work(WorkMessage(JOB, "people", "rows.0.100")))
        (held,) = await work.receive("source")

        async def hand_over() -> None:
            await files.publish(b"staged")
            await work.ack(held)

        Hooked.hook = hand_over

        status = await w.status()

        assert status.state is JobState.RUNNING, "the chunk in transit was counted nowhere"

    def test_finished_means_both_queues_are_empty(self) -> None:
        assert JobState.FINISHED.is_over
        assert JobState.FINISHED_WITH_FAILURES.is_over
        assert not JobState.RUNNING.is_over
        assert not JobState.PLANNING.is_over
        assert not JobState.NOT_PLANNED.is_over


class TestWaiting:
    async def test_waiting_gives_up_after_the_timeout(self) -> None:
        w = World()
        await w.plan()
        started = time.monotonic()

        with pytest.raises(TimeoutError):
            await wait_until_finished(
                w.markers, w.work, w.files, JOB, poll_interval=0.01, give_up_after=0.2
            )

        assert 0.15 <= time.monotonic() - started < 5
