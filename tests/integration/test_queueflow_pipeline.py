"""Source and destination containers, joined by queues: the whole queue workflow in one process.

Real in-memory adapters are used; small subclasses make reads fail, stores fill, and loads die.
Containers are tasks; stopping them is how a test ends a run.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable

import pyarrow as pa
import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.errors import ChunkNotFound, StoreFull
from bitdrop_api.source import ChunkSpec, SourcePlan
from bitdrop_api.testing import make_rows
from bitdrop_connector_memory import MemoryDestination, MemorySource
from bitdrop_core.codec import ArrowIpcCodec
from bitdrop_core.queueflow import (
    ConsumerConfig,
    ConsumerStats,
    DestinationWorkerConfig,
    FileMessage,
    PlannerConfig,
    SourceWorkerConfig,
    WorkMessage,
    decode_file,
    encode_file,
    encode_work,
    run_destination_worker,
    run_planner,
    run_source_worker,
    schema_key,
    settings_key,
)
from bitdrop_marker_memory import MemoryMarkerStore
from bitdrop_queue_memory import MemoryChunkQueue
from bitdrop_store_memory import MemoryChunkStore

JOB = "job-1"
ROWS = 1000
ROWS_PER_CHUNK = 100
CHUNKS = ROWS // ROWS_PER_CHUNK
ROW_BYTES = make_rows(1, ROWS).nbytes // ROWS
TIMEOUT = 20.0

FAST = ConsumerConfig(
    max_attempts=3, poll_interval=0.01, idle_seconds=5.0, retry_delay=0.001, defer_delay=0.01
)


class World:
    """Everything the containers share, and the means to start and stop them."""

    def __init__(
        self,
        *,
        source: MemorySource | None = None,
        destination: MemoryDestination | None = None,
        staging: ChunkStore | None = None,
    ) -> None:
        self.source = source or MemorySource()
        self.source.add_table("people", make_rows(1, ROWS))
        self.destination = destination or MemoryDestination()
        self.staging: ChunkStore = staging or MemoryChunkStore()
        self.work = MemoryChunkQueue()
        self.files = MemoryChunkQueue()
        self.markers = MemoryMarkerStore()
        self.stop = asyncio.Event()
        self.tasks: list[asyncio.Task[ConsumerStats]] = []

    async def plan(self) -> None:
        await run_planner(
            self.source,
            self.destination,
            self.work,
            self.markers,
            PlannerConfig(JOB, "people", "people_copy", ROW_BYTES * ROWS_PER_CHUNK),
        )

    def start_source(
        self, name: str, *, source: MemorySource | None = None, consumer: ConsumerConfig = FAST
    ) -> asyncio.Task[ConsumerStats]:
        task = asyncio.create_task(
            run_source_worker(
                source or self.source,
                self.staging,
                self.work,
                self.files,
                self.markers,
                SourceWorkerConfig(JOB, consumer),
                self.stop,
                name=name,
            )
        )
        self.tasks.append(task)
        return task

    def start_destination(
        self,
        name: str,
        *,
        destination: MemoryDestination | None = None,
        consumer: ConsumerConfig = FAST,
    ) -> asyncio.Task[ConsumerStats]:
        task = asyncio.create_task(
            run_destination_worker(
                destination or self.destination,
                self.staging,
                self.files,
                DestinationWorkerConfig(JOB, "people_copy", consumer),
                self.stop,
                name=name,
            )
        )
        self.tasks.append(task)
        return task

    async def until(self, condition: Callable[[], Awaitable[bool]]) -> None:
        deadline = time.monotonic() + TIMEOUT
        while not await condition():
            if time.monotonic() > deadline:
                pytest.fail("the condition was not reached in time")
            await asyncio.sleep(0.005)

    async def loaded(self) -> bool:
        return (await self.destination.count_rows("people_copy")) == ROWS

    async def quiet(self) -> bool:
        stats = [await self.work.stats(), await self.files.stats()]
        return all(s.waiting == 0 and s.in_flight == 0 for s in stats)

    async def finish(self) -> list[ConsumerStats]:
        self.stop.set()
        done = await asyncio.wait_for(asyncio.gather(*self.tasks, return_exceptions=True), TIMEOUT)
        return [stats for stats in done if isinstance(stats, ConsumerStats)]  # not the killed ones

    def copied(self) -> pa.Table:
        return self.destination.contents("people_copy").sort_by("id")


class TestTheHappyPath:
    async def test_every_row_arrives_exactly_once_and_staging_is_empty(self) -> None:
        w = World()
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")

        await w.until(w.loaded)
        await w.until(w.quiet)
        source_stats, destination_stats = await w.finish()

        assert w.copied().equals(make_rows(1, ROWS))
        assert (source_stats.handled, destination_stats.handled) == (CHUNKS, CHUNKS)
        assert (source_stats.dead, destination_stats.dead) == (0, 0)
        for index in range(CHUNKS):
            with pytest.raises(ChunkNotFound):
                await w.staging.get(f"{JOB}:rows.{index * ROWS_PER_CHUNK}.{ROWS_PER_CHUNK}")

    async def test_many_containers_of_each_kind_share_the_work_without_duplicates(self) -> None:
        w = World()
        await w.plan()
        for i in range(4):
            w.start_source(f"source-{i}")
            w.start_destination(f"destination-{i}")

        await w.until(w.loaded)
        await w.until(w.quiet)
        stats = await w.finish()

        assert w.copied().equals(make_rows(1, ROWS))
        assert sum(s.handled for s in stats[0::2]) == CHUNKS  # source containers
        assert sum(s.handled for s in stats[1::2]) == CHUNKS  # destination containers

    async def test_destinations_may_start_before_the_planner_has_published_anything(self) -> None:
        w = World()
        w.start_destination("destination-1")
        w.start_source("source-1")
        await asyncio.sleep(0.05)

        await w.plan()
        await w.until(w.loaded)
        await w.finish()

        assert w.copied().equals(make_rows(1, ROWS))

    async def test_the_file_message_names_the_staged_chunk(self) -> None:
        w = World()
        await w.plan()
        w.start_source("source-1")
        await w.until(lambda: _count(w.files, CHUNKS))
        await w.finish()

        delivered = await w.files.receive("reader", max_messages=CHUNKS)
        messages = sorted((decode_file(d.body) for d in delivered), key=lambda m: m.chunk)
        assert len(messages) == CHUNKS
        first = next(m for m in messages if m.chunk == "rows.0.100")
        assert (first.job, first.table, first.rows) == (JOB, "people", ROWS_PER_CHUNK)
        assert len(await w.staging.get(first.key)) == first.bytes


async def _discard(queue: MemoryChunkQueue) -> None:
    """Remove every waiting message, as if it had been consumed."""
    for delivery in await queue.receive("discarder", max_messages=1000):
        await queue.ack(delivery)


async def _count(queue: MemoryChunkQueue, waiting: int) -> bool:
    return (await queue.stats()).waiting == waiting


class ReadCounting(MemorySource):
    """Counts the reads of every chunk and fails the ones in ``fail`` for ``times`` reads."""

    def __init__(self, fail: dict[str, int] | None = None) -> None:
        super().__init__()
        self.reads: dict[str, int] = {}
        self.fail = dict(fail or {})

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        self.reads[chunk.chunk_id] = self.reads.get(chunk.chunk_id, 0) + 1
        if self.fail.get(chunk.chunk_id, 0) != 0:
            self.fail[chunk.chunk_id] -= 1
            raise ConnectionError("source went away")
        return await super().read_chunk(plan, chunk)


class TestSourceTrouble:
    async def test_a_read_that_fails_now_and_then_is_retried_inside_the_container(self) -> None:
        source = ReadCounting({"rows.300.100": 2})
        w = World(source=source)
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")

        await w.until(w.loaded)
        stats = await w.finish()

        assert w.copied().equals(make_rows(1, ROWS))
        assert source.reads["rows.300.100"] == 3
        assert stats[0].retried == 2

    async def test_a_chunk_that_can_never_be_read_is_set_aside_and_the_rest_arrive(self) -> None:
        source = ReadCounting({"rows.300.100": -1})
        w = World(source=source)
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")

        await w.until(lambda: _loaded_rows(w, ROWS - ROWS_PER_CHUNK))
        await w.until(w.quiet)
        stats = await w.finish()

        assert stats[0].dead == 1
        assert (await w.work.stats()).dead == 1
        assert (await w.destination.count_rows("people_copy")) == ROWS - ROWS_PER_CHUNK

    async def test_a_chunk_id_the_source_cannot_parse_is_set_aside_without_a_read(self) -> None:
        source = ReadCounting()
        w = World(source=source)
        await w.plan()
        await _discard(w.work)  # keep only the bad message
        await w.work.publish(encode_work(WorkMessage(JOB, "people", "not-a-chunk")))
        w.start_source("source-1")

        await w.until(w.quiet)
        stats = await w.finish()

        assert stats[0].dead == 1
        assert source.reads == {}

    async def test_a_full_staging_store_makes_the_source_wait_instead_of_giving_up(self) -> None:
        staging = FullFor(fullness=6)
        w = World(staging=staging)
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")

        await w.until(w.loaded)
        stats = await w.finish()

        assert stats[0].deferred == 6
        assert (stats[0].dead, stats[0].retried) == (0, 0)


class FullFor(MemoryChunkStore):
    """Refuses the first ``fullness`` puts, like a store that is full until the loaders catch up."""

    def __init__(self, fullness: int) -> None:
        super().__init__()
        self.fullness = fullness

    async def put(self, key: str, value: bytes) -> None:
        if self.fullness > 0:
            self.fullness -= 1
            raise StoreFull(key)
        await super().put(key, value)


async def _loaded_rows(w: World, rows: int) -> bool:
    return (await w.destination.count_rows("people_copy")) == rows


class TestContainersDying:
    async def test_a_source_killed_while_reading_hands_its_chunk_to_another(self) -> None:
        gate = asyncio.Event()
        reading = asyncio.Event()

        class Stuck(ReadCounting):
            async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
                if chunk.chunk_id == "rows.0.100" and not gate.is_set():
                    reading.set()
                    await asyncio.sleep(3600)
                return await super().read_chunk(plan, chunk)

        w = World(source=Stuck())
        await w.plan()
        doomed = w.start_source("source-doomed")
        await asyncio.wait_for(reading.wait(), 5)
        doomed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await doomed
        gate.set()
        w.start_source("source-2")
        w.start_destination("destination-1")

        await w.until(w.loaded)
        await w.until(w.quiet)
        await w.finish()

        assert w.copied().equals(make_rows(1, ROWS))
        assert (await w.work.stats()).dead == 0

    async def test_a_source_that_went_silent_loses_its_chunk_after_the_idle_time(self) -> None:
        w = World()
        await w.plan()
        await w.work.receive(
            "zombie", max_messages=3
        )  # three chunks taken by a container that died
        quick = ConsumerConfig(
            max_attempts=3, poll_interval=0.01, idle_seconds=0.1, retry_delay=0.001
        )
        w.start_source("source-1", consumer=quick)
        w.start_destination("destination-1")

        await w.until(w.loaded)
        stats = await w.finish()

        assert w.copied().equals(make_rows(1, ROWS))
        assert stats[0].taken_over == 3

    async def test_a_loader_that_dies_after_its_commit_causes_no_duplicate_rows(self) -> None:
        class CommitThenDie(MemoryDestination):
            """Really commits the 4th chunk, then fails once, as if the container died after it."""

            def __init__(self) -> None:
                super().__init__()
                self.writes = 0

            async def write_chunk(self, job_id, chunk_id, table, data):  # type: ignore[no-untyped-def]
                result = await super().write_chunk(job_id, chunk_id, table, data)
                self.writes += 1
                if self.writes == 4:
                    raise ConnectionError("container died after the commit")
                return result

        destination = CommitThenDie()
        w = World(destination=destination)
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1", destination=destination)

        await w.until(w.loaded)
        await w.until(w.quiet)
        stats = await w.finish()

        assert w.copied().equals(make_rows(1, ROWS)), "rows are missing or doubled"
        assert stats[1].retried == 1

    async def test_a_destination_killed_while_loading_hands_its_file_to_another(self) -> None:
        loading = asyncio.Event()
        released = asyncio.Event()

        class Stuck(MemoryDestination):
            async def write_chunk(self, job_id, chunk_id, table, data):  # type: ignore[no-untyped-def]
                if not released.is_set():
                    loading.set()
                    await asyncio.sleep(3600)
                return await super().write_chunk(job_id, chunk_id, table, data)

        w = World(destination=Stuck())
        await w.plan()
        w.start_source("source-1")
        doomed = w.start_destination("destination-doomed")
        await asyncio.wait_for(loading.wait(), 5)
        doomed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await doomed
        released.set()
        w.start_destination("destination-2")

        await w.until(w.loaded)
        await w.until(w.quiet)
        await w.finish()

        assert w.copied().equals(make_rows(1, ROWS))


class TestDestinationTrouble:
    async def test_a_staged_chunk_that_vanished_is_set_aside_not_forgotten(self) -> None:
        class Forgetful(MemoryChunkStore):
            async def get(self, key: str) -> bytes:
                if key.endswith("rows.200.100"):
                    raise ChunkNotFound(key)
                return await super().get(key)

        w = World(staging=Forgetful())
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")

        await w.until(lambda: _loaded_rows(w, ROWS - ROWS_PER_CHUNK))
        await w.until(w.quiet)
        stats = await w.finish()

        assert stats[1].dead == 1
        assert (await w.files.stats()).dead == 1

    async def test_a_corrupt_staged_chunk_is_set_aside_without_a_retry(self) -> None:
        class Mangling(MemoryChunkStore):
            async def get(self, key: str) -> bytes:
                data = await super().get(key)
                return data[:10] if key.endswith("rows.500.100") else data

        w = World(staging=Mangling())
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")

        await w.until(lambda: _loaded_rows(w, ROWS - ROWS_PER_CHUNK))
        await w.until(w.quiet)
        stats = await w.finish()

        assert (stats[1].dead, stats[1].retried) == (1, 0)

    async def test_a_chunk_staged_by_the_container_decodes_to_what_the_source_read(self) -> None:
        w = World()
        await w.plan()
        w.start_source("source-1")
        await w.until(lambda: _count(w.files, CHUNKS))
        await w.finish()

        stored = await w.staging.get(f"{JOB}:rows.0.100")

        assert ArrowIpcCodec().decode(stored).equals(make_rows(1, ROWS).slice(0, ROWS_PER_CHUNK))


class TestOtherJobs:
    async def test_a_message_for_another_job_is_set_aside(self) -> None:
        w = World()
        await w.plan()
        await _discard(w.work)
        await w.work.publish(encode_work(WorkMessage("job-2", "people", "rows.0.100")))
        w.start_source("source-1")

        await w.until(w.quiet)
        stats = await w.finish()

        assert stats[0].dead == 1
        assert await w.markers.get(settings_key("job-2")) is None


class TestTheLedgerAndTheCleanup:
    async def test_the_destination_is_told_each_chunk_by_its_planned_id(self) -> None:
        class Recording(MemoryDestination):
            def __init__(self) -> None:
                super().__init__()
                self.ids: list[str] = []

            async def write_chunk(self, job_id, chunk_id, table, data):  # type: ignore[no-untyped-def]
                self.ids.append(f"{job_id}/{chunk_id}")
                return await super().write_chunk(job_id, chunk_id, table, data)

        destination = Recording()
        w = World(destination=destination)
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")

        await w.until(w.loaded)
        await w.finish()

        expected = {f"{JOB}/rows.{i * ROWS_PER_CHUNK}.{ROWS_PER_CHUNK}" for i in range(CHUNKS)}
        assert set(destination.ids) == expected

    async def test_a_staged_chunk_is_deleted_only_after_its_message_is_acknowledged(self) -> None:
        """Deleted first, a crash before the acknowledgement would look like a lost chunk."""
        seen: list[int] = []

        class Watching(MemoryChunkStore):
            files: MemoryChunkQueue

            async def delete(self, key: str) -> None:
                seen.append((await self.files.stats()).in_flight)
                await super().delete(key)

        staging = Watching()
        w = World(staging=staging)
        staging.files = w.files
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")

        await w.until(w.loaded)
        await w.until(w.quiet)
        await w.finish()

        assert seen, "nothing was deleted"
        assert set(seen) == {0}, f"in flight at the time of each delete: {seen}"

    async def test_a_file_message_for_another_job_is_set_aside_and_loads_nothing(self) -> None:
        w = World()
        await w.plan()
        await _discard(w.work)
        await w.staging.put("job-2:c", ArrowIpcCodec().encode(make_rows(1, 5)))  # a loadable chunk
        await w.files.publish(encode_file(FileMessage("job-2", "people", "c", "job-2:c", 5, 1)))
        w.start_destination("destination-1")

        await w.until(w.quiet)
        stats = await w.finish()

        assert (stats[0].dead, stats[0].handled, stats[0].retried) == (1, 0, 0)
        assert (await w.destination.count_rows("people_copy")) == 0


class TestWaitingForThePlanner:
    async def test_a_source_that_cannot_find_the_schema_waits_instead_of_giving_up(self) -> None:
        class Hiding(MemoryMarkerStore):
            hidden = True

            async def get(self, key: str) -> bytes | None:
                return None if self.hidden and key == schema_key(JOB) else await super().get(key)

        w = World()
        hiding = Hiding()
        w.markers = hiding
        await w.plan()
        w.start_source("source-1")
        w.start_destination("destination-1")
        await asyncio.sleep(0.1)
        assert (await w.work.stats()).dead == 0

        hiding.hidden = False
        await w.until(w.loaded)
        await w.finish()

        assert w.copied().equals(make_rows(1, ROWS))
