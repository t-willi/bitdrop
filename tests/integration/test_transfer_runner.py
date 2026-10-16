"""The transfer runner moves a table through chunk store and metadata store, and survives faults.

Real in-memory adapters are used (not stubs), with small subclasses that inject faults: a source
that fails, a destination that commits and then 'dies', a store that loses a blob.
"""

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.errors import (
    ChunkNotFound,
    InvalidKey,
    StoreFull,
    TableNotFound,
)
from bitdrop_api.metadata_store import ChunkState, MetadataStore, Stage
from bitdrop_api.source import ChunkSpec, SourcePlan
from bitdrop_api.testing import make_rows
from bitdrop_connector_memory import MemoryDestination, MemorySource
from bitdrop_core.codec import ArrowIpcCodec
from bitdrop_core.registry import open_chunk_store, open_metadata_store
from bitdrop_core.transfer import TransferConfig, TransferReport, run_transfer
from bitdrop_metadata_memory import MemoryMetadataStore
from bitdrop_store_memory import MemoryChunkStore

ROWS = 5000
ROWS_PER_CHUNK = 100
ROW_BYTES = make_rows(1, ROWS).nbytes // ROWS
TIMEOUT = 20.0


def config(**overrides: object) -> TransferConfig:
    defaults: dict[str, object] = {
        "job_id": "job-1",
        "source_table": "people",
        "destination_table": "people_copy",
        "target_chunk_bytes": ROW_BYTES * ROWS_PER_CHUNK,
        "extractors": 3,
        "loaders": 3,
        "lease_seconds": 5.0,
        "poll_interval": 0.002,
        "reaper_interval": 0.02,
    }
    return TransferConfig(**{**defaults, **overrides})  # type: ignore[arg-type]


@dataclass
class World:
    source: MemorySource
    destination: MemoryDestination
    chunks: ChunkStore
    meta: MetadataStore


def world(
    *,
    source: MemorySource | None = None,
    destination: MemoryDestination | None = None,
    chunks: ChunkStore | None = None,
    meta: MetadataStore | None = None,
    rows: int = ROWS,
) -> World:
    source = source or MemorySource()
    source.add_table("people", make_rows(1, rows))
    return World(
        source=source,
        destination=destination or MemoryDestination(),
        chunks=chunks or MemoryChunkStore(),
        meta=meta or MemoryMetadataStore(),
    )


async def run(w: World, cfg: TransferConfig | None = None) -> TransferReport:
    async with asyncio.timeout(TIMEOUT):
        return await run_transfer(w.source, w.destination, w.chunks, w.meta, cfg or config())


def copied(w: World) -> pa.Table:
    return w.destination.contents("people_copy").sort_by("id")


async def assert_staging_is_empty(w: World, report: TransferReport) -> None:
    """Every blob was removed: the RAM tier holds nothing once the job is over."""
    for index in range(report.chunks_total):
        with pytest.raises(ChunkNotFound):
            await w.chunks.get(f"job-1:c{index:06d}")


# -- the happy path -------------------------------------------------------------------------------


class TestHappyPath:
    async def test_every_row_arrives_exactly_once(self) -> None:
        w = world()

        report = await run(w)

        assert report.succeeded
        assert copied(w).equals(make_rows(1, ROWS))
        assert report.chunks_total == ROWS // ROWS_PER_CHUNK
        assert (report.chunks_loaded, report.chunks_failed) == (report.chunks_total, 0)
        assert (report.rows_extracted, report.rows_loaded) == (ROWS, ROWS)
        assert report.duplicates_skipped == 0
        assert report.bytes_staged > 0

    async def test_the_chunk_store_is_empty_afterwards(self) -> None:
        w = world()

        report = await run(w)

        await assert_staging_is_empty(w, report)

    async def test_an_empty_table_succeeds_with_nothing_to_do(self) -> None:
        w = world(rows=0)

        report = await run(w)

        assert report.succeeded
        assert (report.chunks_total, report.rows_loaded) == (0, 0)
        assert await w.destination.count_rows("people_copy") == 0

    async def test_the_report_names_the_job_and_times_it(self) -> None:
        report = await run(world())

        assert report.job_id == "job-1"
        assert report.duration_seconds > 0

    @pytest.mark.parametrize("compression", ["none", "lz4", "zstd"])
    async def test_every_codec_setting_works(self, compression: str) -> None:
        w = world(rows=1000)
        async with asyncio.timeout(TIMEOUT):
            report = await run_transfer(
                w.source,
                w.destination,
                w.chunks,
                w.meta,
                config(),
                codec=ArrowIpcCodec(compression=compression),  # type: ignore[arg-type]
            )

        assert report.succeeded
        assert copied(w).equals(make_rows(1, 1000))

    async def test_a_single_worker_of_each_kind_is_enough(self) -> None:
        w = world(rows=500)

        report = await run(w, config(extractors=1, loaders=1))

        assert report.succeeded
        assert copied(w).equals(make_rows(1, 500))


class TestSwappingTheStores:
    """The same transfer, with the stores chosen only by URL."""

    @pytest.mark.parametrize("chunk_url", ["memory://", "file://{tmp}/chunks"])
    @pytest.mark.parametrize("meta_url", ["memory://", "sqlite://{tmp}/meta.db"])
    async def test_every_combination_delivers_every_row(
        self, tmp_path: Path, chunk_url: str, meta_url: str
    ) -> None:
        chunks = open_chunk_store(chunk_url.format(tmp=tmp_path))
        meta = open_metadata_store(meta_url.format(tmp=tmp_path))
        w = world(chunks=chunks, meta=meta, rows=1000)

        report = await run(w)

        assert report.succeeded
        assert copied(w).equals(make_rows(1, 1000))
        await chunks.aclose()
        await meta.aclose()


# -- faults ---------------------------------------------------------------------------------------


class FlakySource(MemorySource):
    """Fails the first ``failures`` reads of chunks listed in ``flaky`` (all reads if -1)."""

    def __init__(self, flaky: set[str], failures: int) -> None:
        super().__init__()
        self.flaky, self.failures, self.attempts = flaky, failures, 0

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        if chunk.chunk_id in self.flaky and (self.failures < 0 or self.attempts < self.failures):
            self.attempts += 1
            raise ConnectionError("source went away")
        return await super().read_chunk(plan, chunk)


class SlowSource(MemorySource):
    """Takes ``delay`` seconds to read one particular chunk."""

    def __init__(self, slow_chunk: str, delay: float) -> None:
        super().__init__()
        self.slow_chunk, self.delay, self.reads = slow_chunk, delay, 0

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        self.reads += 1
        if chunk.chunk_id == self.slow_chunk:
            await asyncio.sleep(self.delay)
        return await super().read_chunk(plan, chunk)


class TestHeartbeats:
    async def test_a_read_slower_than_the_lease_is_not_taken_over_by_someone_else(self) -> None:
        """Without heartbeats the reaper would void the lease and the chunk would be read twice."""
        source = SlowSource("c000002", delay=0.5)
        w = world(source=source, rows=1000)

        report = await run(w, config(lease_seconds=0.15, reaper_interval=0.02))

        assert report.succeeded
        assert source.reads == report.chunks_total, "a chunk was read more than once"
        assert (await w.meta.get_chunk("job-1", "c000002")).failures == 0


class TestSourceFailures:
    async def test_a_transient_read_error_is_retried(self) -> None:
        w = world(source=FlakySource({"c000003"}, failures=1))

        report = await run(w)

        assert report.succeeded
        assert copied(w).equals(make_rows(1, ROWS))
        assert (await w.meta.get_chunk("job-1", "c000003")).failures == 1

    async def test_a_permanently_failing_chunk_is_reported_and_the_rest_still_arrives(self) -> None:
        w = world(source=FlakySource({"c000003"}, failures=-1))

        report = await run(w, config(max_attempts=2))

        assert not report.succeeded
        assert (report.chunks_loaded, report.chunks_failed) == (report.chunks_total - 1, 1)
        failure = report.failures[0]
        assert (failure.chunk_id, failure.state, failure.failures) == (
            "c000003",
            ChunkState.FAILED,
            2,
        )
        assert failure.last_error is not None
        assert "source went away" in failure.last_error
        assert copied(w).num_rows == ROWS - ROWS_PER_CHUNK

    async def test_an_unknown_table_fails_before_anything_is_registered(self) -> None:
        w = world()

        with pytest.raises(TableNotFound):
            await run(w, config(source_table="no_such_table"))

        assert sum((await w.meta.counts("job-1")).values()) == 0


class CommitThenDieDestination(MemoryDestination):
    """Writes the chunk (committing it), then fails once, as if the loader died right after."""

    def __init__(self, victim: str, *, hang: bool = False) -> None:
        super().__init__()
        self.victim, self.hang, self.struck = victim, hang, False

    async def write_chunk(self, job_id: str, chunk_id: str, table: str, data: pa.Table):  # type: ignore[no-untyped-def]
        result = await super().write_chunk(job_id, chunk_id, table, data)
        if chunk_id == self.victim and not self.struck:
            self.struck = True
            if self.hang:
                await asyncio.sleep(3600)  # a zombie: committed, then never heard from again
            raise ConnectionError("loader died after committing")
        return result


class TestLoaderCrashAfterCommit:
    """The duplicate window the destination ledger exists for."""

    async def test_a_failure_after_commit_does_not_duplicate_rows(self) -> None:
        w = world(destination=CommitThenDieDestination("c000007"))

        report = await run(w)

        assert report.succeeded
        assert copied(w).equals(make_rows(1, ROWS)), "rows were duplicated or lost"
        assert report.duplicates_skipped == 1

    async def test_a_hung_loader_is_timed_out_and_its_chunk_retried_without_duplicates(
        self,
    ) -> None:
        """The loader commits, then hangs. The chunk timeout cuts it off and the chunk is redone."""
        w = world(destination=CommitThenDieDestination("c000007", hang=True))

        report = await run(w, config(chunk_timeout=0.15))

        assert report.succeeded
        assert copied(w).equals(make_rows(1, ROWS)), "rows were duplicated or lost"
        assert report.duplicates_skipped == 1


class AmnesiacStore(MemoryChunkStore):
    """Loses the blob of one chunk the first time it is read, like an evicted key."""

    def __init__(self, victim_key: str) -> None:
        super().__init__()
        self.victim_key, self.struck = victim_key, False

    async def get(self, key: str) -> bytes:
        if key == self.victim_key and not self.struck:
            self.struck = True
            await self.delete(key)
        return await super().get(key)


class TestLostBlob:
    async def test_a_chunk_missing_from_the_store_is_extracted_again(self) -> None:
        w = world(chunks=AmnesiacStore("job-1:c000005"))

        report = await run(w)

        assert report.succeeded
        assert copied(w).equals(make_rows(1, ROWS))
        assert (await w.meta.get_chunk("job-1", "c000005")).failures == 1


class HangingStore(MemoryChunkStore):
    """Hangs the first ``get`` or ``put`` of one key forever, like a stalled network connection."""

    def __init__(self, victim_key: str, *, hang_on: str) -> None:
        super().__init__()
        self.victim_key, self.hang_on, self.struck = victim_key, hang_on, False

    async def _maybe_hang(self, operation: str, key: str) -> None:
        if operation == self.hang_on and key == self.victim_key and not self.struck:
            self.struck = True
            await asyncio.sleep(3600)

    async def get(self, key: str) -> bytes:
        await self._maybe_hang("get", key)
        return await super().get(key)

    async def put(self, key: str, value: bytes) -> None:
        await self._maybe_hang("put", key)
        await super().put(key, value)


class TestHungChunkStore:
    """A networked store can stall. Its calls need the same timeout as source and destination."""

    @pytest.mark.parametrize("operation", ["get", "put"])
    async def test_a_hung_call_is_timed_out_and_the_chunk_retried(self, operation: str) -> None:
        w = world(chunks=HangingStore("job-1:c000004", hang_on=operation))

        report = await run(w, config(chunk_timeout=0.15))

        assert report.succeeded
        assert copied(w).equals(make_rows(1, ROWS))
        assert (await w.meta.get_chunk("job-1", "c000004")).failures == 1
        await assert_staging_is_empty(w, report)


class CountingStore(MemoryChunkStore):
    """Counts how often ``put`` was refused because the store was full."""

    def __init__(self, max_bytes: int) -> None:
        super().__init__(max_bytes=max_bytes)
        self.refusals = 0

    async def put(self, key: str, value: bytes) -> None:
        try:
            await super().put(key, value)
        except StoreFull:
            self.refusals += 1
            raise


class SlowDestination(MemoryDestination):
    async def write_chunk(self, job_id: str, chunk_id: str, table: str, data: pa.Table):  # type: ignore[no-untyped-def]
        await asyncio.sleep(0.005)
        return await super().write_chunk(job_id, chunk_id, table, data)


class TestBackpressure:
    async def test_a_store_smaller_than_the_job_slows_extraction_instead_of_failing_it(
        self,
    ) -> None:
        chunk_bytes = len(ArrowIpcCodec().encode(make_rows(1, ROWS_PER_CHUNK)))
        store = CountingStore(max_bytes=chunk_bytes * 3)
        w = world(chunks=store, destination=SlowDestination())

        report = await run(w, config(extractors=4, loaders=1))

        assert report.succeeded
        assert copied(w).equals(make_rows(1, ROWS))
        assert store.refusals > 0, "the store never filled up: the test proves nothing"
        assert (await w.meta.counts("job-1"))[ChunkState.FAILED] == 0

    async def test_a_value_limit_of_the_store_shrinks_the_chunks(self) -> None:
        """A store with a small item limit (like memcached's 1 MB) steers the chunk size."""
        store = MemoryChunkStore(max_value_bytes=ROW_BYTES * ROWS_PER_CHUNK)
        w = world(chunks=store)

        report = await run(w, config(target_chunk_bytes=ROW_BYTES * ROWS))  # asks for ONE chunk

        assert report.succeeded
        assert report.chunks_total > 1
        assert copied(w).equals(make_rows(1, ROWS))


class StallAfterFirstLoadCompletion(MemoryMetadataStore):
    """Records the first load completion, then stalls: the process 'died' right after marking it."""

    def __init__(self, reached: asyncio.Event) -> None:
        super().__init__()
        self.reached, self.struck = reached, False

    async def complete(self, claim):  # type: ignore[no-untyped-def]
        await super().complete(claim)
        if claim.stage is Stage.LOAD and not self.struck:
            self.struck = True
            self.reached.set()
            await asyncio.sleep(3600)


class TestCrashAfterCompletion:
    """The window after a chunk was recorded as loaded but before its staged copy was deleted."""

    async def test_nothing_is_left_staged_after_the_job_is_resumed(self, tmp_path: Path) -> None:
        """A staging store that outlives the workers (disk, or a network server) must not leak.

        If the blob were deleted only after the chunk was marked loaded, a crash in between would
        strand it for good: nobody revisits a loaded chunk, and in a shared server that is RAM.
        """
        reached = asyncio.Event()
        chunks = open_chunk_store(f"file://{tmp_path}/chunks")
        w = world(chunks=chunks, meta=StallAfterFirstLoadCompletion(reached), rows=1000)
        cfg = config(lease_seconds=0.3, reaper_interval=0.05)
        first_run = asyncio.create_task(run(w, cfg))
        await asyncio.wait_for(reached.wait(), timeout=10)
        first_run.cancel()  # the 'crash'
        with suppress(asyncio.CancelledError):
            await first_run

        resumed = await run(w, cfg)

        assert resumed.succeeded
        assert copied(w).equals(make_rows(1, 1000))
        assert list((tmp_path / "chunks").iterdir()) == [], "a staged chunk was leaked"


# -- resuming -------------------------------------------------------------------------------------


class TestRerun:
    async def test_running_the_same_job_again_loads_nothing_twice(self) -> None:
        w = world()
        await run(w)

        second = await run(w)

        assert second.succeeded
        assert second.rows_loaded == 0
        assert copied(w).equals(make_rows(1, ROWS))


# -- configuration --------------------------------------------------------------------------------


class TestConfigValidation:
    @pytest.mark.parametrize(
        "overrides",
        [
            {"extractors": 0},
            {"loaders": 0},
            {"lease_seconds": 0},
            {"target_chunk_bytes": 0},
            {"max_attempts": 0},
            {"poll_interval": 0},
            {"chunk_timeout": 0},
            {"backpressure_timeout": 0},
            {"reaper_interval": 0},
        ],
    )
    def test_nonsense_numbers_are_rejected(self, overrides: dict[str, object]) -> None:
        with pytest.raises(ValueError, match=next(iter(overrides))):
            config(**overrides)

    def test_the_job_id_must_be_a_valid_key_and_short_enough_for_store_keys(self) -> None:
        with pytest.raises(InvalidKey):
            config(job_id="has/slash")
        with pytest.raises(ValueError, match="job_id"):
            config(job_id="j" * 65)
