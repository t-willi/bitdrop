"""Run one table transfer: plan, extract, stage in RAM, load, and recover from faults.

``run_transfer`` is the master and the workers in one process. It only talks to the contracts
(``Source``, ``Destination``, ``ChunkStore``, ``MetadataStore``, ``ChunkCodec``), so any adapters
can be plugged in; see ``docs/principles.md``.

The flow, per the design in ``docs/ideation.md``:

1. **Plan**: the source cuts the table into chunks; they are registered as ``PENDING``.
2. **Extract**: extractors claim a chunk, read it, encode it, and stage the bytes in the chunk
   store.
3. **Load**: loaders claim an extracted chunk, fetch and decode it, and write it to the destination,
   which records the chunk in its ledger in the same transaction (so doing it twice is harmless).
4. **Recover**: a reaper voids leases of dead workers; failed chunks are retried up to
   ``max_attempts``; heartbeats keep the leases of slow-but-alive workers.
5. **Finish** when every chunk is ``LOADED`` or ``FAILED``.

Backpressure is simple for now: an extractor facing a full chunk store waits for loaders to free
space. A real memory governor, which also scales the workers, is future work.
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.codec import ChunkCodec
from bitdrop_api.destination import Destination
from bitdrop_api.errors import ChunkNotFound, CorruptChunk, LeaseLost, StoreFull
from bitdrop_api.keys import validate_key
from bitdrop_api.metadata_store import (
    DEFAULT_MAX_ATTEMPTS,
    ChunkRecord,
    ChunkState,
    Claim,
    MetadataStore,
    Stage,
)
from bitdrop_api.source import ChunkSpec, Source, SourcePlan
from bitdrop_core.codec import ArrowIpcCodec

__all__ = ["TransferConfig", "TransferReport", "run_transfer"]

_LOG = logging.getLogger("bitdrop.transfer")

MIB = 1024 * 1024
MAX_JOB_ID_LENGTH = 64
"""Keeps ``<job_id>:<chunk_id>`` within the 200 character key limit."""

_VALUE_LIMIT_HEADROOM = 2
"""Chunks are sized to half a store's value limit: encoding adds overhead; sizes are estimates."""


@dataclass(frozen=True, slots=True)
class TransferConfig:
    """What to transfer and how hard to push."""

    job_id: str
    """Identifies the job in the metadata store and the destination ledger. Re-using it resumes."""

    source_table: str
    destination_table: str

    target_chunk_bytes: int = 64 * MIB
    """Desired size of one chunk. Shrunk if the chunk store has a smaller value limit."""

    split_column: str | None = None
    """Hint for sources that split by a key column."""

    extractors: int = 4
    """Concurrent chunk reads from the source."""

    loaders: int = 4
    """Concurrent chunk writes to the destination."""

    lease_seconds: float = 60.0
    """How long a claim is valid without a heartbeat."""

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    """Failures a chunk may suffer before it is given up on."""

    chunk_timeout: float = 600.0
    """Longest a single call to the source, destination, or chunk store may take; a stalled
    connection then counts as a failure of the chunk instead of hanging a worker forever."""

    backpressure_timeout: float = 600.0
    """Longest an extractor waits for room in a full chunk store before the chunk fails."""

    poll_interval: float = 0.05
    """How often idle workers and the master look for news."""

    reaper_interval: float = 1.0
    """How often leases of dead workers are voided."""

    def __post_init__(self) -> None:
        """Reject impossible settings early."""
        validate_key(self.job_id)
        if len(self.job_id) > MAX_JOB_ID_LENGTH:
            raise ValueError(f"job_id must be at most {MAX_JOB_ID_LENGTH} characters")
        for name in ("extractors", "loaders", "max_attempts"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1, got {getattr(self, name)}")
        for name in (
            "target_chunk_bytes",
            "lease_seconds",
            "chunk_timeout",
            "backpressure_timeout",
            "poll_interval",
            "reaper_interval",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive, got {getattr(self, name)}")


@dataclass(frozen=True, slots=True)
class TransferReport:
    """What a finished transfer did."""

    job_id: str
    chunks_total: int
    chunks_loaded: int
    chunks_failed: int
    rows_extracted: int
    """Rows read from the source by this run (chunks retried count again)."""

    rows_loaded: int
    """Rows newly committed to the destination by this run."""

    duplicates_skipped: int
    """Chunks the destination had already loaded (a retry after a crash, or a re-run)."""

    bytes_staged: int
    """Bytes put into the chunk store by this run."""

    duration_seconds: float
    failures: tuple[ChunkRecord, ...]
    """The chunks that failed for good, with their last error."""

    @property
    def succeeded(self) -> bool:
        """``True`` if every chunk is loaded."""
        return self.chunks_failed == 0 and self.chunks_loaded == self.chunks_total


@dataclass(slots=True)
class _Stats:
    rows_extracted: int = 0
    rows_loaded: int = 0
    duplicates_skipped: int = 0
    bytes_staged: int = 0


async def run_transfer(
    source: Source,
    destination: Destination,
    chunk_store: ChunkStore,
    metadata_store: MetadataStore,
    config: TransferConfig,
    *,
    codec: ChunkCodec | None = None,
) -> TransferReport:
    """Transfer ``config.source_table`` into ``config.destination_table`` and report the result.

    Chunks that fail for good do not abort the run; they are listed in the report. Planning errors
    (unknown table, unsupported column type) propagate before anything is moved.
    """
    started = time.monotonic()
    target = _effective_target(config, chunk_store)
    plan = await source.plan(
        config.source_table, target_chunk_bytes=target, split_column=config.split_column
    )
    for note in plan.notes:
        _LOG.warning("%s: %s", config.source_table, note)
    await destination.prepare(config.destination_table, plan.schema)
    await metadata_store.register_chunks(
        config.job_id,
        [chunk.chunk_id for chunk in plan.chunks],
        max_attempts=config.max_attempts,
    )
    _LOG.info(
        "job %s: %d chunks of about %d bytes from %s",
        config.job_id,
        len(plan.chunks),
        target,
        config.source_table,
    )

    transfer = _Transfer(
        source, destination, chunk_store, metadata_store, codec or ArrowIpcCodec(), config, plan
    )
    await transfer.run()
    report = await transfer.report(time.monotonic() - started)
    _LOG.info(
        "job %s: %d/%d chunks loaded, %d failed, %d rows in %.2fs",
        report.job_id,
        report.chunks_loaded,
        report.chunks_total,
        report.chunks_failed,
        report.rows_loaded,
        report.duration_seconds,
    )
    return report


def _effective_target(config: TransferConfig, chunk_store: ChunkStore) -> int:
    limit = chunk_store.capabilities().max_value_bytes
    if limit is None:
        return config.target_chunk_bytes
    return min(config.target_chunk_bytes, max(1, limit // _VALUE_LIMIT_HEADROOM))


class _Transfer:
    """The state shared by the master and its workers for one run."""

    def __init__(
        self,
        source: Source,
        destination: Destination,
        chunks: ChunkStore,
        meta: MetadataStore,
        codec: ChunkCodec,
        config: TransferConfig,
        plan: SourcePlan,
    ) -> None:
        self._source = source
        self._destination = destination
        self._chunks = chunks
        self._meta = meta
        self._codec = codec
        self._config = config
        self._plan = plan
        self._specs: dict[str, ChunkSpec] = {chunk.chunk_id: chunk for chunk in plan.chunks}
        self._stats = _Stats()

    # -- orchestration --------------------------------------------------------------------------

    async def run(self) -> None:
        """Run workers until every chunk is finished; re-raise if a worker crashes."""
        if not self._specs:
            return
        cfg = self._config
        workers = [
            *(
                asyncio.create_task(self._work(Stage.EXTRACT, f"extractor-{i}"))
                for i in range(cfg.extractors)
            ),
            *(
                asyncio.create_task(self._work(Stage.LOAD, f"loader-{i}"))
                for i in range(cfg.loaders)
            ),
            asyncio.create_task(self._reap()),
        ]
        supervisor = asyncio.create_task(self._wait_until_finished())
        try:
            done, _ = await asyncio.wait(
                {supervisor, *workers}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()  # workers never end normally: this re-raises a crash
        finally:
            for task in (supervisor, *workers):
                task.cancel()
            await asyncio.gather(supervisor, *workers, return_exceptions=True)

    async def report(self, duration_seconds: float) -> TransferReport:
        """Summarize the finished run and clean up the blobs of chunks that failed for good."""
        cfg = self._config
        counts = await self._meta.counts(cfg.job_id)
        failures: list[ChunkRecord] = []
        if counts[ChunkState.FAILED]:
            for chunk_id in self._specs:
                record = await self._meta.get_chunk(cfg.job_id, chunk_id)
                if record.state is ChunkState.FAILED:
                    failures.append(record)
                    with suppress(Exception):  # best effort: the job is over either way
                        await self._chunks.delete(self._blob_key(chunk_id))
        return TransferReport(
            job_id=cfg.job_id,
            chunks_total=len(self._specs),
            chunks_loaded=counts[ChunkState.LOADED],
            chunks_failed=counts[ChunkState.FAILED],
            rows_extracted=self._stats.rows_extracted,
            rows_loaded=self._stats.rows_loaded,
            duplicates_skipped=self._stats.duplicates_skipped,
            bytes_staged=self._stats.bytes_staged,
            duration_seconds=duration_seconds,
            failures=tuple(failures),
        )

    async def _wait_until_finished(self) -> None:
        while True:
            counts = await self._meta.counts(self._config.job_id)
            if all(counts[state] == 0 for state in ChunkState if not state.is_terminal):
                return
            await asyncio.sleep(self._config.poll_interval)

    async def _reap(self) -> None:
        while True:
            await self._meta.release_expired(self._config.job_id)
            await asyncio.sleep(self._config.reaper_interval)

    # -- workers --------------------------------------------------------------------------------

    async def _work(self, stage: Stage, name: str) -> None:
        """Claim chunks for ``stage`` forever; the master cancels the worker when the job ends."""
        cfg = self._config
        handler = self._extract if stage is Stage.EXTRACT else self._load
        while True:
            claim = await self._meta.claim(
                cfg.job_id, stage, worker_id=name, lease_seconds=cfg.lease_seconds
            )
            if claim is None:
                await asyncio.sleep(cfg.poll_interval)
                continue
            await self._process(claim, handler)

    async def _process(self, claim: Claim, handler: Callable[[Claim], Awaitable[None]]) -> None:
        """Run ``handler`` for the claim while keeping its lease alive; report any failure."""
        async with self._keep_alive(claim):
            try:
                await handler(claim)
            except LeaseLost:
                _LOG.info(
                    "%s: lease lost on chunk %s; the new owner takes over",
                    claim.worker_id,
                    claim.chunk_id,
                )
            except Exception as error:
                await self._fail(claim, error)

    async def _fail(self, claim: Claim, error: Exception) -> None:
        reason = (
            f"timed out after {self._config.chunk_timeout}s"
            if isinstance(error, TimeoutError)
            else f"{type(error).__name__}: {error}"
        )
        # A loader that finds the blob missing or unreadable needs the chunk extracted again.
        reextract = claim.stage is Stage.LOAD and isinstance(error, ChunkNotFound | CorruptChunk)
        _LOG.warning("chunk %s (%s) failed: %s", claim.chunk_id, claim.stage, reason)
        with suppress(LeaseLost):
            await self._meta.fail(claim, reason, reextract=reextract)

    @asynccontextmanager
    async def _keep_alive(self, claim: Claim) -> AsyncIterator[None]:
        """Renew the claim's lease in the background while the body runs."""
        lease = self._config.lease_seconds

        async def beat() -> None:
            while True:
                await asyncio.sleep(lease / 3)
                try:
                    await self._meta.heartbeat(claim, lease_seconds=lease)
                except LeaseLost:
                    return

        heartbeat = asyncio.create_task(beat())
        try:
            yield
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _extract(self, claim: Claim) -> None:
        cfg = self._config
        async with asyncio.timeout(cfg.chunk_timeout):
            table = await self._source.read_chunk(self._plan, self._specs[claim.chunk_id])
            data = await asyncio.to_thread(self._codec.encode, table)
        await self._stage(self._blob_key(claim.chunk_id), data)
        await self._meta.complete(claim)
        self._stats.rows_extracted += table.num_rows
        self._stats.bytes_staged += len(data)

    async def _stage(self, key: str, data: bytes) -> None:
        """Put the blob in the chunk store, waiting for loaders to free space if it is full."""
        deadline = time.monotonic() + self._config.backpressure_timeout
        while True:
            try:
                async with asyncio.timeout(self._config.chunk_timeout):  # a stalled connection
                    await self._chunks.put(key, data)
                return
            except StoreFull:
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(self._config.poll_interval)

    async def _load(self, claim: Claim) -> None:
        cfg = self._config
        key = self._blob_key(claim.chunk_id)
        async with asyncio.timeout(cfg.chunk_timeout):
            data = await self._chunks.get(key)
            table = await asyncio.to_thread(self._codec.decode, data)
            written = await self._destination.write_chunk(
                cfg.job_id, claim.chunk_id, cfg.destination_table, table
            )
        if written.already_loaded:
            self._stats.duplicates_skipped += 1
        else:
            self._stats.rows_loaded += written.rows
        # Free the RAM *before* recording the chunk as done. The data is safe in the destination
        # (its ledger will recognise a repeat), so the only cost of a crash between these two
        # steps is one re-extraction. The other order would strand the blob: nobody revisits a
        # loaded chunk.
        async with asyncio.timeout(cfg.chunk_timeout):
            await self._chunks.delete(key)
        await self._meta.complete(claim)

    def _blob_key(self, chunk_id: str) -> str:
        return f"{self._config.job_id}:{chunk_id}"
