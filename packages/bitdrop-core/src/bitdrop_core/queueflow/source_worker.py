"""The source container: take a chunk id from the work queue, read exactly that chunk, stage it.

For each work message it turns the chunk id back into the chunk with ``Source.chunk_from_id`` (the
id says what to read), reads it with the schema the planner recorded, encodes it, puts the bytes in
the chunk store, and publishes a file message for the destination containers. The work message is
acknowledged only after that, so a container that dies midway leaves the chunk for another one, and
reading and staging a chunk twice is harmless (a staged chunk is overwritten; the destination's
ledger absorbs a repeated load).

A full chunk store makes the container wait (the message is handed back, uncounted) instead of
failing the chunk. A message that can never succeed (another job's, a chunk id the source does not
understand, a chunk too large for the store) is set aside at once.
"""

import asyncio
from dataclasses import dataclass, field

from bitdrop_api.chunk_queue import ChunkQueue
from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.codec import ChunkCodec
from bitdrop_api.errors import ChunkTooLarge, CorruptPlan, InvalidChunkId, InvalidKey, StoreFull
from bitdrop_api.marker_store import MarkerStore
from bitdrop_api.plan import decode_plan
from bitdrop_api.source import Source, SourcePlan
from bitdrop_core.codec import ArrowIpcCodec
from bitdrop_core.queueflow.consumer import ConsumerConfig, ConsumerStats, Defer, GiveUp, consume
from bitdrop_core.queueflow.messages import (
    CorruptMessage,
    FileMessage,
    decode_work,
    encode_file,
    schema_key,
)

__all__ = ["SourceWorkerConfig", "run_source_worker", "staging_key"]


@dataclass(frozen=True, slots=True)
class SourceWorkerConfig:
    """What a source container works on."""

    job_id: str
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)


def staging_key(job_id: str, chunk_id: str) -> str:
    """Where a chunk of a job is staged in the chunk store."""
    return f"{job_id}:{chunk_id}"


class _SourceHandler:
    """Does the work for one work message. One instance serves one container."""

    def __init__(
        self,
        source: Source,
        staging: ChunkStore,
        files: ChunkQueue,
        markers: MarkerStore,
        job_id: str,
        codec: ChunkCodec,
    ) -> None:
        self._source = source
        self._staging = staging
        self._files = files
        self._markers = markers
        self._job_id = job_id
        self._codec = codec
        self._plan: SourcePlan | None = None

    async def __call__(self, body: bytes) -> None:
        try:
            message = decode_work(body)
        except CorruptMessage as error:
            raise GiveUp(f"not a work message: {error}") from error
        if message.job != self._job_id:
            raise GiveUp(f"a message for job {message.job!r}, not {self._job_id!r}")
        plan = await self._schema()
        if message.table != plan.table:
            raise GiveUp(f"a message for table {message.table!r}, the job reads {plan.table!r}")
        try:
            chunk = self._source.chunk_from_id(message.table, message.chunk)
        except InvalidChunkId as error:
            raise GiveUp(str(error)) from error

        data = await self._source.read_chunk(plan, chunk)
        encoded = await asyncio.to_thread(self._codec.encode, data)
        key = staging_key(message.job, message.chunk)
        try:
            await self._staging.put(key, encoded)
        except StoreFull:
            raise Defer from None  # the loaders will free space; try again later
        except (ChunkTooLarge, InvalidKey) as error:
            raise GiveUp(f"cannot stage the chunk: {error}") from error
        await self._files.publish(
            encode_file(
                FileMessage(
                    message.job, message.table, message.chunk, key, data.num_rows, len(encoded)
                )
            )
        )

    async def _schema(self) -> SourcePlan:
        """The table and schema the planner recorded, as a plan with no chunks."""
        if self._plan is None:
            recorded = await self._markers.get(schema_key(self._job_id))
            if recorded is None:
                raise Defer  # the planner records the schema before it publishes anything
            try:
                self._plan = decode_plan(recorded)
            except CorruptPlan as error:
                raise GiveUp(f"the recorded schema is unreadable: {error}") from error
        return self._plan


async def run_source_worker(
    source: Source,
    staging: ChunkStore,
    work: ChunkQueue,
    files: ChunkQueue,
    markers: MarkerStore,
    config: SourceWorkerConfig,
    stop: asyncio.Event,
    *,
    name: str = "source-1",
    codec: ChunkCodec | None = None,
) -> ConsumerStats:
    """Run one source container until ``stop`` is set; return what it did.

    ``name`` identifies the container to the queue; give each container its own.
    """
    handler = _SourceHandler(
        source, staging, files, markers, config.job_id, codec or ArrowIpcCodec()
    )
    return await consume(work, name, handler, config.consumer, stop)
