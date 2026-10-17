"""The destination container: take a staged chunk's name from the file queue and load it.

For each file message it fetches the chunk from the chunk store, decodes it, and writes it to the
destination, which records the chunk in its ledger in the same transaction, so a load repeated
after a crash is a no-op. The file message is acknowledged after the write, and only then is the
staged chunk deleted. That order matters: if the container died between deleting and
acknowledging, the next one would find the chunk missing and could not tell "loaded already" from
"lost". Deleting last means a missing chunk really is lost, and it is set aside where an operator
sees it (and ``republish`` of the plan brings it back). A crash between the acknowledgement and the
delete leaves a staged chunk behind, which costs RAM but never data.
"""

import asyncio
from dataclasses import dataclass, field

from bitdrop_api.chunk_queue import ChunkQueue
from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.codec import ChunkCodec
from bitdrop_api.destination import Destination
from bitdrop_api.errors import ChunkNotFound, CorruptChunk
from bitdrop_core.codec import ArrowIpcCodec
from bitdrop_core.queueflow.consumer import (
    Cleanup,
    ConsumerConfig,
    ConsumerStats,
    GiveUp,
    consume,
)
from bitdrop_core.queueflow.messages import CorruptMessage, decode_file

__all__ = ["DestinationWorkerConfig", "run_destination_worker"]


@dataclass(frozen=True, slots=True)
class DestinationWorkerConfig:
    """What a destination container works on."""

    job_id: str
    destination_table: str
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)


class _DestinationHandler:
    """Does the work for one file message. One instance serves one container."""

    def __init__(
        self,
        destination: Destination,
        staging: ChunkStore,
        config: DestinationWorkerConfig,
        codec: ChunkCodec,
    ) -> None:
        self._destination = destination
        self._staging = staging
        self._config = config
        self._codec = codec

    async def __call__(self, body: bytes) -> Cleanup:
        try:
            message = decode_file(body)
        except CorruptMessage as error:
            raise GiveUp(f"not a file message: {error}") from error
        if message.job != self._config.job_id:
            raise GiveUp(f"a message for job {message.job!r}, not {self._config.job_id!r}")
        try:
            encoded = await self._staging.get(message.key)
        except ChunkNotFound:
            raise GiveUp(
                f"the staged chunk {message.key!r} is gone; publish the plan again to extract it"
            ) from None
        try:
            data = await asyncio.to_thread(self._codec.decode, encoded)
        except CorruptChunk as error:
            raise GiveUp(f"the staged chunk {message.key!r} is corrupt: {error}") from error

        await self._destination.write_chunk(
            message.job, message.chunk, self._config.destination_table, data
        )

        async def delete_staged_chunk() -> None:
            await self._staging.delete(message.key)

        return delete_staged_chunk


async def run_destination_worker(
    destination: Destination,
    staging: ChunkStore,
    files: ChunkQueue,
    config: DestinationWorkerConfig,
    stop: asyncio.Event,
    *,
    name: str = "destination-1",
    codec: ChunkCodec | None = None,
) -> ConsumerStats:
    """Run one destination container until ``stop`` is set; return what it did.

    ``name`` identifies the container to the queue; give each container its own.
    """
    handler = _DestinationHandler(destination, staging, config, codec or ArrowIpcCodec())
    return await consume(files, name, handler, config.consumer, stop)
