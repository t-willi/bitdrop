"""The whole design in miniature, assembled from URLs only.

Extractor and loader workers move chunks through a ChunkStore while a MetadataStore tracks them,
and a reaper recovers from a dead worker. Every combination of installed chunk store and metadata
store must deliver each chunk to the destination exactly once.

The worker loops here are test scaffolding. They show how the real master and workers will use the
contracts; when those exist they replace this code.
"""

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Callable
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.errors import ChunkNotFound, LeaseLost
from bitdrop_api.metadata_store import ChunkState, Claim, MetadataStore, Stage
from bitdrop_api.testing import make_payload
from bitdrop_core.registry import (
    available_metadata_stores,
    open_chunk_store,
    open_metadata_store,
)

if TYPE_CHECKING:
    from conftest import KvServer

JOB = "job-1"
CHUNK_COUNT = 40
WORKER_LEASE = 30.0
POLL = 0.002
PIPELINE_TIMEOUT = 10.0
"""A hung pipeline must fail the test, not the whole run."""

# One entry per installed adapter: how to build a working URL for it. Add a line with each adapter.
CHUNK_STORE_URLS: dict[str, Callable[[Path], str]] = {
    "memory": lambda tmp_path: "memory://",
    "file": lambda tmp_path: f"file://{tmp_path}/chunks",
}
METADATA_STORE_URLS: dict[str, Callable[[Path], str]] = {
    "memory": lambda tmp_path: "memory://",
    "sqlite": lambda tmp_path: f"sqlite://{tmp_path}/metadata.db",
}


def payload_of(chunk_id: str) -> bytes:
    """The 'source data' of a chunk: deterministic, so the sink can verify what it received."""
    return make_payload(32 * 1024, seed=int(chunk_id.removeprefix("c")))


def blob_key(chunk_id: str) -> str:
    return f"{JOB}:{chunk_id}"


class Destination:
    """Stands in for the real destination: remembers what was written, per chunk."""

    def __init__(self) -> None:
        self.writes: dict[str, list[bytes]] = defaultdict(list)

    def write(self, chunk_id: str, data: bytes) -> None:
        self.writes[chunk_id].append(sha256(data).digest())


async def is_finished(meta: MetadataStore) -> bool:
    """True when every chunk is in a terminal state."""
    counts = await meta.counts(JOB)
    return all(count == 0 for state, count in counts.items() if not state.is_terminal)


def writes_per_chunk(destination: Destination) -> dict[str, int]:
    return {chunk_id: len(writes) for chunk_id, writes in destination.writes.items()}


async def extractor(meta: MetadataStore, chunks: ChunkStore, name: str) -> None:
    while True:
        claim = await meta.claim(JOB, Stage.EXTRACT, worker_id=name, lease_seconds=WORKER_LEASE)
        if claim is None:
            counts = await meta.counts(JOB)
            if counts[ChunkState.PENDING] == 0 and counts[ChunkState.EXTRACTING] == 0:
                return
            await asyncio.sleep(POLL)
            continue
        await chunks.put(blob_key(claim.chunk_id), payload_of(claim.chunk_id))  # "read the source"
        await meta.complete(claim)


async def loader(
    meta: MetadataStore, chunks: ChunkStore, destination: Destination, name: str
) -> None:
    while True:
        claim = await meta.claim(JOB, Stage.LOAD, worker_id=name, lease_seconds=WORKER_LEASE)
        if claim is None:
            if await is_finished(meta):
                return
            await asyncio.sleep(POLL)
            continue
        data = await chunks.get(blob_key(claim.chunk_id))
        destination.write(claim.chunk_id, data)
        await meta.complete(claim)
        await chunks.delete(blob_key(claim.chunk_id))  # free the RAM as soon as it is safe


async def reaper(meta: MetadataStore, stop: asyncio.Event) -> None:
    """The master's housekeeping: put chunks of dead workers back into circulation."""
    while not stop.is_set():
        await meta.release_expired(JOB)
        await asyncio.sleep(0.01)


async def run_pipeline(meta: MetadataStore, chunks: ChunkStore) -> Destination:
    destination = Destination()
    stop = asyncio.Event()
    reaping = asyncio.create_task(reaper(meta, stop))
    async with asyncio.timeout(PIPELINE_TIMEOUT):
        await asyncio.gather(
            *(extractor(meta, chunks, f"extractor-{i}") for i in range(3)),
            *(loader(meta, chunks, destination, f"loader-{i}") for i in range(3)),
        )
    stop.set()
    await reaping
    return destination


@pytest.fixture(params=sorted(CHUNK_STORE_URLS))
async def chunks(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[ChunkStore]:
    store = open_chunk_store(CHUNK_STORE_URLS[request.param](tmp_path))
    yield store
    await store.aclose()


@pytest.fixture(params=sorted(METADATA_STORE_URLS))
async def meta(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[MetadataStore]:
    store = open_metadata_store(METADATA_STORE_URLS[request.param](tmp_path))
    yield store
    await store.aclose()


def test_every_installed_metadata_adapter_is_covered_by_this_test() -> None:
    """Fails when a new adapter is installed but not yet covered here."""
    assert set(available_metadata_stores()) == set(METADATA_STORE_URLS)


async def test_every_chunk_reaches_the_destination_exactly_once(
    meta: MetadataStore, chunks: ChunkStore
) -> None:
    chunk_ids = [f"c{i:03d}" for i in range(CHUNK_COUNT)]
    await meta.register_chunks(JOB, chunk_ids)

    destination = await run_pipeline(meta, chunks)

    assert writes_per_chunk(destination) == dict.fromkeys(chunk_ids, 1)
    for chunk_id in chunk_ids:
        assert destination.writes[chunk_id] == [sha256(payload_of(chunk_id)).digest()]
    assert (await meta.counts(JOB))[ChunkState.LOADED] == CHUNK_COUNT
    for chunk_id in chunk_ids:  # the RAM tier is empty again
        assert not await _exists(chunks, blob_key(chunk_id))


async def test_a_dead_extractor_does_not_lose_or_duplicate_its_chunk(
    meta: MetadataStore, chunks: ChunkStore
) -> None:
    chunk_ids = [f"c{i:03d}" for i in range(CHUNK_COUNT)]
    await meta.register_chunks(JOB, chunk_ids)
    dead_claim = await _claim_and_vanish(meta)  # claims a chunk, then never reports back

    destination = await run_pipeline(meta, chunks)

    assert writes_per_chunk(destination) == dict.fromkeys(chunk_ids, 1)
    assert (await meta.counts(JOB))[ChunkState.LOADED] == CHUNK_COUNT
    with pytest.raises(LeaseLost):  # the dead worker "wakes up" far too late
        await meta.complete(dead_claim)


async def _claim_and_vanish(meta: MetadataStore) -> Claim:
    """Simulate a crashed extractor: it holds a chunk on a short lease, then vanishes."""
    claim = await meta.claim(JOB, Stage.EXTRACT, worker_id="doomed", lease_seconds=0.02)
    assert claim is not None
    return claim


async def _exists(chunks: ChunkStore, key: str) -> bool:
    try:
        await chunks.get(key)
    except ChunkNotFound:
        return False
    return True


@pytest.mark.docker
async def test_the_pipeline_works_over_a_network_chunk_store(
    meta: MetadataStore, kv: KvServer
) -> None:
    """The same scenario with the chunks held by a real Redis-protocol server."""
    chunks = open_chunk_store(kv.url)
    try:
        await meta.register_chunks(JOB, [f"c{i:03d}" for i in range(CHUNK_COUNT)])
        dead_claim = await _claim_and_vanish(meta)

        destination = await run_pipeline(meta, chunks)

        assert set(writes_per_chunk(destination).values()) == {1}
        assert (await meta.counts(JOB))[ChunkState.LOADED] == CHUNK_COUNT
        with pytest.raises(LeaseLost):
            await meta.complete(dead_claim)
    finally:
        await chunks.aclose()
