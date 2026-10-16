"""How fast does each chunk store move chunks? (experiment H1 of docs/ideation.md)

Not part of the normal test run. Start it explicitly; it needs Docker for the network stores:

    uv run pytest benchmarks -s

Every store is driven through the same ``ChunkStore`` contract, so the numbers compare adapters,
not client code. For each chunk size and concurrency level the benchmark writes a fixed volume of
chunks (``put``), reads them all back (``get``), and removes them (``delete``).

Read the numbers with care: the servers run in Docker, on macOS that means inside a VM, and the
client and server share one machine. They show relative behaviour and the cost of the Python client,
not what a dedicated network would deliver.
"""

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.testing import MIB, make_payload
from bitdrop_core.registry import open_chunk_store

if TYPE_CHECKING:
    from conftest import KvServer

VOLUME = 128 * MIB
"""Bytes written per measurement."""

CHUNK_SIZES = (1 * MIB, 16 * MIB, 64 * MIB)
CONCURRENCY = (1, 8)


@dataclass(frozen=True)
class Measurement:
    store: str
    chunk_mib: int
    concurrency: int
    put_mib_s: float
    get_mib_s: float
    delete_ms: float


RESULTS: list[Measurement] = []


async def timed(
    store: ChunkStore, keys: list[str], concurrency: int, operation: str, payload: bytes
) -> float:
    """Run ``operation`` for every key with ``concurrency`` in flight; return elapsed seconds."""
    gate = asyncio.Semaphore(concurrency)

    async def one(key: str) -> None:
        async with gate:
            if operation == "put":
                await store.put(key, payload)
            elif operation == "get":
                value = await store.get(key)
                assert len(value) == len(payload)
            else:
                await store.delete(key)

    started = time.perf_counter()
    await asyncio.gather(*(one(key) for key in keys))
    return time.perf_counter() - started


async def measure(label: str, store: ChunkStore) -> None:
    for size in CHUNK_SIZES:
        payload = make_payload(size, seed=size)
        count = max(2, VOLUME // size)
        keys = [f"bench:{size}:{i}" for i in range(count)]
        for concurrency in CONCURRENCY:
            await timed(store, keys[:2], 2, "put", payload)  # warm up connections
            put = await timed(store, keys, concurrency, "put", payload)
            get = await timed(store, keys, concurrency, "get", payload)
            delete = await timed(store, keys, concurrency, "delete", payload)
            mib = count * size / MIB
            RESULTS.append(
                Measurement(label, size // MIB, concurrency, mib / put, mib / get, delete * 1000)
            )


@pytest.mark.parametrize("scheme", ["memory", "file"])
async def test_local_stores(scheme: str, tmp_path: Path) -> None:
    store = open_chunk_store("memory://" if scheme == "memory" else f"file://{tmp_path}/chunks")
    try:
        await measure(scheme, store)
    finally:
        await store.aclose()


@pytest.mark.docker
async def test_network_stores(kv: KvServer) -> None:
    store = open_chunk_store(kv.url)
    try:
        await measure(kv.flavor, store)
    finally:
        await store.aclose()


def test_zz_summary() -> None:
    """Runs last: prints one table with every measurement."""
    if not RESULTS:
        pytest.skip("no measurements were taken")
    header = f"{'store':<10} {'chunk':>7} {'conc':>5} {'put MiB/s':>11} {'get MiB/s':>11} {'delete ms':>10}"
    lines = ["", f"[benchmark] {VOLUME // MIB} MiB per measurement", header, "-" * len(header)]
    for m in RESULTS:
        lines.append(
            f"{m.store:<10} {m.chunk_mib:>5} MiB {m.concurrency:>4} {m.put_mib_s:>11,.0f} "
            f"{m.get_mib_s:>11,.0f} {m.delete_ms:>10,.1f}"
        )
    print("\n".join(lines))
