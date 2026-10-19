"""Does removing the master change the speed? (experiment H2 of docs/ideation.md, in memory)

Not part of the normal test run. Start it explicitly; it needs no Docker:

    uv run pytest benchmarks/test_queueflow_vs_runner.py -s

The same table is moved by the master-based runner (`run_transfer`) and by the queue workflow (a
planner, source containers, destination containers) with the same number of workers, all on the
in-memory adapters. So the numbers compare the *coordination* of the two designs, one metadata store
against two queues and a marker store, and nothing else: there is no network, no database and no
Redis. What a network adds is on `docs/verify-on-your-machine.md`.
"""

import asyncio
import time
from dataclasses import dataclass

import pytest

from bitdrop_api.testing import make_rows
from bitdrop_connector_memory import MemoryDestination, MemorySource
from bitdrop_core.queueflow import (
    ConsumerConfig,
    DestinationWorkerConfig,
    PlannerConfig,
    SourceWorkerConfig,
    run_destination_worker,
    run_planner,
    run_source_worker,
    wait_until_finished,
)
from bitdrop_core.transfer import TransferConfig, run_transfer
from bitdrop_marker_memory import MemoryMarkerStore
from bitdrop_metadata_memory import MemoryMetadataStore
from bitdrop_queue_memory import MemoryChunkQueue
from bitdrop_store_memory import MemoryChunkStore

ROWS = 400_000
ROWS_PER_CHUNK = 2_000
WORKERS = (1, 4)
JOB = "bench-1"


@dataclass(frozen=True)
class Result:
    design: str
    workers: int
    seconds: float

    @property
    def rows_per_second(self) -> float:
        return ROWS / self.seconds


RESULTS: list[Result] = []


def table_and_chunk_bytes() -> tuple[MemorySource, int]:
    table = make_rows(1, ROWS)
    return MemorySource({"people": table}), (table.nbytes // ROWS) * ROWS_PER_CHUNK


async def master_run(workers: int) -> float:
    source, chunk_bytes = table_and_chunk_bytes()
    destination = MemoryDestination()
    config = TransferConfig(
        job_id=JOB,
        source_table="people",
        destination_table="people_copy",
        target_chunk_bytes=chunk_bytes,
        extractors=workers,
        loaders=workers,
        poll_interval=0.005,
    )
    started = time.perf_counter()
    report = await run_transfer(
        source, destination, MemoryChunkStore(), MemoryMetadataStore(), config
    )
    elapsed = time.perf_counter() - started
    assert report.succeeded
    assert await destination.count_rows("people_copy") == ROWS
    return elapsed


async def queue_run(workers: int) -> float:
    source, chunk_bytes = table_and_chunk_bytes()
    destination, staging = MemoryDestination(), MemoryChunkStore()
    work, files, markers = MemoryChunkQueue(), MemoryChunkQueue(), MemoryMarkerStore()
    consumer = ConsumerConfig(poll_interval=0.005, idle_seconds=60)
    stop = asyncio.Event()
    started = time.perf_counter()
    await run_planner(
        source, destination, work, markers, PlannerConfig(JOB, "people", "people_copy", chunk_bytes)
    )
    tasks = [
        asyncio.create_task(
            run_source_worker(
                source,
                staging,
                work,
                files,
                markers,
                SourceWorkerConfig(JOB, consumer),
                stop,
                name=f"source-{i}",
            )
        )
        for i in range(workers)
    ] + [
        asyncio.create_task(
            run_destination_worker(
                destination,
                staging,
                files,
                DestinationWorkerConfig(JOB, "people_copy", consumer),
                stop,
                name=f"destination-{i}",
            )
        )
        for i in range(workers)
    ]
    status = await wait_until_finished(markers, work, files, JOB, poll_interval=0.005)
    elapsed = time.perf_counter() - started
    stop.set()
    await asyncio.gather(*tasks)
    assert status.state.is_over
    assert await destination.count_rows("people_copy") == ROWS
    return elapsed


@pytest.mark.parametrize("workers", WORKERS)
@pytest.mark.parametrize("design", ["master", "queues"])
async def test_move_the_table(design: str, workers: int) -> None:
    run = master_run if design == "master" else queue_run
    best = min([await run(workers) for _ in range(3)])  # the best of three, to shed noise
    RESULTS.append(Result(design, workers, best))


def test_zz_print_the_comparison() -> None:
    print(f"\n{ROWS:,} rows in chunks of {ROWS_PER_CHUNK:,}, in memory (best of three runs)")
    print(f"{'design':<8} {'workers':>7} {'seconds':>8} {'rows/s':>12}")
    for result in sorted(RESULTS, key=lambda r: (r.workers, r.design)):
        print(
            f"{result.design:<8} {result.workers:>7} {result.seconds:>8.2f} "
            f"{result.rows_per_second:>12,.0f}"
        )
