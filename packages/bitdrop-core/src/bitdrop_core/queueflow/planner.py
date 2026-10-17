"""The planner: analyze the table, cut it into chunks, and publish one chunk id per chunk.

It runs before any extraction and exits when it is done (``docs/decoupled-queue-workflow.md``,
section 7). It never reads rows. In this order, so that a crash at any point leaves something the
next run can finish:

1. Plan the table with the source. A table that does not exist fails here, with nothing recorded.
2. Record the job's settings, schema, and chunk-id fingerprint in the marker store, each written
   once. A second planner for the same job id finds what the first recorded: identical is fine,
   different is refused (:class:`~bitdrop_api.errors.ChunkingMismatch`), because the chunk ids are
   the destination's duplicate guard and one job must keep one set of them.
3. Create the destination table from the schema.
4. Publish one work message per chunk.
5. Write the "planned" marker, last. Its presence means every chunk id was published.

A finished plan is not published again (the chunks may be loaded already, and publishing them
would read and stage them for nothing) unless ``republish`` says the queue was lost.
"""

import hashlib
import json
from dataclasses import dataclass

from bitdrop_api.chunk_queue import ChunkQueue
from bitdrop_api.destination import Destination
from bitdrop_api.errors import ChunkingMismatch
from bitdrop_api.keys import validate_key
from bitdrop_api.marker_store import MarkerStore
from bitdrop_api.plan import encode_plan
from bitdrop_api.source import Source, SourcePlan
from bitdrop_core.queueflow.messages import (
    FORMAT,
    WorkMessage,
    chunking_key,
    encode_work,
    planned_key,
    schema_key,
    settings_key,
)

__all__ = ["PlannerConfig", "PlannerReport", "run_planner"]


@dataclass(frozen=True, slots=True)
class PlannerConfig:
    """What to plan."""

    job_id: str
    source_table: str
    destination_table: str
    target_chunk_bytes: int
    """The chunk size to aim for, already shrunk to what the chunk store can hold."""

    split_column: str | None = None

    def __post_init__(self) -> None:
        """Reject a job id that cannot be a marker key, and a chunk size that is not positive."""
        validate_key(self.job_id)
        if self.target_chunk_bytes < 1:
            raise ValueError(f"target_chunk_bytes must be positive, got {self.target_chunk_bytes}")


@dataclass(frozen=True, slots=True)
class PlannerReport:
    """What a planner run did."""

    job_id: str
    chunks_total: int
    estimated_rows: int | None
    already_planned: bool
    """The job was already fully planned; nothing was published."""


async def run_planner(
    source: Source,
    destination: Destination,
    work: ChunkQueue,
    markers: MarkerStore,
    config: PlannerConfig,
    *,
    republish: bool = False,
) -> PlannerReport:
    """Plan the job's table and publish its chunk ids to ``work``.

    ``republish`` publishes every chunk id again even if the job is fully planned, for a work queue
    that was lost; the repeats are harmless because chunk ids are the same and loading is
    idempotent.

    Raises:
        TableNotFound: the source table does not exist.
        ChunkingMismatch: the job was already chunked differently.
    """
    plan = await source.plan(
        config.source_table,
        target_chunk_bytes=config.target_chunk_bytes,
        split_column=config.split_column,
    )
    job = config.job_id
    await _record_once(markers, settings_key(job), _settings(config), job, "settings")
    await _record_once(markers, schema_key(job), _schema(plan), job, "schema")
    await _record_once(markers, chunking_key(job), _chunking(plan), job, "chunks")

    await destination.prepare(config.destination_table, plan.schema)

    planned = await markers.get(planned_key(job))
    if planned is not None and not republish:
        return PlannerReport(job, len(plan.chunks), plan.estimated_rows, already_planned=True)

    for chunk in plan.chunks:
        await work.publish(encode_work(WorkMessage(job, config.source_table, chunk.chunk_id)))
    marker = json.dumps({"format": FORMAT, "chunks": len(plan.chunks)}, sort_keys=True).encode()
    await markers.set_if_absent(planned_key(job), marker)
    return PlannerReport(job, len(plan.chunks), plan.estimated_rows, already_planned=False)


async def _record_once(markers: MarkerStore, key: str, value: bytes, job: str, what: str) -> None:
    """Write ``value`` under ``key`` unless a planner did; refuse if what is there differs."""
    if await markers.set_if_absent(key, value) != value:
        raise ChunkingMismatch(job, what)


def _settings(config: PlannerConfig) -> bytes:
    return json.dumps(
        {
            "format": FORMAT,
            "table": config.source_table,
            "target_chunk_bytes": config.target_chunk_bytes,
            "split_column": config.split_column,
        },
        sort_keys=True,
    ).encode()


def _schema(plan: SourcePlan) -> bytes:
    """The table name and Arrow schema (with the native type hints), as a plan with no chunks."""
    return encode_plan(SourcePlan(plan.table, plan.schema, ()))


def _chunking(plan: SourcePlan) -> bytes:
    """How many chunks there are and a fingerprint of their ids, in order."""
    digest = hashlib.sha256("\n".join(chunk.chunk_id for chunk in plan.chunks).encode()).hexdigest()
    return json.dumps(
        {"format": FORMAT, "chunks": len(plan.chunks), "sha256": digest}, sort_keys=True
    ).encode()
