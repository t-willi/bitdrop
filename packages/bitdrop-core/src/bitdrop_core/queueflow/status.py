"""Is the job finished? Read from the markers and the two queues; nothing is changed.

There is no master, so "finished" is derived rather than announced. The planner's markers say how
many chunks there are and whether every chunk id was published. The work queue holds the chunks
nobody has finished extracting, the file queue those nobody has finished loading. When the plan is
complete and both queues hold nothing waiting and nothing in flight, every chunk has been loaded or
set aside; the ones set aside are the dead letters.

The order of reading matters. A source container publishes a chunk's file message *before* it
acknowledges the work message, so a chunk is always in one of the queues. Counting the work queue
first and the file queue second therefore never misses a chunk in transit; the other way round
could (the chunk moves between the two reads) and report a job finished that is not.

The extracted and loaded counts are derived from the queues, assuming one work message per chunk,
and are approximate if a plan was published again.
"""

import asyncio
import json
import time
from dataclasses import dataclass
from enum import StrEnum

from bitdrop_api.chunk_queue import ChunkQueue, QueueStats
from bitdrop_api.marker_store import MarkerStore
from bitdrop_core.queueflow.messages import chunking_key, planned_key

__all__ = ["JobState", "QueueflowStatus", "queueflow_status", "wait_until_finished"]


class JobState(StrEnum):
    """Where the job stands."""

    NOT_PLANNED = "not planned"
    """The planner has not recorded anything for this job."""

    PLANNING = "planning"
    """The chunks are known, but the planner has not yet published all of them."""

    RUNNING = "running"
    """Every chunk id is published; chunks are still being extracted or loaded."""

    FINISHED = "finished"
    """Every chunk was loaded."""

    FINISHED_WITH_FAILURES = "finished with failures"
    """Nothing is left to do, but some chunks were set aside (see the dead letters)."""

    @property
    def is_over(self) -> bool:
        """``True`` once there is nothing left to do."""
        return self in (JobState.FINISHED, JobState.FINISHED_WITH_FAILURES)


@dataclass(frozen=True, slots=True)
class QueueflowStatus:
    """What the markers and the queues say about one job."""

    job_id: str
    state: JobState
    chunks_total: int | None
    """How many chunks the plan has, or ``None`` before the planner has recorded it."""

    work: QueueStats
    files: QueueStats

    @property
    def failed(self) -> int:
        """Chunks set aside, in either queue."""
        return self.work.dead + self.files.dead

    @property
    def extracted(self) -> int:
        """Chunks that left the work queue for good (staged, or set aside)."""
        if self.chunks_total is None:
            return 0
        left = self.work.waiting + self.work.in_flight + self.work.dead
        return max(0, self.chunks_total - left)

    @property
    def loaded(self) -> int:
        """Chunks that are extracted and no longer in the file queue."""
        left = self.files.waiting + self.files.in_flight + self.files.dead
        return max(0, self.extracted - left)


async def queueflow_status(
    markers: MarkerStore, work: ChunkQueue, files: ChunkQueue, job_id: str
) -> QueueflowStatus:
    """Read the job's state. The work queue is counted before the file queue; see the module."""
    chunking = await markers.get(chunking_key(job_id))
    planned = await markers.get(planned_key(job_id)) is not None
    work_stats = await work.stats()
    files_stats = await files.stats()

    total = _chunks_of(chunking)
    if chunking is None:
        state = JobState.NOT_PLANNED
    elif not planned:
        state = JobState.PLANNING
    elif _busy(work_stats) or _busy(files_stats):
        state = JobState.RUNNING
    elif work_stats.dead or files_stats.dead:
        state = JobState.FINISHED_WITH_FAILURES
    else:
        state = JobState.FINISHED
    return QueueflowStatus(job_id, state, total, work_stats, files_stats)


async def wait_until_finished(
    markers: MarkerStore,
    work: ChunkQueue,
    files: ChunkQueue,
    job_id: str,
    *,
    poll_interval: float = 1.0,
    give_up_after: float | None = None,
) -> QueueflowStatus:
    """Poll until the job is over and return its final status.

    Raises:
        TimeoutError: the job was not over after ``give_up_after`` seconds.
    """
    deadline = None if give_up_after is None else time.monotonic() + give_up_after
    while True:
        status = await queueflow_status(markers, work, files, job_id)
        if status.state.is_over:
            return status
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(f"job {job_id!r} was still {status.state} after {give_up_after} s")
        await asyncio.sleep(poll_interval)


def _busy(stats: QueueStats) -> bool:
    return stats.waiting > 0 or stats.in_flight > 0


def _chunks_of(chunking: bytes | None) -> int | None:
    if chunking is None:
        return None
    count = json.loads(chunking).get("chunks")
    return count if isinstance(count, int) and not isinstance(count, bool) else None
