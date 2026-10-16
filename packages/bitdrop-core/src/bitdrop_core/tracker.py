"""Fold the stream of progress events into what a display shows.

A :class:`JobTracker` is the model behind every progress display, the plain text one and the TUI
alike: feed it each event with :meth:`JobTracker.apply` and read the totals, rates, estimate, chunk
map, and worker activity from its properties. It is pure. Time comes from the ``elapsed_seconds``
of the snapshots, never from a clock, so what it computes is exact and easy to test.

The chunk counts come from the latest :class:`~bitdrop_core.progress.Snapshot` and are
authoritative. The per-chunk map and the worker table are built from the individual chunk events, so
a lease that the reaper voided shows up in them only when the chunk is started again.
"""

from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from bitdrop_api.metadata_store import ChunkState, Stage
from bitdrop_core.progress import (
    ChunkFailed,
    ChunkFinished,
    ChunkStarted,
    PlanReady,
    Snapshot,
    TransferEvent,
)

__all__ = ["JobTracker", "WorkerActivity"]


@dataclass(frozen=True, slots=True)
class WorkerActivity:
    """What a busy worker is doing."""

    chunk_id: str
    stage: Stage
    since_seconds: float
    """Job time (see ``elapsed_seconds``) at which the worker started on the chunk."""


@dataclass(frozen=True, slots=True)
class _Sample:
    elapsed: float
    rows_extracted: int
    rows_loaded: int
    chunks_done: int


class JobTracker:
    """The state of one running job, as far as the events tell."""

    def __init__(
        self, *, rate_window: float = 5.0, history: int = 120, max_failures: int = 200
    ) -> None:
        """Create an empty tracker.

        ``rate_window`` is how many seconds back rates and the estimate look, ``history`` how many
        rate samples the sparklines keep, ``max_failures`` how many recent failures are remembered.
        """
        self._rate_window = rate_window
        self._plan: PlanReady | None = None
        self._snapshot: Snapshot | None = None
        self._states: list[ChunkState] = []
        self._index: dict[str, int] = {}
        self._workers: dict[str, WorkerActivity | None] = {}
        self._failures: deque[ChunkFailed] = deque(maxlen=max_failures)
        self._samples: deque[_Sample] = deque()
        self._extract_history: deque[float] = deque(maxlen=history)
        self._load_history: deque[float] = deque(maxlen=history)
        self._extract_rate = 0.0
        self._load_rate = 0.0
        self._chunk_rate = 0.0

    # -- feeding --------------------------------------------------------------------------------

    def apply(self, event: TransferEvent) -> None:
        """Update the state with one event. Events about unknown chunks are ignored."""
        match event:
            case PlanReady():
                self._start(event)
            case ChunkStarted():
                self._started(event)
            case ChunkFinished():
                self._finished(event)
            case ChunkFailed():
                self._failed(event)
            case Snapshot():
                self._measure(event)

    # -- the plan and the chunk map -------------------------------------------------------------

    @property
    def plan(self) -> PlanReady | None:
        """The announced plan, or ``None`` before the job is planned."""
        return self._plan

    @property
    def chunk_states(self) -> Sequence[ChunkState]:
        """The state of every chunk in plan order. Read-only: do not modify the returned list."""
        return self._states

    def state_of(self, chunk_id: str) -> ChunkState:
        """Return one chunk's state. Raises ``KeyError`` for a chunk outside the plan."""
        return self._states[self._index[chunk_id]]

    @property
    def workers(self) -> Mapping[str, WorkerActivity | None]:
        """Every worker seen so far in name order; ``None`` means idle."""
        return self._workers

    @property
    def failures(self) -> Sequence[ChunkFailed]:
        """The most recent failed attempts, oldest first."""
        return self._failures

    # -- totals ---------------------------------------------------------------------------------

    @property
    def chunks_total(self) -> int:
        """How many chunks the job has."""
        return len(self._states)

    @property
    def chunks_loaded(self) -> int:
        """Chunks committed to the destination."""
        return self._count(ChunkState.LOADED)

    @property
    def chunks_failed(self) -> int:
        """Chunks that gave up."""
        return self._count(ChunkState.FAILED)

    @property
    def fraction_done(self) -> float:
        """Share of chunks that reached an end state (loaded or given up), from 0 to 1."""
        total = self.chunks_total
        return self._chunks_done / total if total else 0.0

    @property
    def is_finished(self) -> bool:
        """``True`` once every chunk has reached an end state."""
        return self._plan is not None and self._chunks_done >= self.chunks_total

    @property
    def elapsed_seconds(self) -> float:
        """Job time at the latest snapshot."""
        return self._snapshot.elapsed_seconds if self._snapshot else 0.0

    @property
    def rows_extracted(self) -> int:
        """Rows read from the source so far."""
        return self._snapshot.rows_extracted if self._snapshot else 0

    @property
    def rows_loaded(self) -> int:
        """Rows committed to the destination so far."""
        return self._snapshot.rows_loaded if self._snapshot else 0

    @property
    def bytes_staged(self) -> int:
        """Bytes put into the chunk store so far."""
        return self._snapshot.bytes_staged if self._snapshot else 0

    @property
    def duplicates_skipped(self) -> int:
        """Chunks the destination already had."""
        return self._snapshot.duplicates_skipped if self._snapshot else 0

    @property
    def staging_used(self) -> int | None:
        """What the chunk store holds, if it can tell."""
        return self._snapshot.staging_used_bytes if self._snapshot else None

    @property
    def staging_capacity(self) -> int | None:
        """The chunk store's capacity, if it has a bound."""
        return self._snapshot.staging_max_bytes if self._snapshot else None

    @property
    def staging_fraction(self) -> float | None:
        """How full the chunk store is, from 0 to 1, or ``None`` if unknown."""
        used, capacity = self.staging_used, self.staging_capacity
        if used is None or not capacity:
            return None
        return used / capacity

    # -- rates ----------------------------------------------------------------------------------

    @property
    def extract_rate(self) -> float:
        """Rows per second read from the source over the recent window."""
        return self._extract_rate

    @property
    def load_rate(self) -> float:
        """Rows per second committed to the destination over the recent window."""
        return self._load_rate

    @property
    def extract_history(self) -> Sequence[float]:
        """The extraction rate at each snapshot, oldest first (for a sparkline)."""
        return self._extract_history

    @property
    def load_history(self) -> Sequence[float]:
        """The loading rate at each snapshot, oldest first (for a sparkline)."""
        return self._load_history

    @property
    def eta_seconds(self) -> float | None:
        """Seconds until the job ends at the recent chunk rate, or ``None`` if it cannot tell."""
        if self._plan is None:
            return None
        remaining = self.chunks_total - self._chunks_done
        if remaining <= 0:
            return 0.0
        if self._chunk_rate <= 0:
            return None
        return remaining / self._chunk_rate

    # -- internals ------------------------------------------------------------------------------

    @property
    def _chunks_done(self) -> int:
        return self.chunks_loaded + self.chunks_failed

    def _count(self, state: ChunkState) -> int:
        return self._snapshot.counts[state] if self._snapshot else 0

    def _start(self, plan: PlanReady) -> None:
        self._plan = plan
        self._states = [ChunkState.PENDING] * len(plan.chunk_ids)
        self._index = {chunk_id: i for i, chunk_id in enumerate(plan.chunk_ids)}

    def _set(self, chunk_id: str, state: ChunkState) -> None:
        position = self._index.get(chunk_id)
        if position is not None:
            self._states[position] = state

    def _started(self, event: ChunkStarted) -> None:
        self._set(event.chunk_id, event.stage.in_flight)
        if event.worker not in self._workers:
            self._workers[event.worker] = None
            self._workers = dict(sorted(self._workers.items()))
        self._workers[event.worker] = WorkerActivity(
            event.chunk_id, event.stage, self.elapsed_seconds
        )

    def _finished(self, event: ChunkFinished) -> None:
        self._set(event.chunk_id, event.stage.done)
        self._workers[event.worker] = None

    def _failed(self, event: ChunkFailed) -> None:
        self._set(event.chunk_id, ChunkState.FAILED if event.final else event.stage.waiting)
        self._workers[event.worker] = None
        self._failures.append(event)

    def _measure(self, snapshot: Snapshot) -> None:
        self._snapshot = snapshot
        self._samples.append(
            _Sample(
                snapshot.elapsed_seconds,
                snapshot.rows_extracted,
                snapshot.rows_loaded,
                self._chunks_done,
            )
        )
        cutoff = snapshot.elapsed_seconds - self._rate_window
        while len(self._samples) > 2 and self._samples[0].elapsed < cutoff:
            self._samples.popleft()

        first, last = self._samples[0], self._samples[-1]
        seconds = last.elapsed - first.elapsed
        if seconds > 0:
            self._extract_rate = (last.rows_extracted - first.rows_extracted) / seconds
            self._load_rate = (last.rows_loaded - first.rows_loaded) / seconds
            self._chunk_rate = (last.chunks_done - first.chunks_done) / seconds
        else:
            self._extract_rate = self._load_rate = self._chunk_rate = 0.0
        self._extract_history.append(self._extract_rate)
        self._load_history.append(self._load_rate)
