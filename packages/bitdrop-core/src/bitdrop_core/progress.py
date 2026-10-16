"""Progress events: a window onto a running transfer.

``run_transfer`` accepts an *observer*, a plain function that receives the events below. A command
line tool prints them, a terminal UI draws them, a test records them. The observer only watches:

* It is called on the event loop, so it must return quickly and must not block.
* If it raises, the exception is logged (once) and swallowed; the transfer carries on and the data
  arrives exactly as it would without an observer.
* Events say what happened *in this process*. The chunk counts in a :class:`Snapshot` come from the
  metadata store and are authoritative; the per-chunk events are what a display uses to show who is
  doing what, and a lease voided by the reaper is not announced as an event.

The order of events for one chunk is ``ChunkStarted`` and ``ChunkFinished`` for extraction, then the
same pair for loading. A failed attempt ends in ``ChunkFailed`` instead, and the chunk is started
again unless the failure was ``final``. ``PlanReady`` comes first; the last event is a ``Snapshot``.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from bitdrop_api.metadata_store import ChunkState, Stage

__all__ = [
    "ChunkFailed",
    "ChunkFinished",
    "ChunkStarted",
    "Notifier",
    "PlanReady",
    "Snapshot",
    "TransferEvent",
    "TransferObserver",
]


@dataclass(frozen=True, slots=True)
class PlanReady:
    """The table was planned and its chunks were registered. Nothing has moved yet."""

    job_id: str
    source_table: str
    destination_table: str
    chunk_ids: tuple[str, ...]
    """Every chunk of the job, in plan order (a display lays out its chunk map from this)."""

    columns: tuple[tuple[str, str], ...]
    """``(name, Arrow type)`` of every column."""

    estimated_rows: int | None
    estimated_row_bytes: int | None
    notes: tuple[str, ...]
    """What the source wants the operator to know, e.g. "no integer primary key"."""

    extractors: int
    loaders: int
    max_attempts: int

    @property
    def chunks_total(self) -> int:
        """How many chunks the job has."""
        return len(self.chunk_ids)


@dataclass(frozen=True, slots=True)
class ChunkStarted:
    """A worker claimed a chunk and began extracting or loading it."""

    chunk_id: str
    stage: Stage
    worker: str


@dataclass(frozen=True, slots=True)
class ChunkFinished:
    """A worker finished one stage of a chunk."""

    chunk_id: str
    stage: Stage
    worker: str
    rows: int
    bytes: int
    """Size of the encoded chunk, as staged in the chunk store."""

    seconds: float
    """How long the worker held the chunk."""

    already_loaded: bool = False
    """Loading only: the destination had this chunk already (a repeat after a crash or re-run)."""


@dataclass(frozen=True, slots=True)
class ChunkFailed:
    """One attempt at a chunk failed."""

    chunk_id: str
    stage: Stage
    worker: str
    reason: str
    failures: int
    """How many attempts this chunk has failed so far, this one included."""

    final: bool
    """``True`` if the chunk used up its attempts and will not be tried again."""


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Where the job stands, published about every ``progress_interval`` and once at the end."""

    job_id: str
    elapsed_seconds: float
    counts: Mapping[ChunkState, int]
    """Chunks per state, from the metadata store."""

    rows_extracted: int
    rows_loaded: int
    duplicates_skipped: int
    bytes_staged: int
    """Total bytes put into the chunk store so far (not what it holds right now)."""

    staging_used_bytes: int | None
    """What the chunk store holds right now, or ``None`` if it cannot tell."""

    staging_max_bytes: int | None
    """The chunk store's capacity, or ``None`` if it is unbounded or unknown."""


TransferEvent = PlanReady | ChunkStarted | ChunkFinished | ChunkFailed | Snapshot
"""Everything an observer can receive."""

TransferObserver = Callable[[TransferEvent], None]
"""A function that watches a transfer. See the module docstring for its obligations."""


class Notifier:
    """Hands events to an observer and makes sure a broken observer cannot break the transfer."""

    def __init__(self, observer: TransferObserver | None, log: logging.Logger) -> None:
        """Wrap ``observer`` (or nothing); failures are logged to ``log``."""
        self._observer = observer
        self._log = log
        self._complained = False

    @property
    def enabled(self) -> bool:
        """``True`` if somebody is listening, so callers can skip building events."""
        return self._observer is not None

    def __call__(self, event: TransferEvent) -> None:
        """Deliver ``event``. Never raises."""
        if self._observer is None:
            return
        try:
            self._observer(event)
        except Exception:
            # Only the first failure is a warning: a display that crashes on every event must not
            # flood the log. The observer keeps being offered events; the problem may be transient.
            level = logging.DEBUG if self._complained else logging.WARNING
            self._complained = True
            self._log.log(level, "the progress observer raised; ignoring it", exc_info=True)
