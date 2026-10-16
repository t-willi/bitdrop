"""The ``MetadataStore`` contract: who is doing what with which chunk.

This is the *coordination* port. It tracks every chunk through the pipeline::

    PENDING --claim(EXTRACT)--> EXTRACTING --complete--> EXTRACTED
    EXTRACTED --claim(LOAD)--> LOADING --complete--> LOADED
    any in-flight state --fail / lease expiry--> back to waiting, or FAILED when attempts run out
    any in-flight state --release--> back to waiting, no failure counted (a worker stops on purpose)

Why a domain-level contract instead of a generic compare-and-set key:value store? The central
operation, "give me one pending chunk", needs an index by state. Over a plain KV store every claim
would scan every chunk. Each adapter instead implements the operations below with its native
atomics (SQL transactions, Redis Lua, ...), and the conformance kit in :mod:`bitdrop_api.testing`
pins the semantics identically for all of them.

Besides the chunks, a store keeps one *plan* per job: the bytes that describe the schema and the
list of chunks (see :mod:`bitdrop_api.plan`). The store never looks inside them. Saving the plan
before registering the chunks lets a job resume from exactly the chunks it started with, and lets
workers in other processes read the same list.

Two mechanisms make crashes harmless:

* **Leases.** A claim is valid for a limited time and must be renewed with ``heartbeat``. If the
  worker dies, ``release_expired`` (run by the master) puts the chunk back.
* **Fencing.** Every claim carries a number that grows with each claim of that chunk. Once a chunk
  has been released and claimed again, the old holder's number is stale and all its calls fail with
  ``LeaseLost``, so a slow "zombie" worker can never overwrite the new owner's work.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

DEFAULT_MAX_ATTEMPTS = 3
"""How many failures (errors or expired leases) a chunk may suffer before it is ``FAILED``."""


class ChunkState(StrEnum):
    """Where a chunk is in the pipeline."""

    PENDING = "pending"
    """Registered, waiting for an extractor."""

    EXTRACTING = "extracting"
    """Claimed by an extractor, which is reading it from the source."""

    EXTRACTED = "extracted"
    """Its data sits in the chunk store, waiting for a loader."""

    LOADING = "loading"
    """Claimed by a loader, which is writing it to the destination."""

    LOADED = "loaded"
    """Committed to the destination. Terminal."""

    FAILED = "failed"
    """Gave up after too many failures. Terminal."""

    @property
    def is_terminal(self) -> bool:
        """``True`` if no further work happens in this state."""
        return self in (ChunkState.LOADED, ChunkState.FAILED)


class Stage(StrEnum):
    """One hop of the pipeline: a chunk waits, is worked on (in flight), and is done."""

    EXTRACT = "extract"
    LOAD = "load"

    @property
    def waiting(self) -> ChunkState:
        """The state in which chunks wait to be claimed for this stage."""
        return ChunkState.PENDING if self is Stage.EXTRACT else ChunkState.EXTRACTED

    @property
    def in_flight(self) -> ChunkState:
        """The state while a worker holds the chunk for this stage."""
        return ChunkState.EXTRACTING if self is Stage.EXTRACT else ChunkState.LOADING

    @property
    def done(self) -> ChunkState:
        """The state after the stage completed successfully."""
        return ChunkState.EXTRACTED if self is Stage.EXTRACT else ChunkState.LOADED


@dataclass(frozen=True, slots=True)
class Claim:
    """Proof that a worker holds a chunk. Pass it back to ``heartbeat``, ``complete``, ``fail``."""

    job_id: str
    chunk_id: str
    stage: Stage
    worker_id: str
    fence: int
    """Grows with every claim of this chunk. A stale number means the lease was lost."""


@dataclass(frozen=True, slots=True)
class ChunkRecord:
    """A snapshot of one chunk's bookkeeping."""

    job_id: str
    chunk_id: str
    state: ChunkState
    failures: int
    """How many times work on this chunk failed or its lease expired."""

    worker_id: str | None
    """The current holder while the chunk is in flight, otherwise ``None``."""

    last_error: str | None
    """The reason of the most recent failure, if any."""


@runtime_checkable
class MetadataStore(Protocol):
    """Tracks chunk states, leases, and retries for one or more jobs.

    Behaviour every implementation must provide (verified by the conformance kit):

    * Every operation is atomic. Concurrent ``claim`` calls never hand the same chunk to two
      workers.
    * Job ids and chunk ids follow :func:`bitdrop_api.keys.validate_key`; anything else raises
      ``InvalidKey``.
    * Jobs are isolated from each other.
    * The order in which waiting chunks are claimed is unspecified.
    """

    async def register_chunks(
        self, job_id: str, chunk_ids: Sequence[str], *, max_attempts: int = DEFAULT_MAX_ATTEMPTS
    ) -> int:
        """Add chunks to ``job_id`` as ``PENDING`` and return how many were newly added.

        Idempotent: a chunk id that is already registered is left exactly as it is (progress is
        never reset), so a restarted planner can safely register the same plan again.
        ``max_attempts`` applies to the chunks added by this call.

        Raises:
            InvalidKey: a job id or chunk id breaks the key rules.
            ValueError: ``max_attempts`` is less than 1.
        """
        ...

    async def save_plan(self, job_id: str, plan: bytes) -> bytes:
        """Keep ``plan`` as the plan of ``job_id`` unless the job has one; return the plan kept.

        The first save wins; a later save changes nothing and returns the plan already stored. Two
        masters that plan the same job at the same moment therefore end up with one plan, each
        using the bytes this call returns. The bytes are stored and returned exactly as given.
        Saving a plan registers no chunks.

        Raises:
            InvalidKey: ``job_id`` breaks the key rules.
        """
        ...

    async def load_plan(self, job_id: str) -> bytes | None:
        """Return the plan saved for ``job_id``, or ``None`` if there is none.

        Raises:
            InvalidKey: ``job_id`` breaks the key rules.
        """
        ...

    async def claim(
        self, job_id: str, stage: Stage, *, worker_id: str, lease_seconds: float
    ) -> Claim | None:
        """Take one chunk waiting for ``stage`` and hold it for ``lease_seconds``.

        Returns ``None`` if no chunk is waiting right now. That does not mean the job is finished:
        chunks may still be in flight, or on their way to this stage.

        Raises:
            InvalidKey: ``job_id`` breaks the key rules.
            ValueError: ``lease_seconds`` is not positive.
        """
        ...

    async def heartbeat(self, claim: Claim, *, lease_seconds: float) -> None:
        """Extend the lease to ``lease_seconds`` from now.

        Raises:
            LeaseLost: the claim is no longer valid.
            ValueError: ``lease_seconds`` is not positive.
        """
        ...

    async def complete(self, claim: Claim) -> None:
        """Mark the claimed stage as done: ``EXTRACTING -> EXTRACTED`` or ``LOADING -> LOADED``.

        Raises:
            LeaseLost: the claim is no longer valid (completing twice also lands here).
        """
        ...

    async def fail(self, claim: Claim, reason: str, *, reextract: bool = False) -> None:
        """Report that the work failed. Counts one failure against the chunk.

        The chunk returns to the start of the claimed stage, or becomes ``FAILED`` if it has used
        up its attempts. With ``reextract=True`` a *load* claim sends the chunk all the way back to
        ``PENDING``, for when its data vanished from the chunk store.

        Raises:
            LeaseLost: the claim is no longer valid.
        """
        ...

    async def release(self, claim: Claim) -> None:
        """Hand a claimed chunk back, unfinished, without counting a failure.

        For a worker that stops on purpose (a shutdown signal): the chunk returns to the start of
        the claimed stage, its failure count and last error stay as they were, and it can be claimed
        again at once instead of waiting for the lease to run out. The old claim is spent, as after
        ``complete`` or ``fail``.

        Raises:
            LeaseLost: the claim is no longer valid.
        """
        ...

    async def release_expired(self, job_id: str) -> int:
        """Void every claim of ``job_id`` whose lease ran out and return how many were voided.

        Each counts as a failure with the reason ``"lease expired"``. Run periodically by the
        master. A lease that has expired but was not yet released may still be completed.
        """
        ...

    async def get_chunk(self, job_id: str, chunk_id: str) -> ChunkRecord:
        """Return the bookkeeping of one chunk.

        Raises:
            UnknownChunk: the chunk was never registered for the job.
        """
        ...

    async def counts(self, job_id: str) -> Mapping[ChunkState, int]:
        """Return how many chunks of ``job_id`` are in each state, zeros included."""
        ...

    async def aclose(self) -> None:
        """Release connections and other resources. The store must not be used afterwards."""
        ...
