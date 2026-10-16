"""MetadataStore adapter keeping chunk states in this process's memory.

Useful for tests and single-process runs; nothing survives a restart. Select it with ``memory://``
(it takes no parameters).
"""

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from bitdrop_api.errors import LeaseLost, UnknownChunk
from bitdrop_api.keys import validate_key
from bitdrop_api.metadata_store import (
    DEFAULT_MAX_ATTEMPTS,
    ChunkRecord,
    ChunkState,
    Claim,
    Stage,
)
from bitdrop_api.urls import parse_url

__all__ = ["MemoryMetadataStore", "create_store"]


@dataclass(slots=True)
class _Chunk:
    """Mutable bookkeeping of one chunk."""

    state: ChunkState
    max_attempts: int
    failures: int = 0
    fence: int = 0
    worker_id: str | None = None
    lease_expires: float = 0.0
    last_error: str | None = None


@dataclass(slots=True)
class _Job:
    """All chunks of one job, indexed by state for O(1) claiming."""

    chunks: dict[str, _Chunk] = field(default_factory=dict)
    # An insertion-ordered dict used as an ordered set: the first key is the next to be claimed.
    by_state: dict[ChunkState, dict[str, None]] = field(
        default_factory=lambda: {state: {} for state in ChunkState}
    )

    def move(self, chunk_id: str, chunk: _Chunk, new_state: ChunkState) -> None:
        """Change a chunk's state and keep the index in sync."""
        del self.by_state[chunk.state][chunk_id]
        chunk.state = new_state
        self.by_state[new_state][chunk_id] = None


class MemoryMetadataStore:
    """A :class:`~bitdrop_api.metadata_store.MetadataStore` backed by plain dicts.

    Safe under asyncio: no method awaits while it changes state, so every operation is atomic with
    respect to other tasks.
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        """Create an empty store. ``clock`` (seconds) is injectable so tests can control time."""
        self._clock = clock
        self._jobs: dict[str, _Job] = {}

    async def register_chunks(
        self, job_id: str, chunk_ids: Sequence[str], *, max_attempts: int = DEFAULT_MAX_ATTEMPTS
    ) -> int:
        """Add chunks as ``PENDING``; see :meth:`MetadataStore.register_chunks`."""
        validate_key(job_id)
        for chunk_id in chunk_ids:  # validate everything first: a bad batch adds nothing
            validate_key(chunk_id)
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be at least 1, got {max_attempts}")

        job = self._jobs.setdefault(job_id, _Job())
        added = 0
        for chunk_id in chunk_ids:
            if chunk_id not in job.chunks:
                job.chunks[chunk_id] = _Chunk(ChunkState.PENDING, max_attempts)
                job.by_state[ChunkState.PENDING][chunk_id] = None
                added += 1
        return added

    async def claim(
        self, job_id: str, stage: Stage, *, worker_id: str, lease_seconds: float
    ) -> Claim | None:
        """Take one waiting chunk; see :meth:`MetadataStore.claim`."""
        validate_key(job_id)
        _check_lease(lease_seconds)
        job = self._jobs.get(job_id)
        if job is None or not job.by_state[stage.waiting]:
            return None

        chunk_id = next(iter(job.by_state[stage.waiting]))
        chunk = job.chunks[chunk_id]
        job.move(chunk_id, chunk, stage.in_flight)
        chunk.fence += 1
        chunk.worker_id = worker_id
        chunk.lease_expires = self._clock() + lease_seconds
        return Claim(job_id, chunk_id, stage, worker_id, chunk.fence)

    async def heartbeat(self, claim: Claim, *, lease_seconds: float) -> None:
        """Extend the lease; see :meth:`MetadataStore.heartbeat`."""
        _check_lease(lease_seconds)
        _, chunk = self._held(claim)
        chunk.lease_expires = self._clock() + lease_seconds

    async def complete(self, claim: Claim) -> None:
        """Finish the claimed stage; see :meth:`MetadataStore.complete`."""
        job, chunk = self._held(claim)
        job.move(claim.chunk_id, chunk, claim.stage.done)
        chunk.worker_id = None

    async def fail(self, claim: Claim, reason: str, *, reextract: bool = False) -> None:
        """Report a failure; see :meth:`MetadataStore.fail`."""
        job, chunk = self._held(claim)
        retry_state = ChunkState.PENDING if reextract else claim.stage.waiting
        self._record_failure(job, claim.chunk_id, chunk, reason, retry_state)

    async def release(self, claim: Claim) -> None:
        """Hand a claimed chunk back without a failure; see :meth:`MetadataStore.release`."""
        job, chunk = self._held(claim)
        job.move(claim.chunk_id, chunk, claim.stage.waiting)
        chunk.worker_id = None

    async def release_expired(self, job_id: str) -> int:
        """Void claims whose lease ran out; see :meth:`MetadataStore.release_expired`."""
        validate_key(job_id)
        job = self._jobs.get(job_id)
        if job is None:
            return 0

        now = self._clock()
        released = 0
        for stage in Stage:
            for chunk_id in list(job.by_state[stage.in_flight]):
                chunk = job.chunks[chunk_id]
                if chunk.lease_expires <= now:
                    self._record_failure(job, chunk_id, chunk, "lease expired", stage.waiting)
                    released += 1
        return released

    async def get_chunk(self, job_id: str, chunk_id: str) -> ChunkRecord:
        """Return one chunk's bookkeeping; see :meth:`MetadataStore.get_chunk`."""
        validate_key(job_id)
        validate_key(chunk_id)
        job = self._jobs.get(job_id)
        chunk = job.chunks.get(chunk_id) if job else None
        if chunk is None:
            raise UnknownChunk(job_id, chunk_id)
        return ChunkRecord(
            job_id=job_id,
            chunk_id=chunk_id,
            state=chunk.state,
            failures=chunk.failures,
            worker_id=chunk.worker_id,
            last_error=chunk.last_error,
        )

    async def counts(self, job_id: str) -> Mapping[ChunkState, int]:
        """Count chunks per state; see :meth:`MetadataStore.counts`."""
        validate_key(job_id)
        job = self._jobs.get(job_id)
        if job is None:
            return dict.fromkeys(ChunkState, 0)
        return {state: len(chunk_ids) for state, chunk_ids in job.by_state.items()}

    async def aclose(self) -> None:
        """Forget everything."""
        self._jobs.clear()

    def _held(self, claim: Claim) -> tuple[_Job, _Chunk]:
        """Return the chunk the claim refers to, or raise ``LeaseLost`` if it is no longer valid.

        A claim is valid while the chunk is still in the claimed stage's in-flight state *and* the
        fence matches: re-claiming bumps the fence, which locks out the previous holder.
        """
        job = self._jobs.get(claim.job_id)
        chunk = job.chunks.get(claim.chunk_id) if job else None
        if job is None or chunk is None:
            raise LeaseLost(claim)
        if chunk.state is not claim.stage.in_flight or chunk.fence != claim.fence:
            raise LeaseLost(claim)
        return job, chunk

    @staticmethod
    def _record_failure(
        job: _Job, chunk_id: str, chunk: _Chunk, reason: str, retry_state: ChunkState
    ) -> None:
        """Count a failure; retry in ``retry_state`` unless the attempts are used up."""
        chunk.failures += 1
        chunk.last_error = reason
        chunk.worker_id = None
        exhausted = chunk.failures >= chunk.max_attempts
        job.move(chunk_id, chunk, ChunkState.FAILED if exhausted else retry_state)


def _check_lease(lease_seconds: float) -> None:
    if lease_seconds <= 0:
        raise ValueError(f"lease_seconds must be positive, got {lease_seconds}")


def create_store(url: str) -> MemoryMetadataStore:
    """Build a store from a ``memory://`` URL. This is the plugin entry point."""
    parsed = parse_url(url, scheme="memory", allowed_params=())
    if parsed.path:
        raise ValueError(f"memory:// URLs take no path, found {parsed.path!r} in {url!r}")
    return MemoryMetadataStore()
