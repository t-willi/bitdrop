"""Errors shared by all bitdrop components.

Adapters translate backend-specific failures (a Redis ``OOM`` reply, a full disk, ...) into these
types, so the core can react to *what happened* without knowing *which backend* it talked to.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bitdrop_api.metadata_store import Claim


class BitdropError(Exception):
    """Base class of every error raised by bitdrop components."""


class InvalidKey(BitdropError):
    """A key or identifier breaks the shared rule (see :func:`bitdrop_api.keys.validate_key`).

    Chunk store keys, job ids, and chunk ids all follow that one rule, so any store raises this.
    """

    def __init__(self, key: str, reason: str) -> None:
        """Create the error for ``key``; ``reason`` says which rule it breaks."""
        super().__init__(f"invalid key: {reason} (key={key!r})")
        self.key = key


class ChunkStoreError(BitdropError):
    """Base class of errors raised by a :class:`~bitdrop_api.chunk_store.ChunkStore`."""

    def __init__(self, key: str, message: str) -> None:
        """Create the error for ``key`` with a human-readable ``message``."""
        super().__init__(f"{message} (key={key!r})")
        self.key = key


class ChunkNotFound(ChunkStoreError):
    """No value is stored under the key (never written, deleted, or evicted)."""

    def __init__(self, key: str) -> None:
        """Create the error for the missing ``key``."""
        super().__init__(key, "chunk not found")


class ChunkTooLarge(ChunkStoreError):
    """The value exceeds the store's declared ``max_value_bytes``."""

    def __init__(self, key: str, size: int, limit: int) -> None:
        """Create the error for a value of ``size`` bytes against a ``limit`` in bytes."""
        super().__init__(key, f"chunk of {size} bytes exceeds the limit of {limit} bytes")
        self.size = size
        self.limit = limit


class StoreFull(ChunkStoreError):
    """The store has no capacity left. Callers should apply backpressure, not retry blindly."""

    def __init__(self, key: str) -> None:
        """Create the error for the ``key`` that could not be stored."""
        super().__init__(key, "store is full")


class MetadataStoreError(BitdropError):
    """Base class of errors raised by a :class:`~bitdrop_api.metadata_store.MetadataStore`."""


class LeaseLost(MetadataStoreError):
    """The claim is no longer valid: the chunk was released, re-claimed, or already finished.

    The holder must stop working on the chunk and discard its result. Another worker owns it now.
    """

    def __init__(self, claim: Claim) -> None:
        """Create the error for the stale ``claim``."""
        super().__init__(
            f"lease lost: {claim.stage} claim #{claim.fence} on chunk {claim.chunk_id!r} "
            f"of job {claim.job_id!r} (worker {claim.worker_id!r}) is no longer valid"
        )
        self.claim = claim


class UnknownChunk(MetadataStoreError):
    """The chunk was never registered for the job."""

    def __init__(self, job_id: str, chunk_id: str) -> None:
        """Create the error for ``chunk_id`` of ``job_id``."""
        super().__init__(f"unknown chunk {chunk_id!r} in job {job_id!r}")
        self.job_id = job_id
        self.chunk_id = chunk_id
