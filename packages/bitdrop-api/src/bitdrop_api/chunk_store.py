"""The ``ChunkStore`` contract: where chunks wait between extraction and loading.

This is the *data plane* port. It is deliberately tiny (put / get / delete) so that any in-memory
key:value store can implement it. Coordination (who works on which chunk) belongs to the metadata
store, never to the chunk store. See ``docs/principles.md``.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable


class Locality(StrEnum):
    """How far apart the extractors and loaders of one store may be placed.

    The planner uses this to place workers; the core never special-cases a backend.
    """

    POD = "pod"
    """Only containers in the same pod can reach it (e.g. a memory-backed ``emptyDir``)."""

    NODE = "node"
    """Only processes on the same machine can reach it."""

    CLUSTER = "cluster"
    """Reachable over the network from anywhere in the cluster (e.g. Redis, memcached)."""


@dataclass(frozen=True, slots=True)
class StoreCapabilities:
    """What a store declares about itself. Static: it does not change while the store is open."""

    max_value_bytes: int | None = None
    """Largest value ``put`` accepts, or ``None`` for no practical limit."""

    locality: Locality = Locality.CLUSTER
    """See :class:`Locality`."""

    memory_stats: bool = False
    """``True`` if :meth:`ChunkStore.stats` returns real numbers instead of ``None``."""

    def __post_init__(self) -> None:
        """Reject impossible declarations early."""
        if self.max_value_bytes is not None and self.max_value_bytes <= 0:
            raise ValueError(f"max_value_bytes must be positive, got {self.max_value_bytes}")


@dataclass(frozen=True, slots=True)
class StoreStats:
    """A snapshot of a store's memory usage. Advisory: the memory governor's own accounting wins."""

    used_bytes: int
    """Bytes currently held (at least the sum of stored values; backends may add overhead)."""

    max_bytes: int | None = None
    """Configured capacity, or ``None`` if the backend does not bound it."""

    def __post_init__(self) -> None:
        """Reject impossible values early."""
        if self.used_bytes < 0:
            raise ValueError(f"used_bytes must not be negative, got {self.used_bytes}")
        if self.max_bytes is not None and self.max_bytes <= 0:
            raise ValueError(f"max_bytes must be positive, got {self.max_bytes}")


@runtime_checkable
class ChunkStore(Protocol):
    """A key:value blob store holding chunks in transit.

    Behaviour every implementation must provide (verified by the conformance kit in
    :mod:`bitdrop_api.testing`):

    * Keys follow :func:`bitdrop_api.keys.validate_key`; anything else raises ``InvalidKey``.
    * ``put`` overwrites. Concurrent writers to one key leave exactly one complete value, never a
      mix.
    * ``get`` of an absent key raises ``ChunkNotFound``. Absence is a *normal* outcome (eviction,
      restart): callers reset the chunk and re-extract. Wrong or partial data is never acceptable.
    * ``delete`` is idempotent.
    * When full, ``put`` raises ``StoreFull`` (or the store may later report ``ChunkNotFound`` for
      evicted keys). It never silently corrupts data.
    """

    async def put(self, key: str, value: bytes) -> None:
        """Store ``value`` under ``key``, replacing any previous value.

        Raises:
            InvalidKey: ``key`` breaks the key rules.
            ChunkTooLarge: ``value`` exceeds ``capabilities().max_value_bytes``.
            StoreFull: the store has no capacity left.
        """
        ...

    async def get(self, key: str) -> bytes:
        """Return the value stored under ``key``.

        Raises:
            InvalidKey: ``key`` breaks the key rules.
            ChunkNotFound: nothing is stored under ``key``.
        """
        ...

    async def delete(self, key: str) -> None:
        """Remove ``key``. Does nothing if it is absent.

        Raises:
            InvalidKey: ``key`` breaks the key rules.
        """
        ...

    def capabilities(self) -> StoreCapabilities:
        """Return the store's static self-description."""
        ...

    async def stats(self) -> StoreStats | None:
        """Return current memory usage, or ``None`` if ``capabilities().memory_stats`` is false."""
        ...

    async def aclose(self) -> None:
        """Release connections and other resources. The store must not be used afterwards."""
        ...
