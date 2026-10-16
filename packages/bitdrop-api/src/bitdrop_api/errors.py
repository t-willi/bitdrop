"""Errors shared by all bitdrop components.

Adapters translate backend-specific failures (a Redis ``OOM`` reply, a full disk, ...) into these
types, so the core can react to *what happened* without knowing *which backend* it talked to.
"""


class BitdropError(Exception):
    """Base class of every error raised by bitdrop components."""


class ChunkStoreError(BitdropError):
    """Base class of errors raised by a :class:`~bitdrop_api.chunk_store.ChunkStore`."""

    def __init__(self, key: str, message: str) -> None:
        """Create the error for ``key`` with a human-readable ``message``."""
        super().__init__(f"{message} (key={key!r})")
        self.key = key


class InvalidKey(ChunkStoreError):
    """The key does not satisfy the key rules (see :func:`bitdrop_api.keys.validate_key`)."""

    def __init__(self, key: str, reason: str) -> None:
        """Create the error for ``key``; ``reason`` says which rule it breaks."""
        super().__init__(key, f"invalid key: {reason}")


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
