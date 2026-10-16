"""ChunkStore adapter keeping chunks in this process's memory.

Useful for tests and single-process runs. Select it with ``memory://``; optional parameters:

* ``max_bytes``: total capacity. Beyond it ``put`` raises ``StoreFull``.
* ``max_value_bytes``: largest accepted value. Beyond it ``put`` raises ``ChunkTooLarge``.
"""

from bitdrop_api.chunk_store import Locality, StoreCapabilities, StoreStats
from bitdrop_api.errors import ChunkNotFound, ChunkTooLarge, StoreFull
from bitdrop_api.keys import validate_key
from bitdrop_api.urls import parse_url

__all__ = ["MemoryChunkStore", "create_store"]


class MemoryChunkStore:
    """A :class:`~bitdrop_api.chunk_store.ChunkStore` backed by a plain ``dict``.

    Safe under asyncio: no method awaits while it changes state, so every operation is atomic with
    respect to other tasks.
    """

    def __init__(self, *, max_bytes: int | None = None, max_value_bytes: int | None = None) -> None:
        """Create an empty store, optionally bounded in total size and/or per-value size."""
        self._max_bytes = max_bytes
        self._capabilities = StoreCapabilities(
            max_value_bytes=max_value_bytes, locality=Locality.NODE, memory_stats=True
        )
        self._values: dict[str, bytes] = {}
        self._used_bytes = 0

    async def put(self, key: str, value: bytes) -> None:
        """Store ``value`` under ``key``; see :meth:`ChunkStore.put`."""
        validate_key(key)
        limit = self._capabilities.max_value_bytes
        if limit is not None and len(value) > limit:
            raise ChunkTooLarge(key, len(value), limit)

        used_after = self._used_bytes - len(self._values.get(key, b"")) + len(value)
        if self._max_bytes is not None and used_after > self._max_bytes:
            raise StoreFull(key)

        self._values[key] = value
        self._used_bytes = used_after

    async def get(self, key: str) -> bytes:
        """Return the value under ``key``; see :meth:`ChunkStore.get`."""
        validate_key(key)
        try:
            return self._values[key]
        except KeyError:
            raise ChunkNotFound(key) from None

    async def delete(self, key: str) -> None:
        """Remove ``key`` if present; see :meth:`ChunkStore.delete`."""
        validate_key(key)
        removed = self._values.pop(key, None)
        if removed is not None:
            self._used_bytes -= len(removed)

    def capabilities(self) -> StoreCapabilities:
        """Return the static self-description of this store."""
        return self._capabilities

    async def stats(self) -> StoreStats:
        """Return the bytes currently held and the configured capacity."""
        return StoreStats(used_bytes=self._used_bytes, max_bytes=self._max_bytes)

    async def aclose(self) -> None:
        """Drop all chunks."""
        self._values.clear()
        self._used_bytes = 0


def create_store(url: str) -> MemoryChunkStore:
    """Build a store from a ``memory://`` URL. This is the plugin entry point."""
    parsed = parse_url(url, scheme="memory", allowed_params=("max_bytes", "max_value_bytes"))
    if parsed.path:
        raise ValueError(f"memory:// URLs take no path, found {parsed.path!r} in {url!r}")
    return MemoryChunkStore(
        max_bytes=parsed.int_param("max_bytes"),
        max_value_bytes=parsed.int_param("max_value_bytes"),
    )
