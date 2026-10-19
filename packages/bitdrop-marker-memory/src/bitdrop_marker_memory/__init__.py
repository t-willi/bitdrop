"""MarkerStore adapter keeping markers in this process's memory.

Useful for tests and single-process runs; nothing survives a restart, and separate stores opened
with ``memory://`` are independent of each other (it takes no parameters).
"""

from bitdrop_api.keys import validate_key
from bitdrop_api.urls import parse_url

__all__ = ["MemoryMarkerStore", "create_store"]


class MemoryMarkerStore:
    """A :class:`~bitdrop_api.marker_store.MarkerStore` backed by a dict.

    Safe under asyncio: no method awaits while it changes state, so ``set_if_absent`` is atomic
    with respect to other tasks.
    """

    def __init__(self) -> None:
        """Create a store with no markers."""
        self._markers: dict[str, bytes] = {}

    async def set_if_absent(self, key: str, value: bytes) -> bytes:
        """Store ``value`` unless ``key`` is set; return the value stored afterwards."""
        validate_key(key)
        return self._markers.setdefault(key, bytes(value))

    async def get(self, key: str) -> bytes | None:
        """Return the value under ``key``, or ``None``."""
        validate_key(key)
        return self._markers.get(key)

    async def aclose(self) -> None:
        """Forget every marker."""
        self._markers.clear()


def create_store(url: str) -> MemoryMarkerStore:
    """Build a store from a ``memory://`` URL. This is the plugin entry point."""
    parsed = parse_url(url, scheme="memory", allowed_params=())
    if parsed.path:
        raise ValueError(f"memory:// URLs take no path, found {parsed.path!r} in {url!r}")
    return MemoryMarkerStore()
