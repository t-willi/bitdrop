"""Find and open components by URL.

The core never imports an adapter. Instead, each adapter package announces itself through a Python
*entry point* in the group ``bitdrop.chunk_stores``::

    [project.entry-points."bitdrop.chunk_stores"]
    memory = "bitdrop_store_memory:create_store"

The entry point's name is the URL scheme; its target is a factory ``(url: str) -> ChunkStore``.
Installing a package is all it takes to make its scheme available, so swapping a backend is a
configuration change (``memory://`` becomes ``file:///dev/shm/bitdrop``), not a code change.
"""

from collections.abc import Callable, Mapping
from importlib.metadata import entry_points
from typing import cast
from urllib.parse import urlsplit

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.errors import BitdropError

CHUNK_STORE_GROUP = "bitdrop.chunk_stores"
"""Entry point group under which chunk store adapters register."""

ChunkStoreFactory = Callable[[str], ChunkStore]
"""Builds a store from the full URL it was selected with."""


class UnknownComponent(BitdropError):
    """No installed adapter handles the URL's scheme."""

    def __init__(self, kind: str, scheme: str, available: list[str]) -> None:
        """Describe the unknown ``scheme`` of ``kind`` and what ``available`` instead."""
        detail = f"available: {', '.join(available)}" if available else f"no {kind}s are installed"
        super().__init__(f"unknown {kind} scheme {scheme!r} ({detail})")


class AmbiguousComponent(BitdropError):
    """Several installed adapters claim the same URL scheme."""

    def __init__(self, kind: str, scheme: str) -> None:
        """Describe the ``scheme`` that more than one ``kind`` adapter registered."""
        super().__init__(f"more than one {kind} adapter is installed for scheme {scheme!r}")


class InvalidComponent(BitdropError):
    """An adapter's factory returned an object that does not implement the contract."""

    def __init__(self, kind: str, scheme: str, got: object) -> None:
        """Describe the ``scheme`` whose factory returned ``got`` instead of a ``kind``."""
        super().__init__(
            f"the {kind} factory for scheme {scheme!r} returned {type(got).__name__}, "
            f"which does not implement the {kind} contract"
        )


def available_chunk_stores() -> list[str]:
    """Return the URL schemes of all installed chunk store adapters, sorted."""
    return sorted({entry_point.name for entry_point in entry_points(group=CHUNK_STORE_GROUP)})


def open_chunk_store(
    url: str, *, factories: Mapping[str, ChunkStoreFactory] | None = None
) -> ChunkStore:
    """Open the chunk store selected by ``url``.

    Args:
        url: ``scheme://...``. The scheme selects the adapter; the rest is its configuration.
        factories: use these scheme-to-factory mappings instead of the installed entry points.
            Meant for tests.

    Raises:
        ValueError: ``url`` has no scheme.
        UnknownComponent: no adapter handles the scheme.
        AmbiguousComponent: more than one installed adapter handles the scheme.
        InvalidComponent: the adapter's factory did not return a ChunkStore.
    """
    parts = urlsplit(url)
    if not parts.scheme or not url.startswith(f"{parts.scheme}://"):
        raise ValueError(f"expected a URL starting with a scheme like 'memory://', got {url!r}")

    factory = _find_factory(parts.scheme, factories)
    store = factory(url)
    if not isinstance(store, ChunkStore):
        raise InvalidComponent("chunk store", parts.scheme, store)
    return store


def _find_factory(
    scheme: str, factories: Mapping[str, ChunkStoreFactory] | None
) -> ChunkStoreFactory:
    """Look up the factory for ``scheme``, importing only the adapter that is needed."""
    if factories is not None:
        try:
            return factories[scheme]
        except KeyError:
            raise UnknownComponent("chunk store", scheme, sorted(factories)) from None

    matches = [e for e in entry_points(group=CHUNK_STORE_GROUP) if e.name == scheme]
    if len(matches) > 1:
        raise AmbiguousComponent("chunk store", scheme)
    if not matches:
        raise UnknownComponent("chunk store", scheme, available_chunk_stores())
    return cast(ChunkStoreFactory, matches[0].load())
