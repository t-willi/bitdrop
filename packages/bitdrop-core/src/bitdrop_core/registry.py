"""Find and open components by URL.

The core never imports an adapter. Instead, each adapter package announces itself through a Python
*entry point*, in one group per kind of component::

    [project.entry-points."bitdrop.chunk_stores"]
    memory = "bitdrop_store_memory:create_store"

    [project.entry-points."bitdrop.metadata_stores"]
    sqlite = "bitdrop_metadata_sqlite:create_store"

The entry point's name is the URL scheme; its target is a factory ``(url: str) -> <component>``.
Installing a package is all it takes to make its scheme available, so swapping a backend is a
configuration change (``memory://`` becomes ``file:///dev/shm/bitdrop``), not a code change. Each
kind has its own namespace: ``memory`` can be both a chunk store and a metadata store.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import cast
from urllib.parse import urlsplit

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.errors import BitdropError
from bitdrop_api.metadata_store import MetadataStore

CHUNK_STORE_GROUP = "bitdrop.chunk_stores"
"""Entry point group under which chunk store adapters register."""

METADATA_STORE_GROUP = "bitdrop.metadata_stores"
"""Entry point group under which metadata store adapters register."""

ChunkStoreFactory = Callable[[str], ChunkStore]
"""Builds a chunk store from the full URL it was selected with."""

MetadataStoreFactory = Callable[[str], MetadataStore]
"""Builds a metadata store from the full URL it was selected with."""


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


@dataclass(frozen=True, slots=True)
class _Kind:
    """One kind of pluggable component."""

    label: str
    """Human-readable name, used in error messages."""

    group: str
    """Entry point group its adapters register under."""

    contract: type
    """The runtime-checkable protocol a factory's result must satisfy."""


_CHUNK_STORES = _Kind("chunk store", CHUNK_STORE_GROUP, ChunkStore)
_METADATA_STORES = _Kind("metadata store", METADATA_STORE_GROUP, MetadataStore)


def available_chunk_stores() -> list[str]:
    """Return the URL schemes of all installed chunk store adapters, sorted."""
    return _available(_CHUNK_STORES)


def available_metadata_stores() -> list[str]:
    """Return the URL schemes of all installed metadata store adapters, sorted."""
    return _available(_METADATA_STORES)


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
    return cast(ChunkStore, _open(_CHUNK_STORES, url, factories))


def open_metadata_store(
    url: str, *, factories: Mapping[str, MetadataStoreFactory] | None = None
) -> MetadataStore:
    """Open the metadata store selected by ``url``.

    Arguments and errors are those of :func:`open_chunk_store`.
    """
    return cast(MetadataStore, _open(_METADATA_STORES, url, factories))


def _available(kind: _Kind) -> list[str]:
    return sorted({entry_point.name for entry_point in entry_points(group=kind.group)})


def _open(kind: _Kind, url: str, factories: Mapping[str, Callable[[str], object]] | None) -> object:
    """Resolve the URL's scheme to a factory, call it, and check the result's contract."""
    parts = urlsplit(url)
    if not parts.scheme or not url.startswith(f"{parts.scheme}://"):
        raise ValueError(f"expected a URL starting with a scheme like 'memory://', got {url!r}")

    component = _find_factory(kind, parts.scheme, factories)(url)
    if not isinstance(component, kind.contract):
        raise InvalidComponent(kind.label, parts.scheme, component)
    return component


def _find_factory(
    kind: _Kind, scheme: str, factories: Mapping[str, Callable[[str], object]] | None
) -> Callable[[str], object]:
    """Look up the factory for ``scheme``, importing only the adapter that is needed."""
    if factories is not None:
        try:
            return factories[scheme]
        except KeyError:
            raise UnknownComponent(kind.label, scheme, sorted(factories)) from None

    matches = [e for e in entry_points(group=kind.group) if e.name == scheme]
    if len(matches) > 1:
        raise AmbiguousComponent(kind.label, scheme)
    if not matches:
        raise UnknownComponent(kind.label, scheme, _available(kind))
    return cast(Callable[[str], object], matches[0].load())
