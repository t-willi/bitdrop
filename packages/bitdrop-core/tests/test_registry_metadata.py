"""The core opens a MetadataStore from a URL, just like a ChunkStore, from its own namespace."""

from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

import pytest

from bitdrop_api.metadata_store import MetadataStore
from bitdrop_core import registry
from bitdrop_core.registry import (
    CHUNK_STORE_GROUP,
    METADATA_STORE_GROUP,
    AmbiguousComponent,
    InvalidComponent,
    UnknownComponent,
    available_chunk_stores,
    available_metadata_stores,
    open_chunk_store,
    open_metadata_store,
)


class StubMetadataStore:
    """The smallest object that satisfies the MetadataStore protocol. Remembers its URL."""

    def __init__(self, url: str) -> None:
        self.url = url

    async def register_chunks(self, *args: Any, **kwargs: Any) -> int:
        return 0

    async def claim(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def heartbeat(self, *args: Any, **kwargs: Any) -> None: ...
    async def complete(self, *args: Any, **kwargs: Any) -> None: ...
    async def fail(self, *args: Any, **kwargs: Any) -> None: ...

    async def release_expired(self, *args: Any, **kwargs: Any) -> int:
        return 0

    async def get_chunk(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def counts(self, *args: Any, **kwargs: Any) -> dict[Any, int]:
        return {}

    async def aclose(self) -> None: ...


def fake_entry_point(name: str, factory: Callable[[str], object]) -> SimpleNamespace:
    return SimpleNamespace(name=name, load=lambda: factory)


@pytest.fixture
def install_entry_points(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    """Make the registry see exactly these entry points, per group, instead of installed ones."""

    def install(**by_group: list[SimpleNamespace]) -> None:
        def fake_discovery(*, group: str) -> Iterable[Any]:
            return by_group.get(group.replace(".", "_"), [])

        monkeypatch.setattr(registry, "entry_points", fake_discovery)

    return install


class TestOpenMetadataStore:
    def test_the_factory_for_the_scheme_receives_the_whole_url(self) -> None:
        store = open_metadata_store("stub://", factories={"stub": StubMetadataStore})

        assert isinstance(store, StubMetadataStore)
        assert store.url == "stub://"

    def test_the_result_satisfies_the_protocol(self) -> None:
        store = open_metadata_store("stub://", factories={"stub": StubMetadataStore})

        assert isinstance(store, MetadataStore)

    def test_unknown_scheme_names_the_kind_and_lists_what_is_available(self) -> None:
        with pytest.raises(UnknownComponent, match=r"metadata store.*nope.*\bstub\b"):
            open_metadata_store("nope://", factories={"stub": StubMetadataStore})

    def test_urls_without_a_scheme_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="scheme"):
            open_metadata_store("just-a-name", factories={"stub": StubMetadataStore})

    def test_a_factory_that_returns_something_else_is_reported(self) -> None:
        with pytest.raises(InvalidComponent, match="metadata store"):
            open_metadata_store("stub://", factories={"stub": lambda url: object()})  # type: ignore[dict-item,return-value]


class TestDiscovery:
    def test_metadata_stores_are_found_in_their_own_entry_point_group(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(bitdrop_metadata_stores=[fake_entry_point("stub", StubMetadataStore)])

        assert isinstance(open_metadata_store("stub://"), StubMetadataStore)
        assert available_metadata_stores() == ["stub"]

    def test_the_two_kinds_have_separate_namespaces(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        """'memory' may exist as a chunk store *and* as a metadata store."""
        install_entry_points(
            bitdrop_chunk_stores=[fake_entry_point("memory", lambda url: "a chunk store")],
            bitdrop_metadata_stores=[fake_entry_point("memory", StubMetadataStore)],
        )

        assert isinstance(open_metadata_store("memory://"), StubMetadataStore)
        assert available_chunk_stores() == ["memory"]
        assert available_metadata_stores() == ["memory"]

    def test_a_chunk_store_scheme_is_not_a_metadata_store_scheme(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(
            bitdrop_chunk_stores=[fake_entry_point("only-chunks", lambda url: object())]
        )

        with pytest.raises(UnknownComponent, match="no metadata stores are installed"):
            open_metadata_store("only-chunks://")

    def test_two_adapters_claiming_one_scheme_is_an_error(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(
            bitdrop_metadata_stores=[
                fake_entry_point("stub", StubMetadataStore),
                fake_entry_point("stub", StubMetadataStore),
            ]
        )

        with pytest.raises(AmbiguousComponent, match="metadata store"):
            open_metadata_store("stub://")

    def test_the_group_names_are_the_documented_ones(self) -> None:
        assert (CHUNK_STORE_GROUP, METADATA_STORE_GROUP) == (
            "bitdrop.chunk_stores",
            "bitdrop.metadata_stores",
        )

    def test_opening_a_chunk_store_still_works(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        """Regression guard for the refactoring that introduced the second kind."""
        install_entry_points(
            bitdrop_chunk_stores=[fake_entry_point("stub", lambda url: _AnyChunkStore())]
        )

        assert isinstance(open_chunk_store("stub://"), _AnyChunkStore)


class _AnyChunkStore:
    async def put(self, key: str, value: bytes) -> None: ...

    async def get(self, key: str) -> bytes:
        return b""

    async def delete(self, key: str) -> None: ...

    def capabilities(self) -> Any:
        return None

    async def stats(self) -> None:
        return None

    async def aclose(self) -> None: ...
