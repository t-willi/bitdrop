"""The core opens a ChunkStore from a URL, knowing nothing about concrete backends."""

from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

import pytest

from bitdrop_api.chunk_store import ChunkStore, StoreCapabilities, StoreStats
from bitdrop_core import registry
from bitdrop_core.registry import (
    CHUNK_STORE_GROUP,
    AmbiguousComponent,
    InvalidComponent,
    UnknownComponent,
    available_chunk_stores,
    open_chunk_store,
)


class StubStore:
    """The smallest object that satisfies the ChunkStore protocol. Remembers its URL."""

    def __init__(self, url: str) -> None:
        self.url = url

    async def put(self, key: str, value: bytes) -> None: ...

    async def get(self, key: str) -> bytes:
        return b""

    async def delete(self, key: str) -> None: ...

    def capabilities(self) -> StoreCapabilities:
        return StoreCapabilities()

    async def stats(self) -> StoreStats | None:
        return None

    async def aclose(self) -> None: ...


def fake_entry_point(name: str, factory: Callable[[str], object]) -> SimpleNamespace:
    """Stand-in for an installed package's entry point."""
    return SimpleNamespace(name=name, load=lambda: factory)


@pytest.fixture
def install_entry_points(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    """Make the registry see exactly the given entry points instead of the installed ones."""

    def install(*entry_points: SimpleNamespace) -> None:
        def fake_discovery(*, group: str) -> Iterable[Any]:
            assert group == CHUNK_STORE_GROUP
            return entry_points

        monkeypatch.setattr(registry, "entry_points", fake_discovery)

    return install


class TestOpenChunkStoreWithInjectedFactories:
    def test_the_factory_registered_for_the_scheme_receives_the_whole_url(self) -> None:
        store = open_chunk_store("stub://?size=3", factories={"stub": StubStore})

        assert isinstance(store, StubStore)
        assert store.url == "stub://?size=3"

    def test_unknown_scheme_lists_what_is_available(self) -> None:
        with pytest.raises(UnknownComponent, match=r"nope.*\bstub\b"):
            open_chunk_store("nope://", factories={"stub": StubStore})

    @pytest.mark.parametrize("url", ["", "just-a-name", "/some/path"])
    def test_urls_without_a_scheme_are_rejected(self, url: str) -> None:
        with pytest.raises(ValueError, match="scheme"):
            open_chunk_store(url, factories={"stub": StubStore})

    def test_a_factory_that_returns_something_else_is_reported(self) -> None:
        with pytest.raises(InvalidComponent, match="stub"):
            open_chunk_store("stub://", factories={"stub": lambda url: object()})  # type: ignore[dict-item,return-value]

    def test_the_result_satisfies_the_protocol(self) -> None:
        assert isinstance(open_chunk_store("stub://", factories={"stub": StubStore}), ChunkStore)


class TestDiscoveryThroughEntryPoints:
    def test_installed_adapters_are_found_by_scheme(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(fake_entry_point("stub", StubStore))

        store = open_chunk_store("stub://x")

        assert isinstance(store, StubStore)

    def test_available_schemes_are_sorted(self, install_entry_points: Callable[..., None]) -> None:
        install_entry_points(
            fake_entry_point("zeta", StubStore),
            fake_entry_point("alpha", StubStore),
        )

        assert available_chunk_stores() == ["alpha", "zeta"]

    def test_only_the_requested_adapter_is_loaded(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        def explode() -> None:
            raise AssertionError("an adapter that was not requested must not be imported")

        broken = SimpleNamespace(name="broken", load=explode)
        install_entry_points(broken, fake_entry_point("stub", StubStore))

        assert isinstance(open_chunk_store("stub://"), StubStore)

    def test_two_adapters_claiming_one_scheme_is_an_error(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(
            fake_entry_point("stub", StubStore),
            fake_entry_point("stub", StubStore),
        )

        with pytest.raises(AmbiguousComponent, match="stub"):
            open_chunk_store("stub://")

    def test_unknown_scheme_with_nothing_installed_says_so(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points()

        with pytest.raises(UnknownComponent, match="no chunk stores are installed"):
            open_chunk_store("stub://")
