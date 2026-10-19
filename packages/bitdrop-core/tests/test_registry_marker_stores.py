"""The core opens a MarkerStore from a URL, from its own namespace, like every other component."""

from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

import pytest

from bitdrop_api.marker_store import MarkerStore
from bitdrop_core import registry
from bitdrop_core.registry import (
    MARKER_STORE_GROUP,
    InvalidComponent,
    UnknownComponent,
    available_chunk_stores,
    available_marker_stores,
    open_marker_store,
)


class StubMarkerStore:
    """The smallest object that satisfies the MarkerStore protocol. Remembers its URL."""

    def __init__(self, url: str) -> None:
        self.url = url

    async def set_if_absent(self, *args: Any, **kwargs: Any) -> bytes:
        return b""

    async def get(self, *args: Any, **kwargs: Any) -> bytes | None:
        return None

    async def aclose(self) -> None: ...


def fake_entry_point(name: str, factory: Callable[[str], object]) -> SimpleNamespace:
    return SimpleNamespace(name=name, load=lambda: factory)


@pytest.fixture
def install_entry_points(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    def install(**by_group: list[SimpleNamespace]) -> None:
        def fake_discovery(*, group: str) -> Iterable[Any]:
            return by_group.get(group.replace(".", "_"), [])

        monkeypatch.setattr(registry, "entry_points", fake_discovery)

    return install


class TestOpenMarkerStore:
    def test_the_factory_for_the_scheme_receives_the_whole_url(self) -> None:
        store = open_marker_store("stub://host/0?x=1", factories={"stub": StubMarkerStore})

        assert isinstance(store, StubMarkerStore)
        assert store.url == "stub://host/0?x=1"
        assert isinstance(store, MarkerStore)

    def test_unknown_scheme_names_the_kind_and_lists_what_is_available(self) -> None:
        with pytest.raises(UnknownComponent, match=r"marker store.*nope.*\bstub\b"):
            open_marker_store("nope://", factories={"stub": StubMarkerStore})

    def test_urls_without_a_scheme_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="scheme"):
            open_marker_store("just-a-name", factories={"stub": StubMarkerStore})

    def test_a_factory_that_returns_something_else_is_reported(self) -> None:
        with pytest.raises(InvalidComponent, match="marker store"):
            open_marker_store("stub://", factories={"stub": lambda url: object()})  # type: ignore[dict-item,return-value]


class TestDiscovery:
    def test_marker_stores_are_found_in_their_own_entry_point_group(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(bitdrop_marker_stores=[fake_entry_point("stub", StubMarkerStore)])

        assert MARKER_STORE_GROUP == "bitdrop.marker_stores"
        assert isinstance(open_marker_store("stub://"), StubMarkerStore)
        assert available_marker_stores() == ["stub"]

    def test_a_chunk_store_scheme_is_not_a_marker_store_scheme(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(
            bitdrop_chunk_stores=[fake_entry_point("only-chunks", lambda url: object())]
        )

        assert available_chunk_stores() == ["only-chunks"]
        with pytest.raises(UnknownComponent, match="no marker stores are installed"):
            open_marker_store("only-chunks://")

    def test_the_installed_memory_marker_store_opens_by_url(self) -> None:
        store = open_marker_store("memory://")

        assert isinstance(store, MarkerStore)
