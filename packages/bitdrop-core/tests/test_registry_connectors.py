"""The core opens Sources and Destinations from URLs, each kind in its own namespace."""

from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

import pytest

from bitdrop_api.destination import Destination
from bitdrop_api.source import Source
from bitdrop_core import registry
from bitdrop_core.registry import (
    DESTINATION_GROUP,
    SOURCE_GROUP,
    InvalidComponent,
    UnknownComponent,
    available_destinations,
    available_sources,
    open_destination,
    open_source,
)


class StubSource:
    def __init__(self, url: str) -> None:
        self.url = url

    async def plan(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def read_chunk(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def aclose(self) -> None: ...


class StubDestination:
    def __init__(self, url: str) -> None:
        self.url = url

    async def prepare(self, *args: Any, **kwargs: Any) -> None: ...

    async def write_chunk(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def count_rows(self, *args: Any, **kwargs: Any) -> int:
        return 0

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


class TestSources:
    def test_the_factory_for_the_scheme_receives_the_whole_url(self) -> None:
        source = open_source("stub://user:pw@host/db", factories={"stub": StubSource})

        assert isinstance(source, Source)
        assert isinstance(source, StubSource)
        assert source.url == "stub://user:pw@host/db"

    def test_unknown_scheme_names_the_kind(self) -> None:
        with pytest.raises(UnknownComponent, match=r"source.*nope.*\bstub\b"):
            open_source("nope://", factories={"stub": StubSource})

    def test_a_factory_returning_something_else_is_reported(self) -> None:
        with pytest.raises(InvalidComponent, match="source"):
            open_source("stub://", factories={"stub": lambda url: object()})  # type: ignore[dict-item,return-value]

    def test_discovery_uses_the_sources_entry_point_group(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(bitdrop_sources=[fake_entry_point("stub", StubSource)])

        assert isinstance(open_source("stub://"), StubSource)
        assert available_sources() == ["stub"]


class TestDestinations:
    def test_the_factory_for_the_scheme_receives_the_whole_url(self) -> None:
        destination = open_destination("stub://x", factories={"stub": StubDestination})

        assert isinstance(destination, Destination)
        assert isinstance(destination, StubDestination)
        assert destination.url == "stub://x"

    def test_unknown_scheme_names_the_kind(self) -> None:
        with pytest.raises(UnknownComponent, match="destination"):
            open_destination("nope://", factories={"stub": StubDestination})

    def test_discovery_uses_the_destinations_entry_point_group(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(bitdrop_destinations=[fake_entry_point("stub", StubDestination)])

        assert isinstance(open_destination("stub://"), StubDestination)
        assert available_destinations() == ["stub"]


class TestNamespaces:
    def test_sources_and_destinations_may_share_a_scheme(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        """'postgresql' is both a place to read from and a place to write to."""
        install_entry_points(
            bitdrop_sources=[fake_entry_point("pg", StubSource)],
            bitdrop_destinations=[fake_entry_point("pg", StubDestination)],
        )

        assert isinstance(open_source("pg://"), StubSource)
        assert isinstance(open_destination("pg://"), StubDestination)

    def test_a_source_scheme_is_not_a_destination_scheme(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(bitdrop_sources=[fake_entry_point("read-only", StubSource)])

        with pytest.raises(UnknownComponent, match="no destinations are installed"):
            open_destination("read-only://")

    def test_the_group_names_are_the_documented_ones(self) -> None:
        assert (SOURCE_GROUP, DESTINATION_GROUP) == ("bitdrop.sources", "bitdrop.destinations")
