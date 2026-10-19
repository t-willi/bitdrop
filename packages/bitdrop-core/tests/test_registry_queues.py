"""The core opens a ChunkQueue from a URL, from its own namespace, like every other component."""

from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

import pytest

from bitdrop_api.chunk_queue import ChunkQueue
from bitdrop_core import registry
from bitdrop_core.registry import (
    CHUNK_QUEUE_GROUP,
    InvalidComponent,
    UnknownComponent,
    available_chunk_queues,
    available_chunk_stores,
    open_chunk_queue,
)


class StubQueue:
    """The smallest object that satisfies the ChunkQueue protocol. Remembers its URL."""

    def __init__(self, url: str) -> None:
        self.url = url

    async def publish(self, *args: Any, **kwargs: Any) -> None: ...

    async def receive(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    async def reclaim(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    async def touch(self, *args: Any, **kwargs: Any) -> None: ...
    async def ack(self, *args: Any, **kwargs: Any) -> None: ...
    async def release(self, *args: Any, **kwargs: Any) -> None: ...
    async def dead_letter(self, *args: Any, **kwargs: Any) -> None: ...

    async def stats(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

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


class TestOpenChunkQueue:
    def test_the_factory_for_the_scheme_receives_the_whole_url(self) -> None:
        queue = open_chunk_queue("stub://host/0?x=1", factories={"stub": StubQueue})

        assert isinstance(queue, StubQueue)
        assert queue.url == "stub://host/0?x=1"
        assert isinstance(queue, ChunkQueue)

    def test_unknown_scheme_names_the_kind_and_lists_what_is_available(self) -> None:
        with pytest.raises(UnknownComponent, match=r"chunk queue.*nope.*\bstub\b"):
            open_chunk_queue("nope://", factories={"stub": StubQueue})

    def test_urls_without_a_scheme_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="scheme"):
            open_chunk_queue("just-a-name", factories={"stub": StubQueue})

    def test_a_factory_that_returns_something_else_is_reported(self) -> None:
        with pytest.raises(InvalidComponent, match="chunk queue"):
            open_chunk_queue("stub://", factories={"stub": lambda url: object()})  # type: ignore[dict-item,return-value]


class TestDiscovery:
    def test_queues_are_found_in_their_own_entry_point_group(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(bitdrop_chunk_queues=[fake_entry_point("stub", StubQueue)])

        assert CHUNK_QUEUE_GROUP == "bitdrop.chunk_queues"
        assert isinstance(open_chunk_queue("stub://"), StubQueue)
        assert available_chunk_queues() == ["stub"]

    def test_a_chunk_store_scheme_is_not_a_queue_scheme(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(
            bitdrop_chunk_stores=[fake_entry_point("only-chunks", lambda url: object())]
        )

        assert available_chunk_stores() == ["only-chunks"]
        with pytest.raises(UnknownComponent, match="no chunk queues are installed"):
            open_chunk_queue("only-chunks://")

    def test_the_installed_memory_queue_opens_by_url(self) -> None:
        queue = open_chunk_queue("memory://")

        assert isinstance(queue, ChunkQueue)
