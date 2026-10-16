"""Progress displays are components too: found by name through their own entry point group."""

import asyncio
import logging
from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

import pytest

from bitdrop_core import registry
from bitdrop_core.progress import TransferEvent
from bitdrop_core.registry import (
    UI_GROUP,
    AmbiguousComponent,
    InvalidComponent,
    UnknownComponent,
    available_uis,
    open_ui,
)
from bitdrop_core.transfer import TransferReport
from bitdrop_core.ui import ProgressUi, UiContext


class StubUi:
    """The smallest object that satisfies the ProgressUi protocol."""

    def __init__(self, context: UiContext) -> None:
        self.context = context

    def __call__(self, event: TransferEvent) -> None: ...

    def log_handler(self) -> logging.Handler | None:
        return None

    async def present(self, job: asyncio.Task[TransferReport]) -> None: ...


CONTEXT = UiContext(
    job_id="job-1",
    source_table="src",
    destination_table="dst",
    endpoints={"source": "a://", "destination": "b://"},
)


def fake_entry_point(name: str, factory: Callable[[UiContext], object]) -> SimpleNamespace:
    return SimpleNamespace(name=name, load=lambda: factory)


@pytest.fixture
def install_entry_points(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    def install(**by_group: list[SimpleNamespace]) -> None:
        def fake_discovery(*, group: str) -> Iterable[Any]:
            return by_group.get(group.replace(".", "_"), [])

        monkeypatch.setattr(registry, "entry_points", fake_discovery)

    return install


class TestOpenUi:
    def test_the_factory_for_the_name_receives_the_context(self) -> None:
        ui = open_ui("stub", CONTEXT, factories={"stub": StubUi})

        assert isinstance(ui, StubUi)
        assert ui.context is CONTEXT

    def test_the_result_satisfies_the_protocol(self) -> None:
        assert isinstance(open_ui("stub", CONTEXT, factories={"stub": StubUi}), ProgressUi)

    def test_unknown_names_list_what_is_available(self) -> None:
        with pytest.raises(UnknownComponent, match=r"progress UI.*nope.*\bstub\b"):
            open_ui("nope", CONTEXT, factories={"stub": StubUi})

    def test_a_factory_that_returns_something_else_is_reported(self) -> None:
        with pytest.raises(InvalidComponent, match="progress UI"):
            open_ui("stub", CONTEXT, factories={"stub": lambda context: object()})  # type: ignore[dict-item,return-value]


class TestDiscovery:
    def test_displays_are_found_in_their_own_entry_point_group(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(
            bitdrop_uis=[fake_entry_point("stub", StubUi), fake_entry_point("alpha", StubUi)]
        )

        assert isinstance(open_ui("stub", CONTEXT), StubUi)
        assert available_uis() == ["alpha", "stub"]

    def test_other_kinds_of_components_are_not_displays(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(bitdrop_sources=[fake_entry_point("stub", StubUi)])

        assert available_uis() == []
        with pytest.raises(UnknownComponent, match="no progress UIs are installed"):
            open_ui("stub", CONTEXT)

    def test_two_displays_with_one_name_is_an_error(
        self, install_entry_points: Callable[..., None]
    ) -> None:
        install_entry_points(
            bitdrop_uis=[fake_entry_point("stub", StubUi), fake_entry_point("stub", StubUi)]
        )

        with pytest.raises(AmbiguousComponent, match="progress UI"):
            open_ui("stub", CONTEXT)

    def test_the_group_name_is_the_documented_one(self) -> None:
        assert UI_GROUP == "bitdrop.uis"
