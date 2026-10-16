"""Choosing the display: ``auto`` picks the TUI on a terminal when it is installed."""

import pytest

from bitdrop_cli.execute import resolve_ui
from bitdrop_core.registry import UnknownComponent

ALL = ["json", "plain", "tui"]


class TestAuto:
    def test_a_terminal_gets_the_tui(self) -> None:
        assert resolve_ui("auto", interactive=True, available=ALL) == "tui"

    def test_a_pipe_gets_plain_text(self) -> None:
        assert resolve_ui("auto", interactive=False, available=ALL) == "plain"

    def test_without_the_tui_installed_a_terminal_gets_plain_text(self) -> None:
        assert resolve_ui("auto", interactive=True, available=["json", "plain"]) == "plain"


class TestExplicit:
    @pytest.mark.parametrize("name", ["plain", "json"])
    def test_a_named_display_is_used_as_asked(self, name: str) -> None:
        assert resolve_ui(name, interactive=False, available=ALL) == name
        assert resolve_ui(name, interactive=True, available=ALL) == name

    def test_the_tui_is_used_when_asked_on_a_terminal(self) -> None:
        assert resolve_ui("tui", interactive=True, available=ALL) == "tui"

    def test_the_tui_is_refused_without_a_terminal(self) -> None:
        with pytest.raises(ValueError, match="terminal"):
            resolve_ui("tui", interactive=False, available=ALL)

    def test_an_unknown_name_lists_what_is_installed(self) -> None:
        with pytest.raises(UnknownComponent, match=r"nope.*json, plain, tui"):
            resolve_ui("nope", interactive=True, available=ALL)
