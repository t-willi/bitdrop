"""``scripts/add_workspace_package.py`` must keep working as the root pyproject.toml grows.

It edits the real file by exact anchors, so any change around those anchors can break it silently
until the day someone adds a package. These tests run it against the real pyproject.toml (in memory,
nothing is written) and check the result by parsing it as TOML.
"""

import importlib.util
import tomllib
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = ROOT / "pyproject.toml"


def load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "add_workspace_package", ROOT / "scripts" / "add_workspace_package.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def register(*, top: bool = False) -> dict[str, Any]:
    script = load_script()
    text = script.register(PYPROJECT.read_text(), "bitdrop-newthing", "bitdrop_newthing", top=top)
    return tomllib.loads(text)


def contract(data: dict[str, Any], kind: str) -> dict[str, Any]:
    return next(c for c in data["tool"]["importlinter"]["contracts"] if c["type"] == kind)


class TestRegisteringAComponent:
    def test_it_is_known_to_the_workspace_and_the_tools(self) -> None:
        data = register()

        assert "bitdrop-newthing" in data["project"]["dependencies"]
        assert data["tool"]["uv"]["sources"]["bitdrop-newthing"] == {"workspace": True}
        assert "bitdrop_newthing" in data["tool"]["ruff"]["lint"]["isort"]["known-first-party"]
        assert "packages/bitdrop-newthing/src" in data["tool"]["mypy"]["mypy_path"]
        assert "bitdrop_newthing" in data["tool"]["importlinter"]["root_packages"]

    def test_it_sits_beside_the_core_and_may_not_be_imported_by_the_command_line_tools(
        self,
    ) -> None:
        data = register()

        layers = contract(data, "layers")["layers"]
        assert "bitdrop_newthing" in layers[1].split(" | ")
        assert "bitdrop_newthing" not in layers[0]
        assert "bitdrop_newthing" in contract(data, "forbidden")["forbidden_modules"]


class TestRegisteringAFrontEnd:
    """The command line tool and the displays sit above the core and import no adapter."""

    def test_it_sits_in_the_top_layer_and_is_checked_for_adapter_imports(self) -> None:
        data = register(top=True)

        layers = contract(data, "layers")["layers"]
        assert "bitdrop_newthing" in layers[0].split(" | ")
        assert "bitdrop_newthing" not in layers[1]
        assert "bitdrop_newthing" in contract(data, "forbidden")["source_modules"]
        assert "bitdrop_newthing" not in contract(data, "forbidden")["forbidden_modules"]

    def test_it_is_known_to_the_workspace_and_the_tools_too(self) -> None:
        data = register(top=True)

        assert "bitdrop-newthing" in data["project"]["dependencies"]
        assert "packages/bitdrop-newthing/src" in data["tool"]["mypy"]["mypy_path"]


def test_registering_twice_is_refused() -> None:
    script = load_script()
    once = script.register(PYPROJECT.read_text(), "bitdrop-newthing", "bitdrop_newthing", top=False)

    with pytest.raises(SystemExit, match="already registered"):
        script.register(once, "bitdrop-newthing", "bitdrop_newthing", top=False)
