"""P1, rule 3: the core and the contracts must not name concrete backends.

Prose may mention backends (docstrings and comments explain things by example). *Code* may not:
no imports of them, no ``if scheme == "redis"``, no attributes or names that spell one out.
"""

import ast
import importlib.util
from pathlib import Path

import pytest

BACKEND_NAMES = ("redis", "valkey", "memcache", "dragonfly", "garnet", "keydb", "bitdrop_store_")
"""Lower-case fragments that must not appear in the code of backend-agnostic packages."""

AGNOSTIC_PACKAGES = ("bitdrop_api", "bitdrop_core")


def find_backend_names(source: str) -> list[str]:
    """Return the backend names that occur in the *code* of ``source`` (not in prose)."""
    tree = ast.parse(source)
    # A bare string statement is a docstring: prose, not code. Remember those string nodes.
    prose = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
    }
    found: list[str] = []
    for node in ast.walk(tree):
        if id(node) in prose:
            continue
        for text in _identifiers_and_literals(node):
            lowered = text.lower()
            found.extend(name for name in BACKEND_NAMES if name in lowered)
    return found


def _identifiers_and_literals(node: ast.AST) -> list[str]:
    """Names, attributes, imported modules, and string literals directly held by ``node``."""
    texts: list[str] = []
    if isinstance(node, ast.Name):
        texts.append(node.id)
    elif isinstance(node, ast.Attribute):
        texts.append(node.attr)
    elif isinstance(node, ast.Import):
        texts.extend(alias.name for alias in node.names)
    elif isinstance(node, ast.ImportFrom):
        texts.append(node.module or "")
        texts.extend(alias.name for alias in node.names)
    elif isinstance(node, ast.Constant) and isinstance(node.value, str):
        texts.append(node.value)
    elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
        texts.append(node.name)
    return texts


class TestTheScannerItself:
    """A scanner that silently finds nothing would make the real check worthless."""

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param("import redis", id="import"),
            pytest.param("from valkey.asyncio import Client", id="from-import"),
            pytest.param("if scheme == 'memcached':\n    pass", id="string-literal"),
            pytest.param("store = backends.Dragonfly", id="attribute"),
            pytest.param("class RedisStore: ...", id="class-name"),
            pytest.param("from bitdrop_store_memory import x", id="adapter-package"),
        ],
    )
    def test_detects_backend_names_in_code(self, source: str) -> None:
        assert find_backend_names(source)

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param('"""Works with Redis."""\nx = 1', id="module-docstring"),
            pytest.param('def f():\n    """Like Redis or memcached."""', id="function-docstring"),
            pytest.param('X = 1\n"""Attribute docs mention Redis."""', id="attribute-docstring"),
            pytest.param("x = 1  # Redis is one example", id="comment"),
            pytest.param("import asyncio", id="innocent-code"),
        ],
    )
    def test_ignores_prose(self, source: str) -> None:
        assert find_backend_names(source) == []


def _source_files(package: str) -> list[Path]:
    spec = importlib.util.find_spec(package)
    assert spec is not None
    assert spec.submodule_search_locations, f"{package} is not a package"
    return [
        path
        for location in spec.submodule_search_locations
        for path in Path(location).rglob("*.py")
        # The conformance kit is a test helper; it may need to speak about concrete failure modes.
        if "testing" not in path.relative_to(location).parts
    ]


@pytest.mark.parametrize("package", AGNOSTIC_PACKAGES)
def test_package_code_names_no_backend(package: str) -> None:
    files = _source_files(package)
    assert files, f"no source files found for {package}: the scan would be vacuous"

    offenders = {
        path.name: names
        for path in files
        if (names := find_backend_names(path.read_text(encoding="utf-8")))
    }

    assert offenders == {}, f"{package} must stay backend-agnostic (P1)"
