"""Register a new package of the uv workspace in the root pyproject.toml.

    uv run python scripts/add_workspace_package.py <dist-name> <import_name>
    uv run python scripts/add_workspace_package.py bitdrop-store-memcached bitdrop_store_memcached

Run it after creating ``packages/<dist-name>/`` (with its own pyproject.toml and
``src/<import_name>/``).
It adds the package to the root dependencies, ``[tool.uv.sources]``, the ruff first-party list, the
mypy path, and the import-linter root packages and layers contract, then you run ``uv sync``.

It edits by exact anchors and aborts without writing anything if one is not found exactly once or
if the package is already registered, so a changed pyproject.toml never ends up half-edited.
"""

import pathlib
import sys

ROOT_PYPROJECT = pathlib.Path("pyproject.toml")


def register(text: str, dist: str, module: str) -> str:
    """Return ``text`` with the package registered everywhere, or raise ``SystemExit``."""
    if f'"{module}"' in text:
        raise SystemExit(f"{module} is already registered in {ROOT_PYPROJECT}")

    def insert_before(anchor: str, addition: str, what: str) -> None:
        nonlocal text
        if text.count(anchor) != 1:
            raise SystemExit(f"anchor for {what} not found exactly once; edit the file by hand")
        text = text.replace(anchor, addition + anchor)

    insert_before("\n]\n\n[dependency-groups]", f'\n    "{dist}",', "the root dependencies")
    insert_before(
        "\n\n# " + "-" * 93 + "\n# pytest",
        f"\n{dist} = {{ workspace = true }}",
        "[tool.uv.sources]",
    )
    insert_before("\n]\n\n[tool.ruff.lint.pydocstyle]", f'\n    "{module}",', "ruff first-party")
    insert_before("\n]\n\n[[tool.mypy.overrides]]", f'\n    "packages/{dist}/src",', "mypy_path")
    insert_before(
        "\n]\n\n[[tool.importlinter.contracts]]",
        f'\n    "{module}",',
        "import-linter root packages",
    )
    layer_end = '",\n    "bitdrop_api",\n]'
    if text.count(layer_end) != 1:
        raise SystemExit("anchor for the import-linter layers not found exactly once")
    return text.replace(layer_end, f" | {module}{layer_end}")


def main() -> None:
    """Read the two arguments and edit the root pyproject.toml in place."""
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    dist, module = sys.argv[1], sys.argv[2]
    ROOT_PYPROJECT.write_text(register(ROOT_PYPROJECT.read_text(), dist, module))
    print(f"registered {dist} ({module}); now run `uv sync`")


if __name__ == "__main__":
    main()
