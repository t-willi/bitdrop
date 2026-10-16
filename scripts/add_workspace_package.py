"""Register a new package of the uv workspace in the root pyproject.toml.

    uv run python scripts/add_workspace_package.py <dist-name> <import_name> [--top]
    uv run python scripts/add_workspace_package.py bitdrop-store-memcached bitdrop_store_memcached
    uv run python scripts/add_workspace_package.py bitdrop-tui bitdrop_tui --top

Run it after creating ``packages/<dist-name>/`` (with its own pyproject.toml and
``src/<import_name>/``).
It adds the package to the root dependencies, ``[tool.uv.sources]``, the ruff first-party list, the
mypy path, and the import-linter root packages, layers, and forbidden-imports contracts, then you
run ``uv sync``. Components (connectors, stores) sit beside the core; ``--top`` is for front
ends such as the command line tool and the displays, which sit above it and may not import an
adapter.

It edits by exact anchors and aborts without writing anything if one is not found exactly once or
if the package is already registered, so a changed pyproject.toml never ends up half-edited.
"""

import pathlib
import re
import sys

ROOT_PYPROJECT = pathlib.Path("pyproject.toml")


def register(text: str, dist: str, module: str, *, top: bool = False) -> str:
    """Return ``text`` with the package registered everywhere, or raise ``SystemExit``.

    A component (a connector or a store) sits in the layer beside the core, may not be imported by
    the front ends, and is added to the front ends' list of forbidden imports. With ``top=True``
    the package is a front end (the command line tool, a display): it sits in the top layer and is
    itself checked for importing an adapter.
    """
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
        '\n]\n\n[[tool.importlinter.contracts]]\nname = "Core and adapters',
        f'\n    "{module}",',
        "import-linter root packages",
    )

    layers = re.search(r'layers = \[\n((?:    "[^"]*",\n)+)\]', text)
    if layers is None:
        raise SystemExit("anchor for the import-linter layers not found; edit the file by hand")
    entries = layers.group(1).splitlines(keepends=True)
    # The top layer is the first entry; the one beside the core is the last before bitdrop_api.
    index = 0 if top else len(entries) - 2
    entries[index] = entries[index].replace('",\n', f' | {module}",\n')
    text = text[: layers.start(1)] + "".join(entries) + text[layers.end(1) :]

    if top:
        sources = re.search(r"source_modules = \[([^\]]*)\]", text)
        if sources is None:
            raise SystemExit("anchor for source_modules not found; edit the file by hand")
        text = text[: sources.end(1)] + f', "{module}"' + text[sources.end(1) :]
    else:
        forbidden = re.search(r'forbidden_modules = \[\n(?:    "[^"]*",\n)+', text)
        if forbidden is None:
            raise SystemExit("anchor for forbidden_modules not found; edit the file by hand")
        text = text[: forbidden.end()] + f'    "{module}",\n' + text[forbidden.end() :]
    return text


def main() -> None:
    """Read the arguments and edit the root pyproject.toml in place."""
    arguments = [argument for argument in sys.argv[1:] if argument != "--top"]
    if len(arguments) != 2:
        raise SystemExit(__doc__)
    dist, module = arguments
    top = "--top" in sys.argv[1:]
    ROOT_PYPROJECT.write_text(register(ROOT_PYPROJECT.read_text(), dist, module, top=top))
    print(f"registered {dist} ({module}); now run `uv sync`")


if __name__ == "__main__":
    main()
