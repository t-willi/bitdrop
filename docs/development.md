# Developing bitdrop

How the repository is laid out, how to run everything, and how to extend it. For *why* it is built this way, read
[principles.md](principles.md); for the product thinking, [ideation.md](ideation.md).

## Setup

You need [uv](https://docs.astral.sh/uv/). Python 3.14 is pinned in `.python-version`; uv fetches it if missing.

```bash
make sync     # create .venv from uv.lock
make check    # lint + strict types + architecture rules + all tests
```

Run `make help` for every target. Each one is a thin wrapper around a `uv run ...` command, so you can also run
those directly, e.g. `uv run pytest packages/bitdrop-store-file -k atomic`.

| Command | What it does |
|---|---|
| `make test` | The whole test suite (`pytest`) |
| `make lint` | `ruff` lint and format check, no changes |
| `make format` | Auto-fix lint issues and format |
| `make typecheck` | `mypy --strict` |
| `make arch` | `import-linter`: who may import whom |
| `make check` | All of the above |

## Repository layout

```
bitdrop/
├── pyproject.toml          Workspace root: dev tools and all tool configuration
├── Makefile                Developer commands (see above)
├── conftest.py             Repo-wide pytest setup
├── docs/                   ideation.md, principles.md, this file
├── packages/               One uv-workspace package per component
│   ├── bitdrop-api/          Contracts + shared types + the conformance kit. Depends on nothing.
│   │   ├── src/bitdrop_api/
│   │   │   ├── chunk_store.py    ChunkStore: where chunks wait in RAM (data plane)
│   │   │   ├── metadata_store.py MetadataStore: chunk states, leases, fencing (coordination)
│   │   │   ├── errors.py         Errors every adapter raises
│   │   │   ├── keys.py           The one key/id rule all backends share
│   │   │   ├── urls.py           Strict parsing of component URLs
│   │   │   └── testing/          Conformance kits (importable base test classes)
│   │   └── tests/
│   ├── bitdrop-core/             Finds adapters by URL scheme (entry points). Imports no adapter.
│   ├── bitdrop-store-memory/     ChunkStore adapter: a dict in this process
│   ├── bitdrop-store-file/       ChunkStore adapter: one file per chunk (point it at a tmpfs)
│   ├── bitdrop-metadata-memory/  MetadataStore adapter: dicts in this process
│   └── bitdrop-metadata-sqlite/  MetadataStore adapter: a SQLite file (persistent, multi-process)
└── tests/                  Tests that span packages
    ├── integration/          Swap tests and a miniature end-to-end pipeline (see below)
    └── architecture/         Rules like "core never names a backend"
```

Each package follows the same shape: `pyproject.toml`, `src/<import_name>/`, `tests/`.

## The architecture rules (and what enforces them)

| Rule | Enforced by |
|---|---|
| Core and adapters import only `bitdrop_api`, never each other | `make arch` (`import-linter`, configured in `pyproject.toml`) |
| Core and contracts never name a concrete backend in code | `tests/architecture/test_core_is_backend_agnostic.py` |
| An adapter is "supported" only if it passes the conformance kit | The adapter's `test_*_conformance.py` |
| Every installed adapter works when chosen only by URL | `tests/integration/test_swapping_chunk_stores.py`, `test_pipeline_smoke.py` |

## How we work: test-driven

1. Write a failing test that describes the behaviour. Run it and **watch it fail for the right reason**.
2. Write the smallest code that makes it pass.
3. Clean up with the tests green, then run `make check`.

The conformance kits are also *tested* in this spirit: deliberately breaking an adapter (corrupting `get`, skipping the
capacity check, making writes non-atomic, dropping the fence check, never expiring leases) must turn the kit red, and
fail rather than hang. If you add a check to a kit, break an adapter on purpose and confirm the kit notices.

`tests/integration/test_pipeline_smoke.py` is the design in miniature: extractor and loader workers plus a reaper move
chunks through every combination of installed chunk store and metadata store, including a worker that dies mid-chunk,
and assert each chunk reaches the destination exactly once. Its worker loops are scaffolding until the real master and
workers exist.

## Adding an adapter (e.g. a Redis ChunkStore or a Postgres MetadataStore)

1. **Create the package** with the same layout as an existing adapter: `packages/bitdrop-store-<name>/` for a
   ChunkStore, `packages/bitdrop-metadata-<name>/` for a MetadataStore. Depend only on `bitdrop-api` plus your client
   library.
2. **Write the conformance test first**: `tests/test_<name>_..._conformance.py`, subclassing `ChunkStoreConformance` or
   `MetadataStoreConformance`.

   ```python
   class TestMyStoreConformance(ChunkStoreConformance):
       @pytest.fixture
       async def store(self) -> AsyncIterator[ChunkStore]:
           store = MyStore(...)  # fresh and empty for every test
           yield store
           await store.aclose()
   ```

   Backends that need a server start it in the fixture (testcontainers). Also override `capped_store` if the backend
   can be limited to well under 64 MiB.
3. **Run it, see it fail**, then implement until it passes.
4. **Register it** in the package's `pyproject.toml`; the entry-point name is the URL scheme and the group says which
   contract it implements (`bitdrop.chunk_stores` or `bitdrop.metadata_stores`):

   ```toml
   [project.entry-points."bitdrop.chunk_stores"]
   redis = "bitdrop_store_redis:create_store"
   ```

5. **Wire it into the workspace**: add the package to `dependencies`, `[tool.uv.sources]` and
   `known-first-party`/`root_packages`/`mypy_path` in the root `pyproject.toml`, to the `layers` contract, and add one
   line to the URL builders in `tests/integration/test_swapping_chunk_stores.py` (chunk stores) or
   `test_pipeline_smoke.py` (metadata stores). Those tests fail until you do.
6. `make sync && make check`.

No line of the core changes. That is the point.

## Choosing a store at runtime

A store is selected by URL; the scheme picks the adapter and the rest is its configuration.

```python
from bitdrop_core.registry import open_chunk_store, open_metadata_store

store = open_chunk_store("memory://?max_bytes=1000000000")
store = open_chunk_store("file:///dev/shm/bitdrop?locality=pod")

meta = open_metadata_store("sqlite:///var/lib/bitdrop/metadata.db")
```

Each kind of store has its own scheme namespace, so `memory` exists as a chunk store *and* as a metadata store.

| Kind | Scheme | Parameters |
|---|---|---|
| ChunkStore | `memory://` | `max_bytes`, `max_value_bytes` |
| ChunkStore | `file:///<directory>` | `max_value_bytes`, `locality` (`pod` / `node` / `cluster`) |
| MetadataStore | `memory://` | none |
| MetadataStore | `sqlite:///<file>` or `sqlite://` (throwaway, in memory) | none |
