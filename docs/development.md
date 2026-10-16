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
| `make test` | The whole test suite (`pytest`), including the tests that start PostgreSQL in Docker |
| `make test-fast` | Everything that needs no Docker (about 5 seconds) |
| `make bench` | Chunk store throughput table (needs Docker, about 10 seconds) |
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
│   │       (also: source.py, destination.py, codec.py, schema.py for the connector contracts)
│   ├── bitdrop-core/             Imports no adapter. Finds them by URL scheme and runs the transfer.
│   │   └── src/bitdrop_core/
│   │       ├── registry.py       open_source / open_destination / open_chunk_store / open_metadata_store
│   │       ├── transfer.py       run_transfer: plan, extract, stage, load, recover (master + workers)
│   │       └── codec.py          Arrow IPC chunk codec (none / lz4 / zstd)
│   ├── bitdrop-store-memory/     ChunkStore adapter: a dict in this process
│   ├── bitdrop-store-file/       ChunkStore adapter: one file per chunk (point it at a tmpfs)
│   ├── bitdrop-store-redis/      ChunkStore adapter for the Redis protocol: Redis, Valkey, Dragonfly
│   ├── bitdrop-metadata-memory/  MetadataStore adapter: dicts in this process
│   ├── bitdrop-metadata-sqlite/  MetadataStore adapter: a SQLite file (persistent, multi-process)
│   ├── bitdrop-connector-memory/ Source + Destination over in-memory Arrow tables
│   └── bitdrop-connector-postgres/ Source + Destination for PostgreSQL (ADBC); needs Docker to test
├── benchmarks/             Throughput measurements (not part of the normal test run)
└── tests/                  Tests that span packages
    ├── integration/          Swap tests, a miniature pipeline, and the transfer runner with injected faults
    ├── e2e/                  Real PostgreSQL to real PostgreSQL (Docker), verified by row fingerprints
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

## Tests that use Docker

Tests marked `docker` start one throwaway PostgreSQL container for the whole session (`postgres:17-alpine`, on
a random port) and give every test its own database. They are skipped, not failed, if Docker is unavailable, and
`make test-fast` leaves them out. Only the containers the tests start are touched and removed; nothing else on your
Docker host is affected.

```bash
uv run pytest tests/e2e -s       # the end-to-end suite, with throughput printed
uv run pytest -m docker          # every test that needs Docker
```

The Redis-protocol tests start Redis, Valkey, and Dragonfly containers (pinned versions, persistence off) and run the
same conformance kit against each, so "any in-memory key-value store works" is demonstrated, not assumed.

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

## Running a transfer

There is no command-line tool yet; a transfer is a function call. Everything is chosen by URL:

```python
import asyncio

from bitdrop_core.registry import (
    open_chunk_store,
    open_destination,
    open_metadata_store,
    open_source,
)
from bitdrop_core.transfer import TransferConfig, run_transfer


async def main() -> None:
    source = open_source("postgresql://user:pw@source-host/shop")
    destination = open_destination("postgresql://user:pw@warehouse-host/shop")
    staging = open_chunk_store("memory://")  # or file:///dev/shm/bitdrop on a tmpfs
    metadata = open_metadata_store("sqlite:///var/lib/bitdrop/job-1.db")  # persistent: resumable

    report = await run_transfer(
        source,
        destination,
        staging,
        metadata,
        TransferConfig(job_id="job-1", source_table="public.orders", destination_table="orders"),
    )
    print(report.succeeded, report.rows_loaded, report.failures)


asyncio.run(main())
```

Running the same `job_id` again against the same metadata store and destination resumes the job: chunks that are
already loaded are skipped, and the destination's ledger makes a repeat harmless even if the metadata is lost.

## The PostgreSQL connector

`postgresql://user:password@host:port/database`. Option `bitdrop_pool_size` (default 4) bounds the connections; all
other query options go to libpq. Passwords are redacted from every error message.

**What it does**

- *Source*: reads the catalog, splits the table by integer primary key into inclusive ranges sized from a sample of
  the row width (sparse keys get wider ranges; the extremes of `bigint` do not overflow). Without an integer primary
  key it reads one chunk, and says so in the plan notes. `split_column` overrides the choice (NOT NULL integer).
- *Destination*: creates the schema and table (from native type hints when the source is PostgreSQL, otherwise from
  Arrow types) **without indexes or constraints**, which is much faster for bulk loads; add them afterwards. Each
  chunk is one transaction: ledger row plus `COPY`.
- Table names are validated (`table` or `schema.table`, case-sensitive) and every identifier is quoted.

**Supported column types** (each proven by `tests/e2e`): `smallint`, `integer`, `bigint`, `real`,
`double precision`, `boolean`, `text`, `varchar(n)`, `char(n)`, `date`, `time`, `timestamp[(p)]`, `timestamptz`,
`bytea`, `uuid`, `json`, `jsonb`, `inet`, `interval`, `numeric(p,s)` with `p <= 38`, and arrays of
`text`/`varchar`/`integer`/`bigint`. Anything else (`money`, unconstrained `numeric`, ranges, enums, geometry, ...) is
refused at planning with the column named. Workaround: transfer a view that casts the column.

**Known limits**

- No snapshot isolation: chunks are read at different moments. Every row is read at most once, but rows inserted,
  deleted, or re-keyed during the transfer may or may not appear.
- Arrays with `NULL` elements are refused (the driver would store them as `0` or `''`); see principle P2.
- Chunk sizes come from `reltuples` (or a `count(*)` for tables never analysed) and a row-width sample; skewed data
  gives uneven chunks.

## The Redis chunk store

`redis://[:password@]host:port/db` works with anything that speaks the Redis protocol; tested against Redis 7.4,
Valkey 8.1, and Dragonfly 2.0 (see `kv_servers` in `conftest.py`). `put` is `SET` (atomic), `get` is `GET`, `delete`
is `UNLINK` (frees big values without blocking the server).

| Option | Default | Meaning |
|---|---|---|
| `bitdrop_prefix` | `bitdrop:` | Prepended to every key, so several users can share a server |
| `bitdrop_max_value_bytes` | 512 MiB | Largest chunk; set it to match the server (a memcached-like 1 MiB limit works too) |
| `bitdrop_max_connections` | 32 | Connection pool size; more callers than this wait their turn |
| anything else | | Passed to redis-py (`socket_timeout`, `health_check_interval`, ...) |

**Configure the server as a data-plane node:** no persistence (`--save "" --appendonly no`; the source is the durable
copy, and a snapshot fork can double memory use) and `maxmemory-policy noeviction` (a full server then refuses writes,
which the runner turns into backpressure, instead of dropping chunks). With an eviction policy the store still never
returns wrong data: evicted chunks are reported as missing and extracted again. Not built yet: TLS (`rediss://`),
Redis Cluster, and sharding one job over several servers.

Failures are `StoreFull` (out of memory, including Dragonfly's different wording) or `StoreUnavailable` (unreachable,
authentication, timeouts); passwords never appear in messages.

## Measuring the chunk stores

```bash
make bench     # or: uv run pytest benchmarks -s
```

Writes, reads, and deletes 128 MiB of chunks (1, 16, and 64 MiB each; 1 and 8 in flight) through the `ChunkStore`
contract for every store, and prints one table. See the caveats in `benchmarks/test_chunk_store_throughput.py`: the
servers run in Docker on the same machine, so read the numbers as relative.
