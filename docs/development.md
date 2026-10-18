# Developing bitdrop

How the repository is laid out, how to run everything, and how to extend it. For *why* it is built this way, read
[principles.md](principles.md); for the product thinking, [ideation.md](ideation.md); for what is done and what is
not, [agent-log.md](agent-log.md).

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
| `make demo` | Watch a demo job run in the TUI (no database, no Docker) |
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
├── docs/                   ideation.md, principles.md, cli-plan.md, this file
├── examples/               Job files to try (demo.toml needs no database)
├── packages/               One uv-workspace package per component
│   ├── bitdrop-api/          Contracts + shared types + the conformance kit. Depends on nothing.
│   │   ├── src/bitdrop_api/
│   │   │   ├── chunk_store.py    ChunkStore: where chunks wait in RAM (data plane)
│   │   │   ├── metadata_store.py MetadataStore: chunk states, leases, fencing, the saved plan (coordination)
│   │   │   ├── plan.py           A source plan to bytes and back (what the metadata store keeps)
│   │   │   ├── errors.py         Errors every adapter raises
│   │   │   ├── keys.py           The one key/id rule all backends share
│   │   │   ├── urls.py           Strict parsing of component URLs
│   │   │   └── testing/          Conformance kits (importable base test classes)
│   │   └── tests/
│   │       (also: source.py, destination.py, codec.py, schema.py for the connector contracts)
│   ├── bitdrop-core/             Imports no adapter. Finds them by URL scheme and runs the transfer.
│   │   └── src/bitdrop_core/
│   │       ├── registry.py       open_source / open_destination / open_chunk_store / open_metadata_store / open_ui
│   │       ├── transfer.py       run_transfer: plan, extract, stage, load, recover (master + workers)
│   │       ├── progress.py       The events a running transfer reports to an observer
│   │       ├── tracker.py        JobTracker: events in, display state out (totals, rates, estimate, chunk map)
│   │       ├── ui.py             The ProgressUi contract: something that shows a running transfer
│   │       ├── summary.py        The end-of-run text every display prints
│   │       ├── humanize.py       Bytes, counts, rates, durations for people
│   │       └── codec.py          Arrow IPC chunk codec (none / lz4 / zstd)
│   ├── bitdrop-store-memory/     ChunkStore adapter: a dict in this process
│   ├── bitdrop-store-file/       ChunkStore adapter: one file per chunk (point it at a tmpfs)
│   ├── bitdrop-store-redis/      ChunkStore adapter for the Redis protocol: Redis, Valkey, Dragonfly
│   ├── bitdrop-metadata-memory/  MetadataStore adapter: dicts in this process
│   ├── bitdrop-metadata-sqlite/  MetadataStore adapter: a SQLite file (persistent, multi-process)
│   ├── bitdrop-queue-redis/      ChunkQueue adapter on Redis Streams (Redis, Valkey, Dragonfly); needs Docker to test on real servers
│   ├── bitdrop-queue-memory/     ChunkQueue adapter: dicts in this process (the queue workflow, not wired in yet)
│   ├── bitdrop-marker-redis/     MarkerStore adapter: `SET NX` on a Redis-protocol server; needs Docker to test on real servers
│   ├── bitdrop-marker-memory/    MarkerStore adapter: a dict in this process (the queue workflow, not wired in yet)
│   ├── bitdrop-connector-memory/ Source + Destination over in-memory Arrow tables
│   ├── bitdrop-connector-postgres/ Source + Destination for PostgreSQL (ADBC); needs Docker to test
│   ├── bitdrop-cli/              The `bitdrop` command: job file, run / plan / status / components, plain and JSON displays
│   └── bitdrop-tui/              The live terminal screen (Textual), found by the command through an entry point
├── benchmarks/             Throughput measurements (not part of the normal test run)
├── scripts/                Developer tools: mutation_check.sh, add_workspace_package.py
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
| Core, contracts, the command line tool, and the TUI never name a concrete backend in code | `tests/architecture/test_core_is_backend_agnostic.py` |
| The command line tool and the TUI never import an adapter, and do not import each other | `make arch` (a layer above the core, and a forbidden-imports contract) |
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

Two rules from the saved plan. A `MetadataStore` keeps the plan bytes exactly as given and the first save wins
(`save_plan`: a Redis `SET NX`, a PostgreSQL `INSERT ... ON CONFLICT DO NOTHING`); the kit checks it, including
two writers at once. A `Source` puts only values in its `ChunkSpec.payload` that JSON keeps exactly (text, numbers,
booleans, null, lists, dicts with text keys; not tuples, dates, or sets): the plan is saved as JSON, and the source
kit fails a payload that would change.

A `Source`'s chunk ids say what to read. `chunk_from_id(table, chunk_id)` turns an id back into the chunk without
planning or I/O, so a worker handed only an id can read it (the queue workflow in
[decoupled-queue-workflow.md](decoupled-queue-workflow.md)). The format is the source's own; the core treats an id as
an opaque key. Each chunk has exactly one spelling, and `chunk_from_id` refuses every other (`InvalidChunkId`),
because the destination ledger is keyed by the id and a second spelling would load the same rows twice. The id must
still obey the key rule, so a source falls back to an encoded form when its natural description does not fit. The
source kit checks the round trip.

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

### The `bitdrop` command

Try it first: `make demo` moves a generated table between two in-memory connectors, slowly enough to watch,
with no database and no Docker. A real transfer is described by a TOML job file (`examples/demo.toml` is
a complete one):

```toml
[job]
id = "orders-1"                  # names the job; running the same id again resumes it

[source]
url = "${SOURCE_URL}"            # scheme://... picks the connector; ${NAME} reads the environment
table = "public.orders"
split_column = "id"              # optional hint for sources that split by a key

[destination]
url = "${DESTINATION_URL}"
table = "orders"

[staging]                        # where chunks wait in RAM
url = "memory://?max_bytes=4000000000"

[metadata]                       # chunk states, leases, retries: use one that survives the process
url = "sqlite:///var/lib/bitdrop/orders-1.db"

[tuning]                         # all optional
extractors = 4
loaders = 4
chunk_mib = 64
```

A misspelled key or section is an error, never a silent default. Put passwords in the environment and refer
to them as `${NAME}`: they stay out of the file, and no message the command prints repeats a value from it.

| Command | What it does |
|---|---|
| `bitdrop run JOB.toml` | Run the job. `--ui auto\|tui\|plain\|json`, `--wait`, `--job-id`, `--extractors`, `--loaders`, `-v` |
| `bitdrop plan JOB.toml` | Show how the source table would be cut into chunks. Moves no data, never opens the destination. `--json` |
| `bitdrop status JOB.toml` | Chunk counts as the metadata store has them, whoever is running the job. `--json` |
| `bitdrop components` | The connectors, stores, queues, marker stores, and displays that are installed |
| `bitdrop queue plan JOB.toml` | The queue workflow's planner: chunk the table, publish one chunk id per chunk. `--republish` for a lost work queue |
| `bitdrop queue source JOB.toml` | A source container: read the chunks named in the work queue and stage them. `--name`, `--stop-when-finished`, `--give-up-after` |
| `bitdrop queue destination JOB.toml` | A destination container: load the staged chunks named in the file queue. Same options |
| `bitdrop queue status JOB.toml` | The state of a queue job from the markers and the two queues. `--wait`, `--give-up-after`, `--json` |

**Displays.** `auto` (the default) is the TUI on an interactive terminal when `bitdrop-tui` is installed, plain
text otherwise. `plain` prints a progress line every two seconds to standard error and a summary to standard
output. `json` prints one JSON object per line on standard output (`plan`, `progress`, `chunk_failed`, then
`report`, `stopped`, or `error`) and no URLs. In the TUI, `q` (or Ctrl-C) stops the job and `l` hides the events. An Activity panel shows how many extractors
and loaders are busy out of the configured numbers, and how many containers run them (always 1 for now: every
worker runs inside the one process that runs the job).

**Opening the TUI without starting the load.** `bitdrop run JOB.toml --wait` opens the TUI and moves nothing; press
`s` to start the load, or `q` to leave (the command then prints `not started` and exits with 130). `--wait` works
only with the TUI, because only it can start a job.

**The queue workflow** (`docs/decoupled-queue-workflow.md`) needs `[queues]` (`work` and `files` URLs) and
`[markers]` in the job file instead of `[metadata]`; `staging`, `source` and `destination` are as before. Run
`bitdrop queue plan` once, then any number of `queue source` and `queue destination` containers, which can start in
any order. A container runs until stopped: SIGTERM or Ctrl-C makes it finish the chunk in hand and leave (a second
Ctrl-C hands the chunk back at once). `--stop-when-finished` makes it leave by itself: a source when every chunk is
extracted, a destination when the whole job is over. Exit codes: 0 done, 1 some chunks were set aside or the wait ran
out, 2 bad configuration, 3 refused by a component (a job id chunked differently, for instance). `[tuning]`:
`lease_seconds` is how long a chunk may sit untouched before another container takes it over, `max_attempts` how
often it may fail, `poll_seconds` how long a container waits for a new message.

**Docker.** `docker/Dockerfile` builds one image for every role of the queue workflow (the role is the command:
`bitdrop queue plan`, `queue source`, `queue destination`), with the Redis and PostgreSQL adapters and no display
toolkit. `examples/queue/docker-compose.yml` runs a whole job with two PostgreSQL databases and two Redis servers
(staging without persistence, coordination with an append-only file):
`docker compose -f examples/queue/docker-compose.yml up --build`, and `--scale source=6 --scale destination=4` to change
the number of containers. `tests/architecture/test_queue_example.py` checks the files against each other without
Docker, and `tests/e2e/test_role_image.py` (Docker) builds the image and runs the command in it. To add an adapter
to the image, add its package to the list in the Dockerfile. The database health checks in the compose file ask over
TCP (`-h 127.0.0.1`): the database image answers a check over its Unix socket while it is still seeding, before it
accepts connections from the other containers.

**Stopping and resuming.** Ctrl-C, SIGTERM, and `q` stop the job cleanly: every chunk a worker holds is handed
back to the metadata store without counting a failure, and the command exits with 130. Running the same command
again continues where it stopped (a second Ctrl-C quits at once). Resuming needs a metadata store that survives
the process; with `memory://` there is nothing to resume from.

**The saved plan.** The first run of a job saves its plan (the schema and the list of chunks) in the metadata store.
A later run of the same job id reads that plan instead of planning again, so a resumed job finishes exactly the
chunks it started with, even if rows were added to the table meanwhile, and a changed `chunk_mib` or
`split_column` in the job file has no effect on it. A job id belongs to its source table: pointing it at another
table is refused (exit code 3, "use a new job id"). To start over, use a new job id.

| Exit code | Meaning |
|---|---|
| 0 | Every chunk was loaded |
| 1 | The job ended, but some chunks gave up (they are listed) |
| 2 | Bad command line, job file, or URL. Nothing was done |
| 3 | A component refused or could not be reached (unknown table, unsupported column type, ...) |
| 70 | A bug (`-v` prints the traceback) |
| 130 | Stopped on purpose |

**Demo data.** The memory connector can stand in for a database: `memory://?demo_rows=N` (source) serves a
generated table named `demo`, and `memory://?delay_ms=D` (source or destination) makes every chunk take `D`
milliseconds.

### From Python

The command is a thin layer over one function. Everything is chosen by URL:

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
        observer=print,  # optional: receives the events of bitdrop_core.progress
    )
    print(report.succeeded, report.rows_loaded, report.failures)


asyncio.run(main())
```

Running the same `job_id` again against the same metadata store and destination resumes the job: chunks that are
already loaded are skipped, and the destination's ledger makes a repeat harmless even if the metadata is lost.
The job's plan is saved in the metadata store on the first run and reused on every later one (`Source.plan` is not
called again), so the chunks stay the same; `PlanMismatch` is raised if the id was planned for another table.
Cancelling the task that runs `run_transfer` is a clean stop: the workers hand their chunks back first.

The `observer` only watches. It is called on the event loop with typed events (`PlanReady`, `ChunkStarted`,
`ChunkFinished`, `ChunkFailed`, and a `Snapshot` about four times a second and once at the end), it must return
quickly, and if it raises the exception is logged once and ignored: the data arrives exactly as without it.
`JobTracker` turns the events into totals, rates, an estimate, a chunk map, and a worker table; the plain display
and the TUI both use it.

### Adding a display

A display is a component like any other. Create `packages/bitdrop-<name>/` with a factory
`(UiContext) -> ProgressUi`, register it in `[project.entry-points."bitdrop.uis"]`, and add it to the workspace
with `uv run python scripts/add_workspace_package.py bitdrop-<name> bitdrop_<name> --top`. `bitdrop run --ui <name>`
finds it; no line of the command line tool changes.

## The PostgreSQL connector

`postgresql://user:password@host:port/database`. Option `bitdrop_pool_size` (default 4) bounds the connections; all
other query options go to libpq. Passwords are redacted from every error message.

**What it does**

- *Source*: reads the catalog, splits the table by integer primary key into inclusive ranges sized from a sample of
  the row width (sparse keys get wider ranges; the extremes of `bigint` do not overflow). Without an integer primary
  key it reads one chunk, and says so in the plan notes. `split_column` overrides the choice (NOT NULL integer).
  Chunk ids are `<column>.<first>.<last>` (`id.1.10000`), `all` for the whole table, or `x.<text>` (the payload as
  URL-safe base64 JSON) for a column name the key rule forbids. The memory connector uses `rows.<offset>.<length>`.
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

## Developer scripts

| Script | Use |
|---|---|
| `scripts/mutation_check.sh <file> <tests> '<old>' '<new>'` | Breaks the code on purpose, runs the tests, restores the file. Exit 0 means a test noticed (good), 1 means the mutant survived (the tests are too weak). A run that hangs is stopped after `MUTATION_TIMEOUT` seconds (default 120) and counts as noticed. |
| `uv run python scripts/add_workspace_package.py <dist> <module> [--top]` | Registers a new package in the root `pyproject.toml` (dependencies, uv sources, ruff, mypy, import-linter). `--top` is for front ends such as the command line tool and displays. Aborts without writing if anything is off. |

## The agent log

`docs/agent-log.md` is the handoff document for the next working session (human or AI): current state, decisions with
reasons, backlog, known limits, and gotchas. Update it with every milestone; `tests/architecture/test_agent_log.py`
fails if it loses a section, forgets a package, or points at a file that no longer exists.
