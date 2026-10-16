# bitdrop — agent log

The handoff document between work sessions: what exists, what was decided and why, what is not done yet.
A new session reads sections 1 and 2 first, checks 5 for what to do next, and updates this file before it ends
(section 9). It is checked by `tests/architecture/test_agent_log.py`, so it cannot silently fall behind the code.

Last updated: 2026-10-16

## 1. Start here

**What bitdrop is.** A bulk data-movement tool (think Airbyte, built for throughput). A table is cut into chunks;
extractors read the chunks in parallel from the source, stage them in RAM (a chunk store), and loaders write them in
parallel to the destination. A metadata store tracks every chunk (states, leases, retries), and the destination records
each loaded chunk in a ledger, so crashes lose nothing and duplicate nothing. RAM is meant to be rented in short,
large bursts, so a huge full load finishes fast and cheaply.

**Read in this order:**

1. This file, sections 2 and 5.
2. `docs/principles.md`: the binding rules (P1 every component is exchangeable, P2 never silently alter data).
3. `docs/development.md`: layout, commands, how to add an adapter, how to run a transfer.
4. `docs/ideation.md`: the original idea and design options; section 12 holds the measured results.

**First commands in a fresh checkout:**

```bash
make sync        # install (uv; Python 3.14)
make test-fast   # ~700 tests, no Docker, about 5 seconds
make check       # lint + strict types + architecture rules + every test (needs Docker for ~300 of them)
```

**Who you work with.** The user owns the project, prefers Python, and wants it clean, tested, and documented.
Their standing instructions are in section 7.

## 2. Current state

Everything below is committed and passes `make check`. At the last full run: **1,002 tests collected, 979 passed, 23
skipped** (the skips are intentional: kit checks that do not apply to a given adapter), **297 of them need Docker**.
Last feature commit: `5425f67`.

| Piece | Package / path | State |
|---|---|---|
| Contracts and conformance kits | `bitdrop-api` | `ChunkStore`, `MetadataStore`, `Source`, `Destination`, `ChunkCodec`, errors, key rule, URL parsing, Arrow native-type metadata; kits in `bitdrop_api.testing` |
| Core | `bitdrop-core` | `registry.py` (opens components by URL via entry points), `transfer.py` (`run_transfer`: planner, extractors, loaders, reaper, heartbeats, timeouts, backpressure), `codec.py` (Arrow IPC, none/lz4/zstd) |
| Chunk stores | `bitdrop-store-memory`, `bitdrop-store-file`, `bitdrop-store-redis` | `memory://`, `file:///dir` (use a tmpfs for RAM), `redis://` (Redis, Valkey, Dragonfly; same kit passes on all three) |
| Metadata stores | `bitdrop-metadata-memory`, `bitdrop-metadata-sqlite` | `memory://`, `sqlite:///file.db` (persistent, multi-process on one host) |
| Connectors | `bitdrop-connector-memory`, `bitdrop-connector-postgres` | `memory://` source+destination; `postgresql://` source (primary-key range splitting) and destination (ledger, native types) |
| Command line | `bitdrop-cli` | the `bitdrop` command: `run`, `plan`, `status`, `components`; TOML job file; plain and JSON-lines progress; exit codes; Ctrl-C and SIGTERM hand chunks back |
| Cross-package tests | `tests/integration`, `tests/e2e`, `tests/architecture` | swap tests, runner with injected faults, real PostgreSQL to PostgreSQL, architecture rules, this log's checks |
| Benchmarks | `benchmarks/test_chunk_store_throughput.py` | `make bench`: chunk store throughput table |
| Tooling | `scripts/mutation_check.sh`, `scripts/add_workspace_package.py` | break-the-code-on-purpose check; register a new workspace package |

**Verified end to end** (real PostgreSQL containers; a fingerprint of every row plus the column types match on both
sides): a 200,000-row, 13-column table in about 0.4 s (roughly 500k rows/s); 26 column types bit for bit; a loader
killed after its commit; a run killed halfway and resumed; the staging server wiped mid-job; staging in Redis, Valkey,
and Dragonfly. Details and numbers: `docs/ideation.md`, section 12.

**What the user can run:** `uv run pytest tests/e2e -s` (end-to-end with throughput printed), `make bench`.
A transfer is also a `bitdrop run job.toml` away (see `docs/cli-plan.md`; the TUI is still to come).

## 3. Decisions

| Date | Decision | Why |
|---|---|---|
| 2026-10-15 | An elastic in-RAM staging tier between source and destination, with metadata kept separately | The original idea: decouple extraction from loading and pay for RAM only during the job |
| 2026-10-16 | Python is the main language | The user is comfortable with it; the heavy lifting happens in native libraries (pyarrow, ADBC) and the design moves chunks, not rows |
| 2026-10-16 | Apache Arrow (pyarrow) is the one canonical data model; pandas stays off the hot path | Lossless, columnar, zero-copy into drivers; Polars/DuckDB possible later as optional components |
| 2026-10-16 | P1: every component is exchangeable, enforced by code (contracts package, conformance kits, import-linter, backend-name scan) and by URL-selected adapters | The user's explicit design rule; "any in-memory key-value store must work" |
| 2026-10-16 | Contracts are async (asyncio); blocking drivers run in `asyncio.to_thread` | Matches "load asynchronously"; cheap concurrency |
| 2026-10-16 | `MetadataStore` is domain-level (register/claim/heartbeat/complete/fail/release_expired), not a generic KV | Claiming "one pending chunk" needs an index by state; leases plus fencing tokens make crashes and zombie workers harmless |
| 2026-10-16 | The chunk is the unit of extraction, staging, loading, and retry | Row-level tracking would cost more than the transfer |
| 2026-10-16 | Duplicate protection lives in the destination: a ledger row keyed `(job_id, table, chunk_id)` in the same transaction as the data | Redis/metadata alone cannot close the "committed, then died" window |
| 2026-10-16 | The destination builds tables from the source's native types, carried as Arrow field metadata; hints are whitelisted before reaching DDL | Letting the driver create tables degraded `numeric` to text and `jsonb` to json |
| 2026-10-16 | P2: never silently alter data (refuse unsupported types at plan time; refuse chunks with NULL array elements) | The end-to-end type matrix found the driver turning NULL array elements into `0` or `''` |
| 2026-10-16 | Splitting is the source connector's job (`Source.plan`); PostgreSQL splits by integer primary key | Each database has its own best strategy; the kit enforces disjoint, complete chunks |
| 2026-10-16 | One adapter serves every Redis-protocol server; run them with persistence off and `noeviction` | The source is the durable copy; a snapshot fork can double memory; a full server should push back, not drop chunks |
| 2026-10-16 | The loader deletes the staged blob before marking the chunk complete; all chunk store calls share `chunk_timeout` | Deleting after completion leaked blobs on a crash; a stalled connection would otherwise hang a worker whose heartbeat keeps its lease alive |
| 2026-10-16 | The user allows committing without asking first | Their instruction; still run `make check` before every commit |

## 4. Session history

| Date | Commit | What happened |
|---|---|---|
| 2026-10-15 | `b57d95c` | Ideation document from the user's idea: architecture, risks, experiments |
| 2026-10-15 | `e1c635f` | Data-plane options: Redis-like stores, shared memory in Kubernetes, three architectures |
| 2026-10-16 | `9196c5a` | Design principles (P1); Python and Arrow decided (`4020969` is the user's own uv setup) |
| 2026-10-16 | `23c07cd` | uv workspace, `ChunkStore` contract, conformance kit, memory and file adapters, import-linter, backend-name scan |
| 2026-10-16 | `f9edf7a` | `MetadataStore` contract and kit, memory and SQLite adapters, miniature pipeline test |
| 2026-10-16 | `2c30197`, `3f47101` | `Source`, `Destination`, `ChunkCodec` contracts; Arrow native-type metadata |
| 2026-10-16 | `ba40d20` | Source and destination kits, in-memory connectors, Arrow IPC codec, `run_transfer` with fault-injection tests |
| 2026-10-16 | `180202b` | PostgreSQL connector (ADBC), tested against a real PostgreSQL in Docker |
| 2026-10-16 | `95fe6c3` | End-to-end PostgreSQL to PostgreSQL tests; found the NULL-array-element driver bug; P2 |
| 2026-10-16 | `5425f67` | Redis-protocol chunk store (Redis, Valkey, Dragonfly); timeouts for chunk store calls; blob-leak fix; benchmark |
| 2026-10-16 | this commit | Agent log, `CLAUDE.md`, helper scripts |

Method used throughout: write the test, watch it fail for the right reason, implement, then break the code on purpose
(`scripts/mutation_check.sh`) to prove the tests would notice. This found real weaknesses (a kit that hung instead of
failing, a race test that caught a non-atomic write only 4 times in 5).

## 5. Backlog

Nothing here is started unless marked. Roughly in the order I would do it.

**Next up**

1. **CLI and job config file** (for example `bitdrop run job.toml`). The runner is only a function today.
2. **Persist the chunk plan** (the `ChunkSpec` payloads are already JSON-friendly) in the metadata store, so
   extractors and loaders can run as separate processes or containers. Today `run_transfer` keeps the plan in its own
   memory.
3. **Role entry points and containers**: one image per role (master, extractor, loader), Kubernetes manifests. P1 says
   every function gets its own container; none exists yet.
4. **Memory governor**: byte-credit admission control, a hard `maxmemory` backstop, scaling workers. Today only simple
   backpressure exists (an extractor waits while the staging store is full). The `Governor` and `Provisioner` contracts
   listed in `docs/principles.md` are not built.
5. **A networked `MetadataStore`** (Redis and/or PostgreSQL) for multi-host runs; SQLite is single-host.

**Connectors and stores**

- More databases: MySQL, ClickHouse, Parquet/S3. Each needs its own splitting strategy and type mapping.
- PostgreSQL: snapshot-consistent reads (`pg_export_snapshot`), `ctid` range splitting for tables without an integer
  primary key, adding constraints and indexes after the load, a workaround for NULL array elements (stage as text),
  and reporting that driver bug upstream.
- Chunk stores: memcached adapter; Redis TLS (`rediss://`), Cluster, and sharding one job over several servers;
  Garnet (should work through the Redis adapter, untested); Arrow Flight peer-to-peer.
- A `ChunkCodec` registry by URL (the codec is injected as an object today).

**Operations**

- Structured logging, progress and metrics, per-chunk timings.
- A verification step inside the runner (row counts and checksums per chunk); today verification lives in the e2e tests.
- Incremental sync and CDC (out of scope so far).

**Experiments** (numbering from `docs/ideation.md`, section 10): H2 decoupling versus direct streaming, H4 row-size
estimator on wide-column data, H5 pod-local tmpfs versus a dedicated store, H6 compression, and H1 with many concurrent
client processes over a real network. H1 is only answered for one client on one machine.

**Questions for the user** (decide before the work that depends on them): which database after PostgreSQL? Where will
it run (Kubernetes, which cloud)? CLI and config style? Open source or product (license, Redis versus Valkey)?

## 6. Known limitations

- No snapshot isolation: chunks are read at different moments. Each row is read at most once, but concurrent inserts,
  deletes, or key changes may or may not appear.
- Unsupported PostgreSQL types are refused at planning: `money`, unconstrained `numeric` (and `p > 38`), ranges, enums,
  geometry, xml, and more. Supported types are listed in `docs/development.md`.
- Arrays containing NULL elements are refused (the driver would store `0` or `''`).
- Tables without an integer primary key (or a `split_column` hint) are read as a single chunk.
- Destination tables are created without indexes or constraints.
- Chunk sizes rely on `reltuples` (or a `count(*)` when never analysed) and a row-width sample; skewed data gives uneven
  chunks.
- The plan exists only inside the running process (see backlog item 2); a restart re-plans, which is only safe if the
  table is unchanged.
- Throughput numbers come from one laptop with local containers; they say nothing about network-bound performance.

## 7. Working rules

These come from the user and from lessons in this project.

- **Test first.** Write the test, run it, see it fail for the right reason, then implement. Then try to break the code
  on purpose with `scripts/mutation_check.sh` and confirm a test notices.
- **Tests the user can run** with plain commands (`make test-fast`, `make check`, `uv run pytest tests/e2e -s`).
- **Clean, documented code** in a structured layout: one uv-workspace package per component, docstrings on everything
  public, steps documented in `docs/`.
- **Commit without asking** once the checks pass, with a descriptive message ending in the co-author trailer. Never skip
  `make check`.
- **Tell the user when a milestone test succeeds** (for example the first real source-to-destination copy).
- **P1**: a component never depends on another's implementation; new backends are new packages with a conformance test.
  **P2**: store a value exactly or refuse it loudly.
- **Never touch Docker containers the tests did not start.** The user's machine runs other containers.
- **No secrets in messages**: URLs are redacted (`***`) in every error.
- To add a package: create it like an existing one, then `uv run python scripts/add_workspace_package.py <dist> <module>`
  and `uv sync`; the remaining steps are in `docs/development.md`.

## 8. Gotchas

- **pytest** runs with `--import-mode=importlib`: test files may share basenames, but tests cannot import sibling
  modules. There is exactly one `conftest.py` (repo root); tests reference its classes only under `TYPE_CHECKING`
  (`from conftest import ...`), and a second conftest breaks mypy's module resolution. Shared fixtures (`pg`,
  `postgres_url`, `kv_servers`, `kv`) live there.
- **conftest** registers `bitdrop_api.testing` for assertion rewriting, so it must not import that package at module
  level; import it inside functions.
- **mypy**: strict. `pyarrow-stubs` is a dev dependency (generics such as `pa.Field[Any]`; a few `chunked_array` calls
  need a documented `type: ignore`). ADBC ships no types, so only `adbc_driver_*` is exempt.
- **ruff** formats code blocks inside Markdown too (documentation snippets must be formatted); line length is 100,
  relaxed for the SQL-heavy `tests/e2e` and `benchmarks`.
- **ADBC / PostgreSQL**: `numeric` arrives as an opaque string extension; `adbc_ingest` into a table it creates degrades
  types; qualified names need `db_schema_name`; NULL array elements are corrupted on write; use `to_regclass` to test
  for a table.
- **URLs**: `urlsplit` silently strips raw newlines (test the percent-encoded form); component URLs for local adapters
  have no host (`parse_url`), network ones do (`parse_network_url`); `bitdrop_*` query options belong to bitdrop.
- **Redis family**: Dragonfly words out-of-memory differently and reports a non-zero `maxmemory` by default, so tests
  restore the original value instead of `0`; run test servers with persistence off. Images are pinned in `conftest.py`.
- **Docker**: containers use random ports and are removed at the end of the session; `docker` tests are skipped, not
  failed, when Docker is unavailable. `testcontainers` is imported as `testcontainers.community.*`.
- **Timing tests** use real, very short leases and sleeps. Loops in kits are bounded so a broken implementation fails
  instead of hanging.
- **Memory of the agent**: the assistant's private memory is machine-local and holds only the working style. This file
  and the repository are the source of truth.
- Temporary helper scripts written outside the repository are lost between sessions; keep anything reusable in `scripts/`.

## 9. Keeping this log current

At the end of every session or milestone, before the final commit:

1. Set `Last updated:` at the top to today's date.
2. Update section 2: the table, the test counts (run `uv run pytest --collect-only -q` and `make check`), the last
   feature commit.
3. Add a row to section 4 for each commit, and replace any "this commit" row from the previous session with its
   real hash (a commit cannot contain its own hash).
4. Move finished items out of section 5 (delete them; the history is in section 4) and add what you discovered.
5. Record decisions in section 3, new limits in section 6, new traps in section 8.
6. Commit the log together with the code it describes.

`tests/architecture/test_agent_log.py` fails if a required section disappears, if a package under `packages/` is not
mentioned, or if the log points at a file that does not exist. Passing it does not prove the log is complete; that part
is on you. Keep entries short and factual, and mark anything untested as untested.
