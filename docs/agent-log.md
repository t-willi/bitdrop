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
make test-fast   # ~1,000 tests, no Docker, about 15 seconds
make demo        # watch a demo job in the TUI (no database, no Docker)
make check       # lint + strict types + architecture rules + every test (needs Docker for ~300 of them)
```

**Who you work with.** The user owns the project, prefers Python, and wants it clean, tested, and documented.
Their standing instructions are in section 7.

## 2. Current state

Everything below is committed and passes `make check`. At the last full run: **1,312 tests collected, 1,289 passed, 23
skipped** (the skips are intentional: kit checks that do not apply to a given adapter), **299 of them need Docker**.
Last feature commit: `577aaaa`.

| Piece | Package / path | State |
|---|---|---|
| Contracts and conformance kits | `bitdrop-api` | `ChunkStore`, `MetadataStore` (with `release`), `Source`, `Destination`, `ChunkCodec`, errors, key rule, URL parsing, Arrow native-type metadata; kits in `bitdrop_api.testing` |
| Core | `bitdrop-core` | `registry.py` (opens components by URL, displays by name, via entry points), `transfer.py` (`run_transfer`: planner, extractors, loaders, reaper, heartbeats, timeouts, backpressure, clean stop, progress observer), `progress.py` (events), `tracker.py` (events to display state), `ui.py` (`ProgressUi` contract), `summary.py`, `humanize.py`, `codec.py` (Arrow IPC, none/lz4/zstd) |
| Chunk stores | `bitdrop-store-memory`, `bitdrop-store-file`, `bitdrop-store-redis` | `memory://`, `file:///dir` (use a tmpfs for RAM), `redis://` (Redis, Valkey, Dragonfly; same kit passes on all three) |
| Metadata stores | `bitdrop-metadata-memory`, `bitdrop-metadata-sqlite` | `memory://`, `sqlite:///file.db` (persistent, multi-process on one host) |
| Connectors | `bitdrop-connector-memory`, `bitdrop-connector-postgres` | `memory://` source+destination (`demo_rows`, `delay_ms`: a generated table and slow chunks, so no database is needed); `postgresql://` source (primary-key range splitting) and destination (ledger, native types) |
| Command line | `bitdrop-cli` | the `bitdrop` command: `run`, `plan`, `status`, `components`; strict TOML job file with `${ENV}` in URLs; `plain` and `json` displays; exit codes 0/1/2/3/70/130; Ctrl-C and SIGTERM hand chunks back |
| Terminal UI | `bitdrop-tui` | a live Textual screen (progress and estimate, rows/s sparklines, staging fill, chunk map, workers, events); `q`, Ctrl-C, or SIGTERM stop the job cleanly; the default on a terminal |
| Cross-package tests | `tests/integration`, `tests/e2e`, `tests/architecture` | swap tests, runner with injected faults and clean stops, real PostgreSQL to PostgreSQL (also through the `bitdrop` command, with a Ctrl-C and a resume), architecture rules, this log's checks |
| Examples | `examples/demo.toml` | a job that needs no database; checked by a test, so it cannot rot |
| Benchmarks | `benchmarks/test_chunk_store_throughput.py` | `make bench`: chunk store throughput table |
| Tooling | `scripts/mutation_check.sh`, `scripts/add_workspace_package.py` | break-the-code-on-purpose check (stops a hanging run); register a new workspace package (`--top` for front ends; tested against the real pyproject) |

**Verified end to end** (real PostgreSQL containers; a fingerprint of every row plus the column types match on both
sides): a 200,000-row, 13-column table in about 0.4 s (roughly 500k rows/s); 26 column types bit for bit; a loader
killed after its commit; a run killed halfway and resumed; the staging server wiped mid-job; staging in Redis, Valkey,
and Dragonfly; the `bitdrop` command between two PostgreSQL databases, including a load stopped part-way with Ctrl-C
and finished by the same command (no duplicate row, nothing left held). Details and numbers: `docs/ideation.md`,
section 12.

**What the user can run:** `make demo` (the TUI, no database), `uv run bitdrop plan examples/demo.toml`,
`uv run bitdrop run examples/demo.toml --ui plain`, `uv run bitdrop components`, `uv run pytest tests/e2e -s`
(end-to-end with throughput printed), `make bench`. The plan and the findings behind the CLI and TUI are in
`docs/cli-plan.md`.

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
| 2026-10-16 | Commit small and often: one logical change per commit, each green on its own | The user's instruction after the CLI and TUI session produced a few large commits; small ones are easy to revert |
| 2026-10-16 | `run_transfer` takes an optional observer that receives typed events; it only watches (an observer that raises is logged once and ignored) | One feed for every display; a broken UI must never change what reaches the destination |
| 2026-10-16 | `MetadataStore.release(claim)` hands a chunk back without counting a failure; cancelling `run_transfer` releases every held chunk, including a claim granted while the worker was being cancelled | A stop used to leave leases (60 s by default) that blocked an immediate resume and cost one of three attempts each |
| 2026-10-16 | Displays are components found by name (entry point group `bitdrop.uis`, contract `ProgressUi`); `bitdrop-cli` and `bitdrop-tui` are separate packages above the core that import no adapter and not each other | P1; role images never need a UI toolkit; adding a display changes no line of the command |
| 2026-10-16 | Job configuration is a strict TOML file; URLs may contain `${NAME}`; no message repeats a value from the file; URLs in errors go through `redact_url` | Passwords stay out of files, logs, and tracebacks (Typer's rich tracebacks are off because they print locals) |
| 2026-10-16 | Progress goes to standard error and the result to standard output; `--ui json` prints events on standard output; the runner's own log lines are not repeated by the displays | Scripts can pipe the result; no line is printed twice |
| 2026-10-16 | Exit codes: 0 done, 1 chunks failed, 2 bad configuration, 3 refused by a component, 70 bug, 130 stopped | Schedulers and scripts can tell the outcomes apart |
| 2026-10-16 | The TUI is a live monitor first; the setup wizard, live worker knobs, and attaching to another process's job wait for their prerequisites (see the backlog) | Each needs a contract or governor that does not exist yet |
| 2026-10-16 | Demo data lives in the memory connector (`demo_rows`, `delay_ms`), not in a new package or in the CLI | The CLI may not name a connector; this keeps the demo and the CLI tests free of Docker |

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
| 2026-10-16 | `fdead4c` | Agent log, `CLAUDE.md`, helper scripts |
| 2026-10-16 | `60de219` | Progress events and observer in `run_transfer`; `docs/cli-plan.md` |
| 2026-10-16 | `f44c4dd` | `MetadataStore.release`; a cancelled run hands its chunks back |
| 2026-10-16 | `7e300a9` | `JobTracker`, number formatting, the `ProgressUi` contract and registry lookup; demo data and delays in the memory connector; public `redact_url` |
| 2026-10-16 | `74e6e69` | The `bitdrop` command; found and fixed the lost late claim with a real-process signal test |
| 2026-10-16 | `577aaaa` | The TUI, `examples/demo.toml`, real-database tests of the command, tooling fixes |
| 2026-10-16 | `b0cc9f6` | Agent log and docs for the CLI and TUI |
| 2026-10-16 | `c202940` | Project instructions ask for small, frequent commits |
| 2026-10-16 | `f194f46` | Plan for persisting the chunk plan, written into the log before any code |

Method used throughout: write the test, watch it fail for the right reason, implement, then break the code on purpose
(`scripts/mutation_check.sh`) to prove the tests would notice. This found real weaknesses (a kit that hung instead of
failing, a race test that caught a non-atomic write only 4 times in 5).

## 5. Backlog

Nothing here is started unless marked. Roughly in the order I would do it.

**IN PROGRESS: persist the chunk plan** (backlog item 1). A session that finds this block continues at the first
unticked step; tick a step in the same commit that finishes it. Every step is one commit that passes `make check`.

Design (decided 2026-10-16):

- The metadata store keeps the plan as **opaque bytes**, so adapters know nothing about Arrow or sources (P1).
  Two new operations on `MetadataStore`: `save_plan(job_id, plan)` stores the bytes only if the job has none yet and
  returns the bytes that are stored (first writer wins, so two masters cannot disagree); `load_plan(job_id)` returns
  them, or `None`.
- Turning a `SourcePlan` into bytes and back is `bitdrop_api.plan` (`encode_plan`, `decode_plan`): JSON with a format
  version, the Arrow schema as base64 IPC (keeps field metadata, so native-type hints survive), the chunk specs, the
  estimates, the notes. A payload that is not JSON is refused loudly at encode time (P2); garbage or an unknown version
  raises `CorruptPlan`.
- `run_transfer` first calls `load_plan`. If a plan is stored it is used and `Source.plan` is not called: a resumed job
  reads exactly the chunks it registered, even if the table changed meanwhile. If the stored plan is for another source
  table the run is refused (`PlanMismatch`: use a new job id). Otherwise it plans, calls `save_plan`, and uses what
  `save_plan` returns. The plan is saved *before* `register_chunks`, so a crash between the two leaves a plan and no
  chunks, which is harmless.
- The source kit gains a check that every chunk payload survives a JSON round trip unchanged (tuples would silently
  become lists otherwise).

Steps:

- [x] 0. This plan, committed first so a handover is always possible.
- [x] 1. Housekeeping: section 4 says "this commit" for the CLI/TUI log update; give it its real hash and add `c202940`.
- [x] 2. `bitdrop_api.plan` with tests: round trip (schema metadata, nested types, payloads, notes, estimates), garbage,
      unknown version, non-JSON payload.
- [x] 3. Source kit: payloads survive a JSON round trip; make sure both connectors pass.
- [x] 4. `MetadataStore.save_plan` / `load_plan` in the contract, the kit, and the memory and SQLite adapters (one
      commit: the kit tests fail for an adapter without them; watch them fail first).
- [ ] 5. `run_transfer` loads, saves, and resumes from the plan; integration tests: resume after the table changed uses
      the saved chunks, another table is refused, no `Source.plan` call on resume.
- [ ] 6. Mutation checks (`scripts/mutation_check.sh`) on steps 2 to 5; strengthen tests for any survivor.
- [ ] 7. Update this log (sections 2, 3, 4, 5, 6), `docs/development.md` if it lists contract methods; full `make check`
      with Docker; set the test counts.

Then continue with the rest of "Next up".

**Next up**

1. *(in progress, see above)* **Persist the chunk plan**, so extractors and loaders can run as separate processes or
   containers. Today `run_transfer` keeps the plan in its own memory.
2. **Role entry points and containers**: one image per role (master, extractor, loader), Kubernetes manifests. P1 says
   every function gets its own container; none exists yet.
3. **Memory governor**: byte-credit admission control, a hard `maxmemory` backstop, scaling workers. Today only simple
   backpressure exists (an extractor waits while the staging store is full). The `Governor` and `Provisioner` contracts
   listed in `docs/principles.md` are not built.
4. **A networked `MetadataStore`** (Redis and/or PostgreSQL) for multi-host runs; SQLite is single-host.

**CLI and TUI follow-ups** (each waits for the item named)

- A setup wizard that browses tables and writes the job file: needs `Source.list_tables` in the contract, both
  connectors, and the kit.
- Attach the TUI to a job run by another process (`bitdrop watch`): needs rows and bytes in the metadata store and the
  persisted plan (item 1 above).
- Change worker counts while running: needs the memory governor (item 3).
- `bitdrop status` shows counts only; listing the failed chunks needs a list-by-state operation on `MetadataStore`.
- Windows: signal handling falls back silently (no handlers), untested. The TUI was tested headless and in a
  pseudo-terminal on macOS only.

**Connectors and stores**

- More databases: MySQL, ClickHouse, Parquet/S3. Each needs its own splitting strategy and type mapping.
- PostgreSQL: snapshot-consistent reads (`pg_export_snapshot`), `ctid` range splitting for tables without an integer
  primary key, adding constraints and indexes after the load, a workaround for NULL array elements (stage as text),
  and reporting that driver bug upstream.
- Chunk stores: memcached adapter; Redis TLS (`rediss://`), Cluster, and sharding one job over several servers;
  Garnet (should work through the Redis adapter, untested); Arrow Flight peer-to-peer.
- A `ChunkCodec` registry by URL (the codec is injected as an object today).

**Operations**

- Metrics and per-chunk timings (progress events exist; logging is still plain text).
- A verification step inside the runner (row counts and checksums per chunk); today verification lives in the e2e tests.
- Incremental sync and CDC (out of scope so far).

**Experiments** (numbering from `docs/ideation.md`, section 10): H2 decoupling versus direct streaming, H4 row-size
estimator on wide-column data, H5 pod-local tmpfs versus a dedicated store, H6 compression, and H1 with many concurrent
client processes over a real network. H1 is only answered for one client on one machine.

**Questions for the user** (decide before the work that depends on them): which database after PostgreSQL? Where will
it run (Kubernetes, which cloud)? Open source or product (license, Redis versus Valkey)? (CLI and config style was
decided: TOML job file, Typer, Textual; see `docs/cli-plan.md`.)

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
- Resuming needs a metadata store that survives the process (`sqlite://`); with `memory://` a stopped job cannot be
  continued. The command says so when it stops.
- A stop does not interrupt a call that already runs in a thread: a PostgreSQL `COPY` under way finishes (and commits)
  before the process exits. The chunk is handed back and the next run skips it through the destination ledger.
- The TUI's chunk map and worker table are built from events, so a lease voided by the reaper shows only when the chunk
  is started again; the counts come from the metadata store and are authoritative.
- `bitdrop status` shows counts only, not which chunks failed.

## 7. Working rules

These come from the user and from lessons in this project.

- **Test first.** Write the test, run it, see it fail for the right reason, then implement. Then try to break the code
  on purpose with `scripts/mutation_check.sh` and confirm a test notices.
- **Tests the user can run** with plain commands (`make test-fast`, `make check`, `uv run pytest tests/e2e -s`).
- **Clean, documented code** in a structured layout: one uv-workspace package per component, docstrings on everything
  public, steps documented in `docs/`.
- **Commit without asking** once the checks pass, with a descriptive message ending in the co-author trailer. Never skip
  `make check`.
- **Commit small and often.** One logical change per commit, each passing `make check` on its own, so any step can be
  reverted alone. A test goes in with the code that makes it pass; a refactor, a tooling fix, a doc update, and a bug
  found on the way are separate commits. Do not batch a session's work into one big commit.
- **Tell the user when a milestone test succeeds** (for example the first real source-to-destination copy).
- **P1**: a component never depends on another's implementation; new backends are new packages with a conformance test.
  **P2**: store a value exactly or refuse it loudly.
- **Never touch Docker containers the tests did not start.** The user's machine runs other containers.
- **No secrets in messages**: URLs are redacted (`***`) in every error.
- To add a package: create it like an existing one, then `uv run python scripts/add_workspace_package.py <dist> <module>`
  (`--top` for a front end) and `uv sync`; the remaining steps are in `docs/development.md`.

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
- **mypy and test names**: pytest tolerates two `test_app.py` in different packages, mypy does not ("duplicate module");
  give test files unique names across the repository.
- **Backend-name scan**: string literals count as code, so CLI help text and messages must not spell a backend
  (`postgresql`, `sqlite`, ...). Examples belong in docstrings and docs. `bitdrop_cli` and `bitdrop_tui` are scanned.
- **Textual**: `run_async` swallows a cancellation, so `asyncio.wait_for` cannot make a stuck screen fail a test; use a
  clock and `pytest.fail` (see `present_promptly` in `packages/bitdrop-tui/tests/test_screen.py`). The test pilot's
  `auto_pilot` does not close the app when it returns. Read a panel's text with `Static.content` and a log with
  `RichLog.lines`.
- **Signals**: only a test that sends a real signal to a real process found that a claim granted during cancellation
  was lost (`packages/bitdrop-cli/tests/test_signals.py`); in-process cancellation tests cannot.
- **Mutation checks can hang**: a mutant that breaks shutdown makes the run hang; `mutation_check.sh` now stops it after
  `MUTATION_TIMEOUT` seconds and counts it as killed. Do not run several mutants in one foreground command.
- **zsh** stops a command chain when a glob matches nothing (`rm -f x*`); avoid globs in `&&` chains.
- **pydantic** errors can carry the offending input; the job file formatter prints only the location and the message, and
  the model is configured to hide inputs as a second layer.
- **Observed once, not explained**: the Valkey 16 MiB round trip of the Redis conformance kit failed once in a full
  `make check` and passed on every rerun.
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
6. Commit a log update with the small change it describes; a larger refresh of the log is a commit of its own.

`tests/architecture/test_agent_log.py` fails if a required section disappears, if a package under `packages/` is not
mentioned, or if the log points at a file that does not exist. Passing it does not prove the log is complete; that part
is on you. Keep entries short and factual, and mark anything untested as untested.
