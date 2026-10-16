# The CLI and the TUI: plan

Agreed with the user on 2026-10-16. This is the plan for backlog item 1 of [agent-log.md](agent-log.md): a command
line tool (`bitdrop`) with a live terminal UI (TUI). It records what was found while reading the code, the order of
work, and the decisions, so a later session can continue where this one stopped.

## Findings that shaped the plan

1. **The runner has no progress feed.** `run_transfer` returns only at the end, and its row and byte counters are
   private. `MetadataStore.counts()` gives chunk totals, but not rows, bytes, or which worker holds which chunk. The CLI
   output and the TUI therefore both need a small event hook in the core.
2. **There is no clean way to stop.** An interrupted run leaves its leases behind. The default lease is 60 s, so a quick
   re-run waits for them to expire, and every expired lease burns one of the chunk's attempts. (The resume tests only
   pass because they use leases of 0.3 to 1.0 s.) `MetadataStore` needs a `release(claim)` that hands a chunk back
   without counting a failure. `docs/principles.md` already promises that every image "drains leases on SIGTERM".
3. **The backend-name scan treats string literals as code**, so CLI help text such as an example URL with a database
   name would fail `tests/architecture/test_core_is_backend_agnostic.py`. Examples belong in docstrings and docs.
4. **Textual owns the terminal.** The runner's `logging` output must go to a pane or a file while the TUI is running,
   never to stderr.

## What the TUI is (version 1)

A live monitor for one running job: overall progress and ETA, rows per second for extraction and loading (sparklines),
how full the staging tier is, a map of every chunk by state, what each worker is doing, and a scrolling list of
failures. `q` stops safely: leases are handed back, and the same command resumes the job.

Out of scope for version 1, each with the prerequisite that already sits in the backlog:

| Later feature | Needs first |
|---|---|
| Setup wizard that browses tables | `Source.list_tables` in the contract and both connectors |
| Changing worker counts while running | The memory governor (agent-log backlog item 4) |
| Attaching to a job run by another process | Rows and bytes in the metadata store, and the persisted plan (backlog item 2) |

## Decisions

| Decision | Choice | Why |
|---|---|---|
| TUI scope | Live monitor first | Everything bigger has a prerequisite that is not built yet |
| Job configuration | A TOML file; URLs may use `${ENV_VAR}` so passwords stay out of the file | Standard library parser; secrets stay out of files and logs |
| Libraries | Typer (CLI), pydantic (validation, as `principles.md` says), Textual (TUI) | Typed, all resolve on Python 3.14 |
| Packaging | `bitdrop-cli` and `bitdrop-tui` are separate workspace packages; the CLI finds the TUI through an entry point | Role images never ship a UI toolkit; the CLI never imports the TUI (P1) |
| Stopping | Add `MetadataStore.release(claim)`; `SIGINT` and `SIGTERM` stop gracefully | Resuming right after a stop must be immediate and must not burn attempts |

## Build order

Each step is test first, ends with `make check`, and is committed.

1. **Progress events** in `bitdrop-core` (`progress.py`): plan ready, chunk state changes, periodic snapshot. An
   observer that raises must never affect the transfer.
2. **`MetadataStore.release`** in the contract, the conformance kit, and both adapters; the runner releases its claims
   when it is cancelled.
3. **`bitdrop-cli`**: the job file model, `bitdrop components`, `bitdrop plan` (a dry run that moves no data),
   `bitdrop run` with plain and JSON-lines progress, `bitdrop status`. Exit codes tell success, failed chunks, bad
   configuration, and a refused plan apart.
4. **Real PostgreSQL through the CLI** in the end-to-end tests, including an interrupt followed by a resume.
5. **`bitdrop-tui`**: a pure view model (events in, display state out), the widgets, and tests driven by Textual's
   headless pilot. `--ui auto` picks the TUI on a terminal and plain output otherwise.
6. **Documentation and guards**: `docs/development.md`, the README, the agent log, and the new packages in the
   import-linter layers and the backend-name scan.

## Status

Updated as the work proceeds; see the agent log for the authoritative state.

- [x] 1 progress events
- [x] 2 release on stop
- [ ] 3 CLI
- [ ] 4 end-to-end through the CLI
- [ ] 5 TUI
- [ ] 6 documentation and guards
