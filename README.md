# bitdrop

Very fast bulk data transfer between databases, using an elastic in-RAM staging tier (any in-memory key:value store)
so extraction and loading run in parallel at the speed of the source and the destination — and the RAM is only paid
for while the job runs.

**Status:** working end to end for PostgreSQL to PostgreSQL. A table is planned into chunks, extracted in parallel,
staged in RAM, and loaded in parallel; crashes, hung calls, lost staging data, and repeated runs are survived without
losing or duplicating a row (a SQL fingerprint of every row proves the copy). Chunks can be staged in memory, on disk, or
in a Redis, Valkey, or Dragonfly server over the network. The `bitdrop` command runs a job from a TOML file
with a live terminal UI, and Ctrl-C stops it cleanly so the same command resumes it. Not yet: memcached,
other databases, snapshot-consistent reads. See
[docs/ideation.md](docs/ideation.md#12-results-so-far-2026-10-16) for measurements and findings.

## Quick start

```bash
make sync        # install (needs uv)
make test-fast   # ~930 tests, no Docker needed, about 10 seconds
make check       # lint + strict types + architecture rules + every test (PostgreSQL ones need Docker)
```

See it work, with no database and no Docker (a generated table moves between two in-memory connectors, slowly
enough to watch; `q` stops it):

```bash
make demo
uv run bitdrop plan examples/demo.toml                  # how the table would be cut into chunks
uv run bitdrop run examples/demo.toml --ui plain        # the same without the full-screen UI
```

A real job is a TOML file (see [docs/development.md](docs/development.md#running-a-transfer)):
`uv run bitdrop run job.toml`.

Watch a real PostgreSQL-to-PostgreSQL transfer (starts a throwaway container):

```bash
uv run pytest tests/e2e -s
```

## Documentation

- [docs/agent-log.md](docs/agent-log.md): what is done, what is not, and why (the handoff log for new sessions)
- [docs/development.md](docs/development.md): repository layout, commands, how to add an adapter
- [docs/principles.md](docs/principles.md): binding design principles (every component is exchangeable)
- [docs/ideation.md](docs/ideation.md): the idea, architecture options, risks, experiments
