# bitdrop

Very fast bulk data transfer between databases, using an elastic in-RAM staging tier (any in-memory key:value store)
so extraction and loading run in parallel at the speed of the source and the destination — and the RAM is only paid
for while the job runs.

**Status:** working end to end for PostgreSQL to PostgreSQL. A table is planned into chunks, extracted in parallel,
staged in RAM, and loaded in parallel; crashes, hung calls, lost staging data, and repeated runs are survived without
losing or duplicating a row (a SQL fingerprint of every row proves the copy). Chunks can be staged in memory, on disk, or
in a Redis, Valkey, or Dragonfly server over the network. Not yet: a CLI, memcached,
other databases, snapshot-consistent reads. See
[docs/ideation.md](docs/ideation.md#12-results-so-far-2026-10-16) for measurements and findings.

## Quick start

```bash
make sync        # install (needs uv)
make test-fast   # ~630 tests, no Docker needed, about 5 seconds
make check       # lint + strict types + architecture rules + every test (PostgreSQL ones need Docker)
```

Watch a real PostgreSQL-to-PostgreSQL transfer (starts a throwaway container):

```bash
uv run pytest tests/e2e -s
```

## Documentation

- [docs/development.md](docs/development.md): repository layout, commands, how to add an adapter
- [docs/principles.md](docs/principles.md): binding design principles (every component is exchangeable)
- [docs/ideation.md](docs/ideation.md): the idea, architecture options, risks, experiments
