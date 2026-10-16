# bitdrop

Very fast bulk data transfer between databases, using an elastic in-RAM staging tier (any in-memory key:value store)
so extraction and loading run in parallel at the speed of the source and the destination — and the RAM is only paid
for while the job runs.

**Status:** early scaffold. The `ChunkStore` contract, its conformance kit, and two adapters (in-memory, filesystem)
exist and are tested. Sources, destinations, and the master process do not exist yet.

## Quick start

```bash
make sync     # install (needs uv)
make check    # lint + strict types + architecture rules + tests
```

## Documentation

- [docs/development.md](docs/development.md): repository layout, commands, how to add an adapter
- [docs/principles.md](docs/principles.md): binding design principles (every component is exchangeable)
- [docs/ideation.md](docs/ideation.md): the idea, architecture options, risks, experiments
