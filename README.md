# bitdrop

Very fast bulk data transfer between databases, using an elastic in-RAM staging tier (any in-memory key:value store)
so extraction and loading run in parallel at the speed of the source and the destination — and the RAM is only paid
for while the job runs.

**Status:** early scaffold. The two storage contracts (`ChunkStore` for data in RAM, `MetadataStore` for chunk states,
leases, and fencing), their conformance kits, and two adapters each (in-memory, filesystem/SQLite) exist and are tested,
including a miniature end-to-end pipeline with a crashing worker. Sources, destinations, and the master process do not
exist yet.

## Quick start

```bash
make sync     # install (needs uv)
make check    # lint + strict types + architecture rules + tests
```

## Documentation

- [docs/development.md](docs/development.md): repository layout, commands, how to add an adapter
- [docs/principles.md](docs/principles.md): binding design principles (every component is exchangeable)
- [docs/ideation.md](docs/ideation.md): the idea, architecture options, risks, experiments
