# bitdrop — design principles

Binding for all design and code from 2026-10-16. Changing a principle needs an explicit decision, recorded here.

## P1. Every component is exchangeable

No component may depend on the concrete implementation of another. Swapping an implementation is a **configuration
change (and a different image)**, never a code change in the core or in another component.

### Components and their contracts

| Component | Contract (port) | Example implementations |
|---|---|---|
| Source connector | `Source`: plan/split a table, read a chunk as record batches | In-memory, PostgreSQL (built); MySQL, … (planned) |
| Destination connector | `Destination`: prepare, write a chunk idempotently (ledger), count | In-memory, PostgreSQL (built); ClickHouse, Parquet/S3, … (planned) |
| Chunk store (data plane) | `ChunkStore`: put/get/delete a blob by key | In-memory, file/tmpfs, Redis protocol (Redis, Valkey, Dragonfly) (built); memcached, Arrow Flight peers (planned; Garnet should work through the Redis adapter but is untested) |
| Metadata store | `MetadataStore`: save the job's plan; register / claim / heartbeat / complete / fail chunks, with leases and fencing | In-memory, SQLite (built); Redis, Postgres, etcd (planned) |
| Chunk queue | `ChunkQueue`: publish, receive (waiting up to a time), reclaim, touch, ack, release, dead-letter; at-least-once | In-memory (built); Redis Streams (planned) |
| Chunk codec | `ChunkCodec`: encode/decode record batches | Arrow IPC, optionally lz4 / zstd (built; injected as an object, no URL registry yet) |
| Memory governor | `Governor`: admission control over byte credits | credit-based, … |
| Progress display | `ProgressUi`: observer of a running transfer, shown to a person or a program | plain text, JSON lines, full-screen TUI (built); selected by name, not URL |
| Provisioner / scaler | `Provisioner`: add/remove workers and data-plane capacity | Kubernetes, local processes, cloud VMs |

The master, extractors, and loaders are *roles*. They know only the contracts above.

### Rule 1: contracts are the lowest common denominator

The `ChunkStore` contract is deliberately tiny, so that **any in-memory key:value store works**:

```
async put(key, bytes)  overwrite allowed
async get(key)         bytes; ChunkNotFound is a normal result
async delete(key)      idempotent
capabilities()         max_value_bytes, locality (pod | node | cluster), memory_stats
async stats()          optional: used_bytes, max_bytes
async aclose()         release connections
```

The authoritative definition is the code: `packages/bitdrop-api/src/bitdrop_api/chunk_store.py`. A `zero_copy`
capability is intentionally absent until `get` can return a handle instead of `bytes`.

- Nothing Redis-specific (Lua, Streams, pub/sub, server-side TTL, transactions) may be used on the data plane.
  Coordination lives in the metadata store, never in the chunk store.
- Anything beyond the contract is an **optional capability**, declared by the adapter, consumed only through its flag,
  and always with a fallback.
- `max_value_bytes` bounds chunk size (`min(target, max_value_bytes)`); memcached's default item limit is 1 MB,
  Redis' is 512 MB.
- The governor's byte credits are store-agnostic and are the primary memory control. Store `stats()` is an optional
  safety net.
- `locality` is how a pod-local tmpfs store is expressed. The planner/provisioner must place extractors and loaders
  accordingly (same pod); the core does not special-case it.
- A chunk missing from the store (eviction, restart) is handled by resetting it to `PENDING`. Silently corrupted or
  partial data is never acceptable.

The `MetadataStore` contract is **domain-level, not a generic key:value store**. The central operation is "give me
one pending chunk", which needs an index by state; over a plain compare-and-set KV store every claim would scan every
chunk (about 160k for a 10 TB job). So the contract names the operations (`register_chunks`, `claim`, `heartbeat`,
`complete`, `fail`, `release`, `release_expired`, `get_chunk`, `counts`), each adapter implements them with its native atomics
(SQL transactions, Redis Lua, ...), and one conformance kit pins the semantics for all of them. The authoritative
definition is `packages/bitdrop-api/src/bitdrop_api/metadata_store.py`. Its two crash-safety mechanisms:

- **Leases**: a claim is valid for a limited time and renewed by `heartbeat`. If the worker dies, the master's
  `release_expired` puts the chunk back and counts one failure. A worker that stops on purpose (a shutdown signal)
  calls `release` instead, which hands the chunk back at once and counts nothing.
- **Fencing**: every claim carries a number that grows with each claim of that chunk. After a chunk was released and
  claimed again, the previous holder's calls fail with `LeaseLost`, so a slow "zombie" worker can never overwrite the
  new owner's work.

### Rule 2: enforced by code

1. **Contracts package** (`bitdrop-api` or similar): interfaces and shared types only. Zero dependencies on any
   implementation.
2. **Dependency rule**: core and roles depend only on contracts; adapters depend only on contracts plus their own
   client library; adapters never depend on each other. A CI check on the dependency graph fails the build otherwise.
3. **No name-branching in core**: CI fails if core packages mention concrete backends (`redis`, `memcached`, …) or
   branch on adapter identity.
4. **Conformance kit**: a contract test suite ships with the contracts. An adapter is *supported* if and only if it
   passes the kit against a real instance of its backend in a container. The kit covers round-trips at 1 KB, 1 MB,
   declared-max and 64 MB; overwrite; idempotent delete; missing key; concurrent writers and readers; checksum
   integrity; truthful capability declarations; clean errors on oversize values; and behaviour when the store is
   full (loud failure or `NotFound`, never silent corruption).
5. **Versioned contracts**: semver; each component declares the contract version it implements; a startup handshake
   refuses mismatches.

### Rule 3: enforced by containerization

- One component, one image, one responsibility. Roles (master, extractor, loader) are images too.
- Everything is configured through environment, arguments, or mounted config, for example
  `BITDROP_CHUNK_STORE=redis://…`, `memcached://…`, `file:///dev/shm/bitdrop`. No config means no assumption.
- Backing services are **stock upstream images, never forked or patched**. If a store needs a patch, it is not
  compliant.
- Every image exposes health/readiness and metrics, and drains leases on SIGTERM.
- A component manifest (`component.yaml`) declares kind, name, contract version, capabilities, and config schema. It
  is used for deploy-time validation and to drive the CI matrix.
- **Swap test in CI**: the same end-to-end job runs against a matrix of adapters (chunk store × metadata store ×
  source × destination) by changing configuration only. If a swap needs a code change, the contract is wrong.

### What this does not mean

- Not that all combinations perform equally. Each supported adapter is benchmarked (H1, H5 in the ideation doc).
- Not an extra network proxy on the hot path. `ChunkStore` and `ChunkCodec` adapters are libraries linked into the
  extractor/loader; the store itself is the container.

### Implementation language: Python (decided 2026-10-16)

How P1 is realised in Python. The decision is Python; the tooling named below is the proposed default.

- **Contracts**: `typing.Protocol` (or ABCs) in the contracts package, checked with pyright/mypy in strict mode.
- **Data model**: Apache Arrow (pyarrow) is the one shared data type in the contracts: connectors yield and consume
  Arrow record batches, and chunks are encoded as Arrow IPC. pandas is kept off the hot path; Polars/DuckDB are
  possible later as optional components. (Decided 2026-10-16; see the ideation doc, section 6.)
- **Discovery**: `importlib.metadata` entry points (groups such as `bitdrop.chunk_stores`). Each adapter is its own
  distributable package exposing a factory, selected by URL scheme from config. Core never imports an adapter.
- **Dependency rule**: `import-linter` contracts (layers and forbidden imports), run in CI.
- **Conformance kit**: shipped as a base test class (`bitdrop_api.testing.ChunkStoreConformance`). An adapter's tests
  subclass it and override one `store` fixture; pytest then runs every contract test against that adapter. Backends
  that need a server are started through testcontainers.
- **Async contracts**: `ChunkStore` methods are coroutines (asyncio), matching the "load asynchronously" requirement.
  Blocking drivers are wrapped with `asyncio.to_thread` inside their adapter.
- **Config**: pydantic models validate runtime config and the component manifest.
- **Layout**: monorepo as a `uv` workspace, one package per component, one image per role built from the workspace.
- **Hot path**: no per-row Python. Chunks stay in Arrow buffers from the source driver to the destination driver, and
  native libraries move the bytes. If a specific adapter becomes the bottleneck, it can be replaced with a native
  extension (e.g. Rust via PyO3) behind the same contract; P1 makes that a local change.

### Open design questions

1. **Plugin mechanism**: in-process adapters selected at startup (fast, one language) versus out-of-process plugins
   over gRPC/Arrow Flight (language-agnostic, isolated, adds a hop). Leaning: in-process for `ChunkStore`,
   `MetadataStore`, and `ChunkCodec`; connectors start in-process but behind a contract that lets them move
   out-of-process later.

With Python, entry-point discovery makes in-process adapters nearly free, which strengthens the in-process lean.

## P2. Never silently alter data

A component either stores or moves a value **exactly**, or it **refuses loudly**. It never degrades,
rounds, coerces, or "best-efforts" a value into something else without an error. Corrupted data that
looks fine is worse than a failed job.

This was learned, not assumed: an end-to-end test comparing a fingerprint of every row on both sides
found that the PostgreSQL driver turns `NULL` elements inside arrays into `0` or `''` without any
error. Hence the rules below.

- **Fail at planning, not mid-transfer.** A source refuses columns whose type it cannot carry
  losslessly (`UnsupportedType`) before any data moves; a type is "supported" only if the end-to-end
  type matrix proves it round-trips bit for bit.
- **Refuse what you cannot store exactly.** The PostgreSQL destination rejects chunks containing
  `NULL` array elements, because the driver would alter them. The chunk fails with an explicit
  reason; the rest of the job continues.
- **Metadata is input too.** Native type hints from another system end up in DDL, so they are checked
  against a whitelist before use. Identifiers are validated and always quoted.
- **Verify, don't trust.** Transfers are checked with a fingerprint (row count plus an md5 over every
  row's text, in key order) on both sides, and column types are compared.
