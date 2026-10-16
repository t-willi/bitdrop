# bitdrop — ideation

Status: **draft 1** · 2026-10-15 · nothing is decided; this is the thinking-out-loud document.

Binding design principles (starting with: every component is exchangeable) live in [principles.md](principles.md).

## 1. Pitch

A bulk data-movement tool (think Airbyte, but for *throughput*) that puts an **elastic, in-RAM staging tier** between
source and destination. Extraction and loading are decoupled, scale independently, and are coordinated through a small
metadata store. RAM is rented in short, large bursts, so a very large full load finishes quickly without paying for
that capacity the rest of the month.

Target property: wall-clock time ≈ `data_size / min(source_read_rate, destination_write_rate)`. Nothing else in the
pipeline should be the bottleneck.

## 2. Core hypothesis

> A RAM buffer that is scaled to the job lets both the source side and the destination side run at their own maximum
> rate at the same time, and is cheap because it exists only for the duration of the job.

What the buffer can and cannot do — worth being precise about, because it shapes the marketing and the design:

- It **cannot** beat `D / min(s, d)`. That is the physical ceiling.
- It **can** keep both ends saturated even when they have different speeds, different burst behaviour, or different
  failure/retry timing.
- It **can** shorten the window in which the *source* is under load (the source is read at full speed, then released
  while the slower destination drains the buffer). This is likely the most compelling real-world benefit: production
  OLTP databases tolerate a 20-minute hammering far better than a 4-hour one.

### Sizing rule of thumb

Let `D` = dataset bytes, `s` = source read rate, `d` = destination write rate (bytes/s), `s > d`:

```
peak buffered bytes  ≈  D × (1 − d/s)
```

Example: D = 2 TB, s = 2 GB/s, d = 500 MB/s → source finishes after ~1000 s, destination after ~4000 s, peak RAM ≈
1.5 TB. If `d ≥ s` the buffer stays tiny (just in-flight slack) and the tool degenerates into a plain parallel stream
— which is fine, and means RAM is only ever as large as it needs to be.

A bounded buffer is also fine: when RAM is full, extractors block (backpressure) and total time becomes
`D / d`. RAM size trades *source-window length* against *cost*.

## 3. Architecture sketch

```
                       ┌───────────────────────────────┐
                       │   Master / coordinator        │
                       │  planner · memory governor    │
                       │  reaper · progress · scaler   │
                       └──────┬─────────────────┬──────┘
                              │                 │
                  ┌───────────▼──────┐   ┌──────▼───────────┐
                  │ Metadata Redis   │   │ Data Redis(es)   │
                  │ job/table/chunk  │   │ chunk blobs      │
                  │ state, leases    │   │ no persistence   │
                  └───▲───────────▲──┘   └──▲────────────▲──┘
                      │           │         │            │
        claim/ack     │           │  put    │            │  get+delete
   ┌──────────────────┴──┐     ┌──┴─────────┴──┐     ┌───┴─────────────────┐
   │  Extractor workers  │ ... │               │ ... │   Loader workers    │
   │  (N conns to source)│     └───────────────┘     │ (M conns to dest)   │
   └──────────▲──────────┘                           └──────────┬──────────┘
              │                                                 │
          ┌───┴────┐                                       ┌────▼────┐
          │ Source │                                       │  Dest   │
          └────────┘                                       └─────────┘
```

Components:

| Component | Job |
|---|---|
| **Planner** | Inspect source, estimate bytes/row, split each table into chunks, write the chunk plan to the metadata store. |
| **Memory governor** | Hands out *byte credits* to extractors; keeps data-plane usage under the threshold. |
| **Extractors** | Claim a `PENDING` chunk, read it from the source, serialize, `SET` into the data store, mark `EXTRACTED`. |
| **Loaders** | Claim an `EXTRACTED` chunk, fetch from the data store, write to the destination, mark `LOADED`, delete the blob, return credits. |
| **Reaper** | Re-queues chunks whose lease expired (worker died). |
| **Scaler** | Adds/removes worker connections (and, optionally, provisions data-plane nodes) based on throughput and RAM headroom. |

Workers are stateless; the master is restartable because all state lives in the metadata store.

The diagram shows the Redis-style dedicated data store (option C in section 7); sections 6–7 cover the alternatives.

## 4. Chunk lifecycle

**The chunk — not the row — is the unit of extraction, buffering, loading, and retry.** Per-row status in Redis would
cost more than the data movement itself. A chunk is a key range / row-count-bounded slice of a table (order of
16–128 MB serialized; this also keeps well under Redis' 512 MB value limit and gives good pipelining).

```
PENDING ──claim──▶ EXTRACTING ──put──▶ EXTRACTED ──claim──▶ LOADING ──commit──▶ LOADED
   ▲                   │                    │                   │
   └──── lease expiry / worker death ───────┴───────────────────┘      (+ FAILED after N retries)
```

- Transitions are atomic compare-and-set (Lua script or `WATCH/MULTI`) and carry a lease with a heartbeat.
- A chunk that is `EXTRACTED` but whose blob is missing (data Redis lost) is simply reset to `PENDING`. **The source is
  the durable copy**, so the data plane never needs persistence — that is what makes RAM-only safe.

### Duplicate prevention — the part that needs care

Metadata Redis alone cannot guarantee "no duplicate rows": a loader can commit to the destination and die *before* it
marks the chunk `LOADED`. The reaper then re-queues it and the rows land twice. Options (can be combined):

> **Status (2026-10-16): built and verified.** The chunk state machine, leases, and fencing are the `MetadataStore`
> contract; option 1 below (a ledger in the destination, written in the same transaction as the data) is implemented in
> the PostgreSQL destination. An end-to-end test against a real PostgreSQL makes a loader commit a chunk and then
> die: the chunk is redone, the ledger recognises it, and the destination holds every row exactly once.

1. **Ledger in the destination**: write `chunk_id` into a `_bitdrop_chunks` table *in the same transaction* as the
   data. Loader checks/inserts the ledger row atomically with the load.
2. **Idempotent chunk load**: `DELETE WHERE chunk range` + `INSERT`, or upsert on primary key, or a deterministic
   per-chunk file name for `COPY`-from-object-storage style destinations.
3. **Load into a per-chunk temp table, then atomic swap/insert-select** guarded by the ledger.

Principle: *the destination is the source of truth for "was this loaded"; Redis is the coordination hint.*

## 5. Load flow (refined from the original 8 steps)

1. **Preflight** — check RAM availability on the data plane (and what could be provisioned if elastic mode is on),
   check source/destination reachability and permissions.
2. **Estimate** — bytes/row per table from catalog stats, corrected by sampling (see risks).
3. **Plan** — choose a snapshot mode, split tables into chunks, derive a target concurrency from RAM, source
   connection limit, and destination parallelism.
4. **Extract** — N extractors pull chunks into the data store; each acquires byte credits first.
5. **Load** — M loaders drain `EXTRACTED` chunks into the destination. Independent of N.
6. **Track** — all state transitions in metadata Redis, as above.
7. **Govern** — master keeps data-plane memory under the threshold (credits + hard `maxmemory`/`noeviction` backstop),
   throttles or scales workers.
8. **Finish** — all chunks `LOADED` → verify (row counts / checksums per chunk) → release resources → exit.

## 6. Hard problems and risks

**Data-plane throughput.** Redis executes commands on one thread, and every byte crosses the network twice
(extractor → Redis → loader). With MB-sized chunks the limit is socket I/O, memcpy bandwidth, and the NIC rather than
command dispatch (see 7.1). "Limited only by source and destination" is only true if the data plane is *not* the
bottleneck. Plan: shard chunks across K data nodes (client-side hashing or Redis Cluster) and benchmark the stores in
7.2. Treat any per-node GB/s figure as an assumption until measured.

**Python on the hot path.** Python is the main language. This works because the design moves chunks (16–128 MB), not
rows: at 10 GB/s that is only a few hundred chunk operations per second, which is trivial orchestration overhead. The
rule that makes it hold is *no per-row Python*: data stays in Arrow buffers from the source driver to the destination
driver, and native libraries move the bytes (pyarrow, ADBC drivers or ConnectorX, psycopg `COPY`, hiredis-backed
clients). Things to design for:

- **GIL**: driver calls, (de)compression, and socket I/O release it, so threads or asyncio are enough for I/O fan-out;
  CPU-heavy steps belong in native code or worker processes. Do not rely on free-threaded builds.
- **Scaling** is by containers anyway (more pods, not more threads in one process), which suits Python.
- **Copies**: a Python client can copy a 64 MB value several times (command building, socket buffers). Use
  `memoryview` and pyarrow buffers wherever the client allows, and measure client-side overhead in H1.
- **Memory**: pyarrow's memory pool tracks Arrow allocations; the governor must account for Python-side buffers too.
- **Escape hatch**: a hot adapter (codec, store client) can later become a native extension behind the same contract.

**Is Redis the right data plane at all?** Not as a given. Put a `ChunkStore` interface in front and compare the
options in section 7. Object-storage staging (S3/GCS Parquet) remains the durable, cheap, slower baseline and is the
natural path for warehouse destinations.

**Memory accounting.**
- Row-size estimates from catalog stats (`pg_class.reltuples/relpages`, `avg_row_length`) are wrong for wide
  variable-length columns (text/jsonb/blob). Sample, and reconcile with *actual* serialized size after each chunk.
- Redis overhead is real: fragmentation, client output buffers, replication buffers. Keep 20–30 % headroom, **disable
  RDB/AOF on the data nodes** (fork + copy-on-write can nearly double memory), `maxmemory-policy noeviction`.
- Credits (admission control at the extractor) are the primary mechanism; `maxmemory` is only the safety net.

**Splitting the source.** Needs a per-engine strategy: PK range (with histogram/sampling for skew), Postgres `ctid`
block ranges, Oracle `ROWID`, hash-mod on PK, native partitions. Tables without a usable key fall back to a single
stream. Skewed chunks need adaptive splitting / work-stealing.

**Consistency.** Choose per job:
- *Snapshot-consistent* — e.g. Postgres `pg_export_snapshot()` shared across all N connections; MySQL needs a brief
  global lock or a single consistent-read transaction. Correct, but pins an old snapshot on the source for the whole
  extraction window (another reason to make that window short).
- *Fuzzy fast* — each chunk read independently. Fine for append-only data; incorrect for mutating tables unless
  followed by a CDC/incremental catch-up.

**Source protection.** "As many connections as RAM allows" is the wrong limit — RAM is not what a production database
runs out of. Concurrency must be capped by `source max_connections − headroom`, an operator-set load ceiling, and an
adaptive controller (grow while aggregate throughput rises, back off on latency/errors; AIMD-style). Default must be
safe.

**Destination write paths differ a lot.** Postgres `COPY BINARY`; ClickHouse native/Arrow insert; Snowflake / Redshift /
BigQuery / Databricks want files in object storage plus a bulk-load command. For warehouses the loader will stage
files anyway — the RAM tier's job there is to assemble well-sized files without hammering the source longer.

**Data model and libraries (decided 2026-10-16).** bitdrop moves data; it does not transform it, so the question is
what format is in flight, not which dataframe library to compute with. Decision: **Apache Arrow via pyarrow is the one
canonical format** for types, in-flight batches, chunk encoding (Arrow IPC), and Parquet I/O.

| Library | Role |
|---|---|
| pyarrow | Canonical in-flight format (record batches), IPC chunk encoding, Parquet I/O. Required. |
| ADBC, ConnectorX | Drivers that read/write Arrow directly. Used inside source/destination adapters. |
| pandas | Not on the hot path (copies, weaker type fidelity, memory heavy). Only at the edge of an adapter whose driver forces it. |
| Polars, DuckDB | Not needed in v1. Candidates for later, optional transform/verify components behind a contract. |

- Contracts are typed on Arrow (`Source` yields record batches, `Destination` consumes them). Arrow is the one shared
  data model; it is a language-neutral standard, and its C stream interface keeps adapters free to use pyarrow, Polars,
  or a native extension.
- Source type mapping into Arrow (Oracle `NUMBER`, MySQL unsigned ints, Postgres `numeric` without precision,
  timezone-aware timestamps, `jsonb`, arrays) is per-adapter work and needs explicit tests.
- Compression trade-off: zstd/lz4 shrink the RAM footprint (RAM is the cost driver) but cost CPU, and `mmap`
  zero-copy reads only work on uncompressed IPC files. `ChunkCodec` is exchangeable so this can be tuned per job (H6).

**Connector breadth.** Airbyte's moat is hundreds of connectors. We cannot compete on count. The wedge is *depth
and speed on a few* sources/destinations; breadth is a later question (possibly wrapping Singer/Airbyte connectors
for non-critical paths, accepting their lower throughput).

**Metadata durability.** Chunk-state traffic is tiny (e.g. 10 TB / 64 MB ≈ 160 k chunks). Turn persistence *on* for
the metadata Redis if we want resumable jobs after a master + Redis crash; otherwise a crash = rerun. Cheap to
decide later.

**Licensing.** Redis changed its license in 2024 (RSAL/SSPL; AGPL option re-added in Redis 8). Valkey is the BSD fork.
Relevant if this becomes a product — check current terms before committing.

## 7. Data plane options

*Added after the 2026-10-15 discussion. Nothing here is decided.*

### 7.1 Single-threaded Redis matters less than it sounds

At 16–64 MB per chunk, 10 GB/s is only about 160 operations per second. Command dispatch is not the limit; socket I/O,
memcpy bandwidth, and the NIC are. Multi-threaded stores help mainly through I/O threading, not command parallelism.
Side effect to remember: a large `GET` blocks other clients on that shard while it is copied out, a second reason to
keep metadata on a separate instance.

### 7.2 Candidate in-RAM stores

| Store | Threading | Notes |
|---|---|---|
| Dragonfly | Shared-nothing, multi-threaded, vertical scaling | Redis API compatible; BSL license |
| Valkey 8+ | I/O threads; command execution still mostly one thread | BSD; sharding via cluster |
| Garnet (Microsoft) | Multi-threaded, RESP-compatible, .NET | MIT; built for high-throughput batched traffic |
| Memcached | Multi-threaded | Simplest blob store; no persistence; client-side sharding; default 1 MB item limit is configurable |
| KeyDB | Multi-threaded Redis fork | Appears stagnant; verify before betting on it |
| NATS JetStream (memory streams) | Multi-threaded Go | Claim/ack/redelivery for free; built for small messages |
| Ignite / Hazelcast / Alluxio | Distributed in-memory grids (JVM) | Heavier; bring their own partitioning and rebalancing |

Shortlist to benchmark: **Dragonfly, Valkey cluster, memcached**. Vendor throughput figures are unverified until
measured here; licenses need a check before committing.

### 7.3 Shared memory in Kubernetes

- **Same pod: works well.** Containers in a pod share an IPC namespace. A memory-backed `emptyDir` (tmpfs) mounted
  into both containers is a shared buffer: the extractor writes an Arrow IPC file, the loader `mmap`s it, and the
  data never touches the network. `/dev/shm` defaults to 64 MB, so mount an `emptyDir` with `medium: Memory` over it
  and size it explicitly.
- **Across pods or nodes: not possible.** RAM is per node. Sharing between pods on one node needs `hostPath` or
  `hostIPC`, which weakens isolation and conflicts with restricted pod-security policies. Avoid.
- **Accounting.** tmpfs usage counts against container memory limits, so the memory governor must track it. The
  buffer dies with the pod, which is acceptable because the source is the durable copy.

### 7.4 Three architectures

| | Data path | Scales by | Trade-off |
|---|---|---|---|
| **A. Pod-local tmpfs** | extractor → tmpfs → loader, same pod | More pods, each owning disjoint chunk ranges | Fastest, no DB; buffer size is tied to the pod/node |
| **B. Peer-to-peer (Arrow Flight)** | Extractors hold chunks in RAM; loaders pull over gRPC | Extractor and loader fleets scale independently | No DB; extractors are stateful until drained |
| **C. Dedicated store** | extractor → Dragonfly / Valkey / memcached → loader | Buffer, extractors, loaders all independent | One extra network hop each way |

"Every function in its own container" holds for all three. In A, a pod is one extractor container plus one loader
container sharing the tmpfs; the master adjusts each container's connection count at runtime, so no pod restarts are
needed. The master and the metadata Redis run as ordinary Deployments. Getting "a huge-RAM node for 20 minutes" comes
from a node autoscaler (e.g. Karpenter) provisioning memory-optimized nodes for the job.

### 7.5 Current leaning

Build **A** first behind the `ChunkStore` interface: cheapest, fastest per node, one fewer component. Build **C**
(Dragonfly) alongside and benchmark the two against each other (H5). Move to **B** only if independent
extractor/loader scaling turns out to matter in practice.

## 8. How this differs from what exists

| Tool | Shape | Where bitdrop would differ |
|---|---|---|
| Airbyte | Container-per-connector, record-oriented protocol between source and destination, huge catalog | Throughput-first, columnar chunks, decoupled elastic extract/load fleets |
| Sling, dlt | Stream source → destination directly, usually one pipeline | No independently scaled buffer tier; no "release the source early" |
| ConnectorX | Partitioned parallel read into Arrow, source side only | We add the load side + coordination + RAM tier |
| Spark JDBC | `partitionColumn` splits, needs a cluster | Lighter, single-purpose, no cluster to run |
| AWS DMS / pgcopydb | Managed / engine-specific full-load parallelism | Engine-agnostic, self-hostable, explicit RAM budget |

The honest one-sentence differentiator: **an explicitly budgeted, elastic RAM staging tier with independently scaled
extract and load fleets, billed as a short burst.**

## 9. Scope

**v1 (prove the hypothesis)**
- Full load only, single table → many tables.
- One source + one destination (suggest Postgres → ClickHouse or Postgres → Postgres, plus Parquet/S3 as a baseline).
- Chunk state machine, memory governor, reaper, destination ledger, row-count verification.
- A benchmark harness — the benchmark *is* the product claim.

**Explicitly later**
- Incremental / cursor-based sync, CDC, schema evolution, transformations, UI, scheduling, multi-tenant.

## 10. Falsifiable claims → first experiments

| # | Claim | Experiment |
|---|---|---|
| H1 | A dedicated in-memory data plane sustains ≥ the faster of source/dest for 16–64 MB chunks | Producers/consumers benchmark across Redis, Valkey, Dragonfly, memcached; 1 node vs sharded; chunk size sweep; use the real Python clients, since client-side copies count |
| H2 | Decoupling reduces *source-hold time* and/or total time vs direct streaming | Postgres → slow-ish destination, compare bitdrop vs a direct pipe (Sling/pgcopydb) at equal parallelism |
| H3 | Chunk state + destination ledger gives zero duplicates and zero losses under crashes | Chaos test: `kill -9` random workers/master/data node mid-run, verify row counts + checksums |
| H4 | Row-size estimator is within ±30 % on realistic schemas | Run against a few wide-column datasets; compare estimate vs actual serialized bytes |
| H5 | Pod-local tmpfs (A) vs dedicated store (C): where is the crossover? | Same job, `ChunkStore` swapped, vary dataset size and dest speed |
| H6 | Compression pays for itself: RAM saved outweighs CPU cost and lost zero-copy | Same job with uncompressed, lz4, zstd IPC; compare ratio, CPU, and end-to-end time on real column mixes |

If H1 fails badly, the architecture changes (in-process buffer or sharding becomes mandatory). Cheapest and most
decisive experiment to run first.

## 11. Open questions for the owner

1. **Primary use case** — one-off migrations / initial loads, recurring full refreshes, or a path to incremental/CDC?
2. **Where it runs** — ephemeral cloud (which cloud?), on-prem, or developer laptop → big VM?
3. **First source/destination pair** to build and benchmark against?
4. ~~**Language**~~ — **decided: Python** (2026-10-16). See "Python on the hot path" in section 6 and
   [principles.md](principles.md).
5. **Open source vs product** — affects license choices (Redis vs Valkey) and connector strategy.
6. **Elastic provisioning** — should bitdrop itself spin up and tear down the data-plane nodes, or just consume
   whatever RAM it is pointed at (and leave provisioning to Terraform/k8s)?
7. **Delivery semantics** — is exactly-once at the destination a hard requirement, or is at-least-once + idempotent
   destination acceptable?
8. **Buffer span** — is a buffer bounded by one (very large) node per pod acceptable, or must a single job's buffer
   span multiple nodes? This decides whether option A is enough or B/C is required.
9. **Plugin mechanism** — in-process adapters (fast, one language) or out-of-process plugins over gRPC/Arrow Flight
   (language-agnostic, isolated, extra hop)? See open questions in [principles.md](principles.md).

## 12. Results so far (2026-10-16)

What exists and what has been measured. Numbers come from `uv run pytest tests/e2e -s` on a laptop, with
PostgreSQL in a local container on both ends and an in-process staging tier, so they say nothing about
network-bound, multi-node performance. They show the pipeline's own overhead is small.

| Fact | Evidence |
|---|---|
| A 200,000-row, 13-column table moves PostgreSQL to PostgreSQL in 53 chunks in about 0.4 s (about 500k rows/s, about 110 MiB/s through the staging tier) | `tests/e2e/test_postgres_to_postgres.py::test_a_wide_table_arrives_exactly` |
| The copy is exact: row count and an md5 over every row's text match, and so do the column types | same test, plus the type matrix below |
| 26 column types (ints, floats incl. `NaN`/`Infinity`, text up to 100 KB, Unicode, dates, timestamps, `bytea`, `uuid`, `json`/`jsonb`, `inet`, `interval`, `numeric(p,s)`, arrays) arrive bit for bit | `test_every_supported_type_survives_the_trip` |
| A loader that commits and then dies causes no duplicate rows | `test_a_loader_dying_after_its_commit_causes_no_duplicate_rows` |
| A run killed mid-job resumes to the identical result (staging lost or kept) | `test_a_killed_run_resumes_to_the_same_result` |
| Hung calls (source, destination, *and* chunk store) time out, slow reads keep their lease through heartbeats, a full staging store applies backpressure, a lost blob is re-extracted | `tests/integration/test_transfer_runner.py` |
| Staging in a real Redis, Valkey, or Dragonfly server over the network gives the same exact copy at about 425k rows/s, 98 MiB/s (identical on all three at this scale) | `tests/e2e/...::test_a_wide_table_through_a_network_chunk_store` |
| Wiping the staging server mid-job (a crash or restart) loses no row and duplicates none; the lost chunks are extracted again | `...::test_wiping_the_staging_server_mid_job_...` |

Findings that changed the design:

- `adbc_ingest` into a table it creates degrades types (`numeric` becomes `text`, `jsonb` becomes `json`). The
  destination therefore creates tables itself from the source's native types, carried as Arrow field metadata.
- `numeric` arrives as an opaque string; `numeric(p,s)` with `p <= 38` is cast to `decimal128` and loads losslessly.
  Unconstrained `numeric` has no lossless Arrow type and is refused at planning.
- The installed ADBC COPY writer stores `NULL` array elements as `0` or `''` without an error. bitdrop refuses
  such chunks (principle P2). Worth reporting upstream; a staged-text workaround is possible.

- The network chunk store exposed a leak: a loader that died *after* marking a chunk loaded but *before* deleting
  its staged copy stranded the blob in the server for good (nobody revisits a loaded chunk). The runner now deletes
  the staged copy before recording completion; the cost of a crash in the other window is one re-extraction.
- A stalled connection to a networked store would have hung a worker forever, because its heartbeat keeps the lease
  alive. Chunk store calls now share the per-call timeout of source and destination.
- Redis and Valkey report a full server as `OutOfMemoryError`; Dragonfly answers a plain `Out of memory`. The adapter
  recognises both.

**H1 (data-plane throughput), first answer** (`make bench`, single Python client process, loopback to a Docker
container on macOS; indicative only): Redis, Valkey, and Dragonfly each move 1 to 64 MiB chunks at roughly
1.3 to 3.4 GiB/s, with no meaningful difference between them. That is 10 to 30 times what the whole pipeline moves
today (about 100 MiB/s through staging), so on one machine the data plane is not the bottleneck. The benchmark cannot
show what multi-threaded Dragonfly buys: that needs many concurrent client processes, and a real network.

Experiments from section 10: **H3** (crash safety) is validated at the connector level; **H4** (row-size estimates) is
implemented by sampling but not yet checked on wide-column data; **H1** is partly answered (above); **H2, H5, H6** (decoupling benefit, pod-local vs dedicated store, compression) are still
open and need a real network to mean anything.

Known gaps: no snapshot-consistent reads, no destination constraints/indexes, splitting only by an integer primary key
(or a hint), no memory governor beyond simple backpressure, no CLI, no memcached adapter, no Redis TLS/Cluster/sharding.
