# bitdrop — ideation

Status: **draft 0** · 2026-10-15 · nothing is decided; this is the thinking-out-loud document.

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

**Data-plane throughput.** Redis executes commands on one thread; a single instance will be bounded by one core and
the NIC, and every byte crosses the network twice (extractor → Redis → loader). "Limited only by source and
destination" is only true if the data plane is *not* the bottleneck. Plan: shard chunks across K data nodes (client-side
hashing or Redis Cluster), and benchmark multi-threaded Redis-compatible stores (Dragonfly, Valkey with I/O threads).
Treat any per-node GB/s figure as an assumption until measured.

**Is Redis the right data plane at all?** Keep it as the primary design, but put a `ChunkStore` interface in front so
we can A/B it against:

| Option | Pros | Cons |
|---|---|---|
| Redis / Valkey / Dragonfly (proposed) | Decouples extract and load fleets across hosts; elastic via sharding; crash-isolated | Extra network hop ×2; RAM overhead (fragmentation, buffers); serialization cost |
| In-process / same-host buffer (Arrow batches over channels) | Fastest, zero extra hop, simplest | Bounded by one host's RAM; buffer dies with the process; harder to scale loaders independently |
| Object storage staging (S3/GCS Parquet) | Durable, cheap, natural for warehouse loads | Slower; not "RAM-speed" |

A single huge memory-optimized VM running an in-process buffer may beat a Redis cluster for many jobs. We should
measure that rather than assume.

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

**Type system.** Need one canonical intermediate representation. Apache Arrow is the obvious candidate: columnar,
compressible (zstd/lz4), zero-copy to Parquet and to ADBC/COPY paths, and a lot of source drivers already emit it
(ADBC, ConnectorX).

**Connector breadth.** Airbyte's moat is hundreds of connectors. We cannot compete on count. The wedge is *depth
and speed on a few* sources/destinations; breadth is a later question (possibly wrapping Singer/Airbyte connectors
for non-critical paths, accepting their lower throughput).

**Metadata durability.** Chunk-state traffic is tiny (e.g. 10 TB / 64 MB ≈ 160 k chunks). Turn persistence *on* for
the metadata Redis if we want resumable jobs after a master + Redis crash; otherwise a crash = rerun. Cheap to
decide later.

**Licensing.** Redis changed its license in 2024 (RSAL/SSPL; AGPL option re-added in Redis 8). Valkey is the BSD fork.
Relevant if this becomes a product — check current terms before committing.

## 7. How this differs from what exists

| Tool | Shape | Where bitdrop would differ |
|---|---|---|
| Airbyte | Container-per-connector, record-oriented protocol between source and destination, huge catalog | Throughput-first, columnar chunks, decoupled elastic extract/load fleets |
| Sling, dlt | Stream source → destination directly, usually one pipeline | No independently scaled buffer tier; no "release the source early" |
| ConnectorX | Partitioned parallel read into Arrow, source side only | We add the load side + coordination + RAM tier |
| Spark JDBC | `partitionColumn` splits, needs a cluster | Lighter, single-purpose, no cluster to run |
| AWS DMS / pgcopydb | Managed / engine-specific full-load parallelism | Engine-agnostic, self-hostable, explicit RAM budget |

The honest one-sentence differentiator: **an explicitly budgeted, elastic RAM staging tier with independently scaled
extract and load fleets, billed as a short burst.**

## 8. Scope

**v1 (prove the hypothesis)**
- Full load only, single table → many tables.
- One source + one destination (suggest Postgres → ClickHouse or Postgres → Postgres, plus Parquet/S3 as a baseline).
- Chunk state machine, memory governor, reaper, destination ledger, row-count verification.
- A benchmark harness — the benchmark *is* the product claim.

**Explicitly later**
- Incremental / cursor-based sync, CDC, schema evolution, transformations, UI, scheduling, multi-tenant.

## 9. Falsifiable claims → first experiments

| # | Claim | Experiment |
|---|---|---|
| H1 | A Redis-compatible data plane sustains ≥ the faster of source/dest for 16–64 MB chunks | Producers/consumers benchmark across Redis, Valkey, Dragonfly; 1 node vs sharded; chunk size sweep |
| H2 | Decoupling reduces *source-hold time* and/or total time vs direct streaming | Postgres → slow-ish destination, compare bitdrop vs a direct pipe (Sling/pgcopydb) at equal parallelism |
| H3 | Chunk state + destination ledger gives zero duplicates and zero losses under crashes | Chaos test: `kill -9` random workers/master/data node mid-run, verify row counts + checksums |
| H4 | Row-size estimator is within ±30 % on realistic schemas | Run against a few wide-column datasets; compare estimate vs actual serialized bytes |
| H5 | In-process buffer on one big host vs Redis tier: where is the crossover? | Same job, `ChunkStore` swapped, vary dataset size and dest speed |

If H1 fails badly, the architecture changes (in-process buffer or sharding becomes mandatory). Cheapest and most
decisive experiment to run first.

## 10. Open questions for the owner

1. **Primary use case** — one-off migrations / initial loads, recurring full refreshes, or a path to incremental/CDC?
2. **Where it runs** — ephemeral cloud (which cloud?), on-prem, or developer laptop → big VM?
3. **First source/destination pair** to build and benchmark against?
4. **Language** — Rust (tokio + arrow-rs, no GC with GBs in flight), Go (ADBC/Sling-style ecosystem, simpler), or
   Python for a fast prototype?
5. **Open source vs product** — affects license choices (Redis vs Valkey) and connector strategy.
6. **Elastic provisioning** — should bitdrop itself spin up and tear down the data-plane nodes, or just consume
   whatever RAM it is pointed at (and leave provisioning to Terraform/k8s)?
7. **Delivery semantics** — is exactly-once at the destination a hard requirement, or is at-least-once + idempotent
   destination acceptable?
