# bitdrop — alternative workflow: decoupled source and destination over a queue

Status: **idea, nothing built** (written 2026-10-17). This describes a second way to run a transfer next to the
current one (`docs/ideation.md`, `docs/principles.md`). It keeps the principles: components stay behind contracts
(P1) and a value is stored exactly or refused (P2).

## 1. The idea in one paragraph

A one-shot **planner** first, then two kinds of simple containers, with no master between them:

- **Planner**: before any extraction, analyzes the source table and cuts it into chunks. It publishes one message
  per chunk identifier to a **work queue** (section 7). It then exits.
- **Source connector**: takes a chunk identifier from the work queue, reads exactly that chunk from the source
  database, and writes it as a file into Redis. For every file it writes, it appends one message to a second
  queue, the **file queue**, also in Redis.
- **Destination connector**: asks the queue for a file name about once a second. When it gets one, it loads the
  file from Redis into the destination, then removes it.

Both kinds scale out by starting more copies. They never talk to each other; Redis is the only thing they share.

```
            work queue            file queue
 planner ─► (chunk ids) ─► source ─► (file names) ─► destination
   │                       conn. x N       │            conn. x M
   │ analyze + cut            │            │               │
   ▼                          ▼            ▼               ▼
 source DB ◄── read chunk ────┘     Redis: blobs     load + delete ─► destination DB
```

N and M are independent: a slow destination needs more destination containers, not more source containers.

## 2. How it differs from the current design

| | Current design | This workflow |
|---|---|---|
| Coordinator | master plus a metadata store (states, leases, fencing) | none; the queue is the coordination |
| Who gets which chunk | `claim` on the metadata store | the queue hands out messages |
| Crash recovery | lease expiry, reaper, fencing tokens | unacknowledged messages are handed out again |
| Duplicate protection | ledger in the destination, same transaction as the data | **unchanged**: still needed (section 5) |
| Progress and "is it done" | metadata store counts | must be built (section 6) |
| Scaling | master starts workers | start more containers; Kubernetes can scale each kind on queue length |

Less to run and to explain, in exchange for less visibility. The hard parts do not go away; they move into the
queue's rules (section 5).

## 3. Mapping to the existing contracts

Most pieces exist already, which is the point of P1:

| Role here | Existing contract | Status |
|---|---|---|
| Source connector reads a chunk | `Source.read_chunk` | built (PostgreSQL, memory) |
| File in Redis | `ChunkStore` (`redis://` adapter) | built |
| Destination loads a chunk | `Destination.write_chunk` with the ledger | built (PostgreSQL, memory) |
| Encoding of the file | `ChunkCodec` (Arrow IPC, optional lz4/zstd) | built |
| The queue | **new contract**, `ChunkQueue` (section 4) | not built |
| Analyzing and cutting the table | the plan (`Source.plan`) | built; runs in the planner (section 7) |
| Handing chunk ids to source containers | the work queue, the same `ChunkQueue` contract | not built (section 7) |
| Reading one chunk by its id | `Source.read_chunk`, plus a new `Source` method that turns an id into a chunk spec | read built; id parsing not built (section 7) |

The queue must be its own contract, not a method on `ChunkStore`, so that it can be backed by Redis Streams today
and by something else (Kafka, SQS, PostgreSQL) later without touching the connectors.

## 4. The queue

### Contract (sketch)

```
async publish(message)                       durable once it returns
async receive(consumer, max, block_ms)       messages, each with a receipt; empty list is a normal result
async ack(receipt)                           done; idempotent
async release(receipt)                       hand back without counting a failure
async reclaim(consumer, min_idle_ms)         take over messages whose consumer went silent
async stats()                                pending, in flight, dead
```

A conformance kit like the other kits checks every adapter: nothing is lost, an unacknowledged message comes back,
two consumers never hold the same message at once, and an acknowledged message never returns.

### Redis adapter: use a stream, not a list

Redis Streams with a consumer group fit exactly: `XADD` to publish, `XREADGROUP` to receive, `XACK` to confirm,
`XAUTOCLAIM` to take over from a dead consumer, `XPENDING` for stats. A plain list (`LPUSH`/`BRPOP`) loses the
message if the consumer dies after taking it.

Destination containers keep polling the queue; the one-second interval is an estimate, so it is a setting
(`poll_interval`), not a rule. A plain poll makes the destination wait up to one interval per file for no reason.
`XREADGROUP` with `BLOCK` returns the instant a message arrives and costs Redis no more than polling does, so the
loop blocks for up to `poll_interval` and then wakes anyway to heartbeat and check for a stop request. To the
container it is still "keep asking the queue".

### Optional: a dispatcher after the stream

Instead of every destination container pulling from the stream, a **dispatcher** (a load balancer) could read the
stream and hand each chunk to a destination container. Two shapes exist:

| Shape | How | When it earns its place |
|---|---|---|
| Pull (section 4, as above) | each destination asks the stream for the next message | default: the stream's consumer group already balances by itself, because a busy container asks less often and a fast one more often |
| Push through a dispatcher | one or more dispatcher containers read the stream and send each chunk to a chosen destination container | when the choice needs knowledge the stream does not have |

Reasons to choose the destination on purpose: per-destination limits (for example at most 4 concurrent loads into
one database host), spreading chunks of one table so they do not all hit the same table lock, routing to a
destination container in the right zone, or draining a container before it is removed.

What a dispatcher costs, and why pull stays the default:

- A pushed chunk sits in a busy container's inbox while an idle one waits (head-of-line blocking). Pull cannot do
  that. The dispatcher must therefore send only to containers that have said "I have a free slot", which is pull
  with an extra step.
- It must know which containers are alive and how loaded they are, so it needs heartbeats and a registry: state
  that this workflow set out to avoid.
- It is one more thing to run and to make highly available. Run two; the stream's consumer group lets them share
  the work without a lock.

**Recommendation:** build pull first. Put the dispatcher behind its own contract so it can be added later without
changing the connectors:

```
Dispatcher: reads ChunkQueue messages, chooses a destination slot, hands the message over;
            a destination announces free slots and withdraws them on shutdown
```

A destination container then has two ways to get work, `ChunkQueue.receive` (pull) or a slot offered by a
dispatcher, and the rest of its loop (load, delete, acknowledge, ledger) is identical. The acknowledgement still
goes to the stream, so the failure rules in section 5 hold for both.

### Message

```
{ "job": "<job id>", "table": "<name>", "chunk": "<chunk id>", "key": "<file key in the chunk store>",
  "rows": 123456, "bytes": 8123456, "attempt": 1, "format": 1 }
```

Small and self-describing: the file name is `key`. No data travels in the message. The `format` number lets the
message change later without breaking running containers.

## 5. Correctness: what the queue does not give for free

The queue is at-least-once. Each rule below closes one gap; each needs a test that fails first.

1. **Write the file, then publish.** The source writes the blob, then the message. A crash in between leaves an
   orphan file with no message: harmless, but it fills RAM. A janitor removes blobs that no message names after a
   grace period. The reverse order (message first) would send destinations to a file that does not exist.
2. **Destination loads, then deletes, then acknowledges.** Same order the loader uses today: a crash after the
   load but before the acknowledgement means the message returns and the chunk is offered again.
3. **The ledger stops the duplicate.** Because of rule 2 a chunk can be offered twice. The destination keeps its
   ledger row `(job, table, chunk)` in the same transaction as the data, so the second load is skipped. This is the
   one rule that cannot be dropped; the queue only makes a duplicate offer more likely than the lease design did.
4. **Silent consumers.** A message idle in someone's pending list for longer than a timeout is reclaimed by
   another consumer. The timeout must exceed the longest honest load, or a slow load is taken over while it runs;
   the ledger keeps that safe, but it wastes work. The consumer may extend its claim while it loads.
5. **Poison chunks.** The message carries an attempt count. After a limit (3 today) it moves to a dead-letter stream
   and is counted as failed, so one bad chunk cannot loop forever and `status` can show it.
6. **A missing file is a normal result.** `ChunkNotFound` after a reclaim usually means another consumer already
   loaded and deleted it; acknowledge and move on.
7. **Redis must not drop data silently.** Run with `noeviction` (as today). Persistence off for the blobs is fine
   because the source is the durable copy; the *queue* is different: if Redis restarts, messages are gone while
   the destination may be half loaded. Section 8 lists the options.

## 6. Backpressure and completion

**Backpressure.** Without a master, nobody stops the sources from filling Redis. The source checks before it
writes: if staged bytes (or queue length) are over a limit, it waits. This reuses the staging-fill idea of the
current runner; the limit is a setting, and `maxmemory` stays as the hard backstop.

**Completion.** With no master, "the job is over" must be written down by someone:

- The **planner's marker** `{job, chunks: N}` is the expected total. There are no per-source markers: the
  planner knows N before any extraction starts.
- The job is complete when the marker is present, the destination ledger holds N chunks, and nothing is pending
  or in flight in either queue.
- A small, stateless `bitdrop status` reads these counts. It is a viewer, not a coordinator; it can run anywhere
  and be killed without effect.

## 7. Planning first, then a work queue of chunk identifiers

Decided by the owner (2026-10-17): the table is analyzed and chunked **before extraction starts**, and each chunk
identifier is put into a queue from which a source connector reads, then reads that chunk of the table.

This also answers how several source containers share a table without reading rows twice or leaving a gap:
nobody coordinates, because the planner already cut the table into disjoint, complete chunks and the queue hands
each chunk id to one container at a time.

### The planner

1. Calls `Source.plan` for the table. The source connector is the one place that knows how to split (PostgreSQL
   by integer primary key, others differently), and its kit already enforces that chunks are disjoint and
   complete. The planner never reads rows.
2. Records the settings the chunking used (table, split column, chunk size) in Redis, set-if-absent, creates the destination table from
   the plan's columns and types, and refuses if this job id was already chunked with different settings.
3. Publishes one **work message** per chunk to the work queue, then one **plan-complete marker**
   `{job, chunks: N}`, and exits.

It can run anywhere that reaches the source and Redis: a job on its own, or the `bitdrop` command (the dry run
`bitdrop plan` is the same analysis without the publishing). Running it twice is harmless: chunk ids are
deterministic, so a second run publishes the same ids, and a re-read overwrites the same blob key.

### The chunk id says what to read

Decided by the owner (2026-10-17): the chunk id carries the chunk's definition, for example `id.0-10000` (column,
first and last key), so a source container needs only the queue message to read it:

```
{ "job": "<job id>", "table": "<name>", "chunk": "id.0-10000", "attempt": 1, "format": 1 }
```

The PostgreSQL connector already reads a chunk as `column BETWEEN lo AND hi`; the id is that same payload written
as text. Rules that keep it sound:

- **The format belongs to the source connector.** The core treats the id as an opaque string (P1). Each connector
  writes and parses its own form; a connector with no natural readable form may encode its payload instead. A new
  `Source` method turns an id back into a chunk spec, and the source kit checks that parsing an id gives the spec
  the planner made.
- **The id names the split column**, not just numbers, so `t_1_0_10000` alone is not enough. The table comes from
  the message. The id obeys the key rule (at most 200 characters from `A-Z a-z 0-9 . _ : -`); a range that cannot
  fit, such as a long text or composite key, uses the connector's encoded fallback.
- **Ranges are closed and fixed at planning time**, as the PostgreSQL connector plans them today (from the key
  minimum and maximum it found): a row inserted later outside that range is not read, the same limit as the
  master-based workflow (no snapshot isolation). Making the first and last chunk open-ended would read such rows
  and is a separate decision. A table with no integer primary key is one whole-table chunk, `all`.
- **Order does not matter** (decided above): the destination never needs chunks in any order. For the destination
  the id is only an idempotency key: the ledger row `(job, table, chunk)` makes a repeated chunk a no-op, which is
  what prevents duplicate rows.
- **The id is therefore only a safe key while the chunking is fixed for the job.** Two plans with different chunk
  sizes would give different ids over the same rows, and the ledger would load those rows twice. So a job id is
  bound to one chunking: the planner records its settings (table, split column, chunk size) with the plan-complete
  marker, and a second planner run for the same job with different settings refuses instead of publishing. With
  identical settings it publishes the same ids and is harmless.
- **Column names and types do not go in the id**, but a source container still needs them to read
  (`read_chunk` takes a plan for its schema). Found while building step 1. The planner has them, so it writes the
  schema into Redis with the recorded settings, creates the destination table before it publishes the first work
  message, and a source container builds a plan with no chunks from the stored schema. Destination containers
  then need no plan either. Left for step 5.

What this gives up: the saved plan's check that a job id was not reused for another table, and the plan as the
one place to see all chunks. An alternative that keeps both is to put the id in the message and the full plan in
a saved plan (the metadata store today, or a blob in the chunk store); it costs a second shared place and larger
bookkeeping, and is kept only as a fallback if a connector cannot describe a chunk in its id.

### The source container's loop

1. Take a work message (blocking read with `poll_interval`, as for the destination).
2. Parse the id into a chunk spec with the source connector. An id it cannot parse is a bug: dead-letter it.
3. `Source.read_chunk`, encode, `put` the blob (overwrite allowed).
4. Publish the file message (section 4), then acknowledge the work message. Same order as section 5, rule 1: the
   blob first, then the file message, then the acknowledgement. A crash anywhere repeats the chunk safely.

### Why a separate queue for work and for files

Different consumers, different rates, different failure handling: work items are tiny and cheap to repeat; file
messages point at RAM that costs money. Both are `ChunkQueue`, so it is one contract and one kit, two instances
(two stream names, for example `work:<job>` and `files:<job>`).

## 8. Redis layout and durability

Both uses can live in one Redis or in two. Recommendation: **two logical stores** behind two URLs (`staging` and
`queue`), which may point at the same server in a small setup.

| Option | For | Against |
|---|---|---|
| One Redis for blobs and queue | one thing to run; a chunk and its message are close | a flood of blobs can evict or block the queue; one `maxmemory` for both |
| Separate queue Redis, small, with persistence on (AOF) | queue survives a restart; blob Redis stays persistence-free and large | two things to run |
| Queue in the destination database (PostgreSQL, `SKIP LOCKED`) | durable with the data; the message and the ledger share a transaction, which makes rule 3 almost free | slower, and ties the queue to one destination kind (breaks P1 unless it is an adapter) |

If the queue is lost, the recovery is a **rebuild**: list the blob keys in staging, subtract what the ledger says is
loaded, republish the rest. This is the reason for keeping the message small and the key deterministic.

## 9. Failure table

| Event | What happens |
|---|---|
| Planner dies halfway through publishing | run it again: the same ids are published again, duplicates are harmless; the marker is published last, so the job is never "complete" early |
| Source container dies after reading, before writing the blob | the work item returns after its timeout; another container re-reads the range |
| Source dies after writing the blob, before publishing | orphan blob; the work item returns, the re-read overwrites it and publishes |
| Source dies after publishing, before acknowledging its work item | the item returns; the re-read overwrites the same blob; a second chunk message is possible and is harmless (ledger) |
| Destination dies mid-load | transaction rolls back; message reclaimed after the timeout; loaded again |
| Destination dies after commit, before acknowledging | message reclaimed; the ledger skips the load; the blob is deleted; acknowledged |
| Redis (blobs) restarts empty | messages point at nothing; destinations report not found; the rebuild in section 8 republishes from the source (the source is the durable copy) |
| Redis (queue) restarts without persistence | the rebuild in section 8 |
| Two destinations get the same chunk | the ledger lets one win |

## 10. Trade-offs, honestly

- **Gains:** no master to run or to fail; scaling is just a replica count; a role image needs only its own
  contract; the shape suits Kubernetes autoscaling on queue length; no ordering to maintain.
- **Losses:** per-chunk states, leases and fencing tokens are gone (the stream's pending list only approximates
  them); exact progress, ETA and per-worker views need extra counters; clean "stop now, hand everything back" is
  weaker (a stopped consumer simply stops acknowledging); the memory governor's byte credits become a coarse
  threshold unless it is also built here.
- **Same risk as before:** Redis memory is the shared resource. The queue makes it easier to overfill it, because
  there is no central admission control.

## 11. What to try first

Smallest steps that answer a question each; all run with no database via the memory connector.

1. **Chunk ids carry the chunk** (`Source` change; **done 2026-10-17**, `eb9480a` and `1b8cfa1`): a method that turns a chunk id back into a chunk spec, the
   readable id form for the PostgreSQL and memory connectors, and a kit check that parsing an id gives the spec the
   planner made. Question: does every connector have a natural id form?
2. **`ChunkQueue` contract**, a memory adapter, and the conformance kit (**done 2026-10-17**). Question: is the
   contract tight enough? It settled these points: a delivery is numbered, only the latest is current, and a taken-over
   holder's `touch`, `release` and `dead_letter` fail with `DeliveryLost`; `ack` settles a message in flight whoever
   holds it but does not remove one that was handed back; `release` does not count as a delivery; the consumer, not
   the queue, decides when a message that keeps coming back is dead-lettered.
3. **`MarkerStore` contract** (a tiny one: set-if-absent and get of a small value, used for the recorded chunking
   settings and the plan-complete marker), a memory adapter, and its kit, with two writers at once.
4. **Redis adapters** for the queue (Streams) and the markers (`SET NX`), passing the same kits on Redis, Valkey and
   Dragonfly. Question: do the stream commands behave the same on all three?
5. **The planner**: analyze, record the settings, create the destination table, publish work messages and the
   marker. Tests: a second run with other settings is refused; the same settings republish the same ids; a crash
   halfway republishes safely.
6. **The source loop and the destination loop** as plain functions over the contracts, driven by the
   fault-injection style tests already used for `run_transfer` (kill after write, kill after commit, duplicate
   offer, poison chunk, an unparseable id), with several source containers: the rows read are complete and
   disjoint after a kill.
7. **Completion and a read-only `status`** from the planner's marker and the destination ledger.
8. **Two role images and a docker-compose file**; then measure throughput against the current design on the same
   table. Question: does removing the master change speed, or only complexity?

Each step is test first and committed on its own. Steps 1 to 3 and 5 to 7 run with no database or Docker through
the memory adapters; step 4 and the PostgreSQL part of step 1 need Docker.

## 12. Decided and open

**Decided by the owner (2026-10-17):**

- Chunk order does not matter. Nothing in the design may rely on it: any chunk can be loaded at any time, by any
  destination container, in any order, and a re-offered chunk can arrive late. (This is what makes the rebuild in
  section 8 and the reclaim in section 5 simple.)
- Destination containers keep polling the queue; the one-second interval is an estimate and a setting.
- A load balancer (dispatcher) after the stream is possible; see section 4. Pull is the starting point.
- The table is analyzed and chunked before extraction starts; chunk identifiers go into a work queue; a source
  connector takes an id and reads that chunk (section 7).
- Redis is the only shared service. The planner's recorded chunking settings (the check that stops a second,
  different chunking of one job) are a key in Redis next to the plan-complete marker, written set-if-absent
  (`SET NX`), so two planners started at once end with one chunking.
- The chunk id carries the chunk's definition; the destination needs no order, and uses the id only to refuse
  duplicates through its ledger (section 7).

**Open:**

- Should this **replace** the master-based runner or sit **beside** it as a second mode? (It reuses every
  connector and store, so both can live in one repository.)
- Is a lost queue acceptable if the rebuild in section 8 exists, or must the queue itself be durable?
- Is there a concrete reason for a dispatcher (a per-database concurrency limit, zone routing), or is pull enough
  for now?
- Which queue after Redis Streams, if any: Kafka, SQS, the destination database?
