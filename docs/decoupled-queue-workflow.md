# bitdrop — alternative workflow: decoupled source and destination over a queue

Status: **idea, nothing built** (written 2026-10-17). This describes a second way to run a transfer next to the
current one (`docs/ideation.md`, `docs/principles.md`). It keeps the principles: components stay behind contracts
(P1) and a value is stored exactly or refused (P2).

## 1. The idea in one paragraph

Two kinds of simple containers, with no master between them:

- **Source connector**: reads from the source database and writes each piece as a file (a *chunk*) into Redis.
  For every file it writes, it appends one message to a queue, also in Redis.
- **Destination connector**: asks the queue for a file name about once a second. When it gets one, it loads the
  file from Redis into the destination, then removes it.

Both kinds scale out by starting more copies. They never talk to each other; Redis is the only thing they share.

```
 source DB                      Redis                         destination DB
 ┌────────┐   write file   ┌───────────────┐   read file    ┌────────┐
 │ source │ ─────────────► │ chunk:<id>    │ ─────────────► │  dest  │
 │ conn.  │   add message  │ (blobs)       │                │  conn. │
 │  x N   │ ─────────────► │ queue         │ ◄───────────── │   x M  │
 └────────┘                │ (file names)  │   take message └────────┘
                           └───────────────┘
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
| Deciding what the source containers read | the plan (`Source.plan`) | needs a work-sharing answer (section 7) |

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

- Each source container publishes, as its last message, a **done marker** `{job, source_id, chunks_published}`.
- The job is complete when every expected marker is present and the destination ledger holds that many chunks
  with nothing pending or in flight.
- A small, stateless `bitdrop status` reads these counts. It is a viewer, not a coordinator; it can run anywhere
  and be killed without effect.

## 7. Scaling the source: who reads what

A single source container needs no coordination. Several do: they must not read the same rows twice or leave a gap.

| Option | How | Verdict |
|---|---|---|
| A. Plan queue | One `plan` step splits the table (existing `Source.plan`) and publishes one *work item* per chunk range to a second queue; source containers take work items exactly like destinations take chunk messages | **Recommended.** Same mechanism twice, reuses `Source.plan` and its disjoint, complete guarantee, scales to any N |
| B. Static slices | Container `i` of `N` takes every `N`th range | Simple, but N is fixed at start and a dead container leaves a hole |
| C. One planner container | A designated container plans, then others join | A special case to elect and recover; option A without the second queue |

With A, the picture is a pipeline of two queues: ranges to extractors, files to loaders. A restart of a source
container makes its unacknowledged work item return, and the chunk id is deterministic from the range, so a
re-read overwrites the same blob key (`put` allows overwrite) and cannot create a second logical chunk.

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

1. `ChunkQueue` contract, a memory adapter, and the conformance kit. Question: is the contract tight enough?
2. Redis Streams adapter, passing the same kit on Redis, Valkey and Dragonfly. Question: do the stream commands
   behave the same on all three (Dragonfly and Valkey differ in places)?
3. Source loop and destination loop as plain functions over the contracts, driven by the fault-injection style
   tests already used for `run_transfer` (kill after write, kill after commit, duplicate offer, poison chunk).
4. Work-item queue and several source containers (option A in section 7); a test that the union of rows is
   complete and disjoint after a kill.
5. Completion markers and a read-only `status`.
6. Two role images and a docker-compose file; then measure throughput against the current design on the same
   table. Question: does removing the master change speed, or only complexity?

## 12. Decided and open

**Decided by the owner (2026-10-17):**

- Chunk order does not matter. Nothing in the design may rely on it: any chunk can be loaded at any time, by any
  destination container, in any order, and a re-offered chunk can arrive late. (This is what makes the rebuild in
  section 8 and the reclaim in section 5 simple.)
- Destination containers keep polling the queue; the one-second interval is an estimate and a setting.
- A load balancer (dispatcher) after the stream is possible; see section 4. Pull is the starting point.

**Open:**

- Should this **replace** the master-based runner or sit **beside** it as a second mode? (It reuses every
  connector and store, so both can live in one repository.)
- Is a lost queue acceptable if the rebuild in section 8 exists, or must the queue itself be durable?
- Is there a concrete reason for a dispatcher (a per-database concurrency limit, zone routing), or is pull enough
  for now?
- Which queue after Redis Streams, if any: Kafka, SQS, the destination database?
