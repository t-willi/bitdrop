# To verify on a machine with Docker

Reminder for the owner. Some work is written in cloud sessions that have **no Docker**, so it is checked only against
in-process fakes until someone runs it here. Run this list where Docker works, and tell the next session what failed.
Tick a line when it passes, and delete the line when it has been green for a while.

The one command that covers everything that can be automated is:

```bash
make sync && make check      # about 2 minutes; starts and removes its own containers
```

If `make check` is red, the narrower commands below say where to look.

## Open items

- [ ] **Queue throughput across a real network, between hosts.** Measured on one machine only (2026-10-18, Docker
  Desktop on a Mac, the 2,000,000-row `orders` table of the compose example, 3 extractors or sources and 2 loaders or
  destinations, both designs as host processes through the same published ports):

  | Chunks | Master (`bitdrop run`) | Queue (planner, then 3 + 2 processes) |
  |---|---|---|
  | 16 (8 MiB) | 2.6 s (3 runs: 2.55 to 2.66) | 4.1 s (3 runs: 4.05 to 4.19) |
  | 62 (2 MiB) | 3.1 s (1 run) | 4.5 s (1 run) |
  | 123 (1 MiB) | 3.0 s (1 run) | 4.8 s (1 run) |

  Inside the compose network the whole example (first container started to last container exited) took 2.76 s for
  the same table with 3 + 2 containers. Reading: most of the gap is fixed cost (five more interpreter starts and the
  one-second poll for "is the job over"); the extra cost per chunk is a few milliseconds. This says nothing about
  two hosts with a real network between them and Redis; time `bitdrop run` and the queue workflow there.
- [ ] **The TUI looked at by a person.** Driven through a real pseudo-terminal on 2026-10-18 (see below), but nobody
  has watched it: `uv run bitdrop run examples/demo.toml --wait`, look at the Activity panel (busy extractors and
  loaders, containers), press `s` to start, then `q`.

## Verified

Delete a line when it has been green for a while.

- [x] **The queue workflow end to end in Docker** (2026-10-18). `docker compose -f examples/queue/docker-compose.yml
  up --build`. The planner printed `planned 16 chunks`, the containers left by themselves, and `orders` in the
  destination holds 2,000,000 rows; a checksum of the whole table (`md5(string_agg(o::text, ',' order by id))`) is the
  same on both sides. Also done:
  - Repeated from scratch: 5 default runs (3 + 2 containers) all completed, but only by a narrow margin, see the race
    below; the scaled run (`--scale source=6 --scale destination=4`) failed 2 times in 2 until the health checks
    were fixed, and after that 3 scaled runs and a default run all completed.
  - **A source killed mid-job.** To catch a source holding a chunk (the whole job takes 3 s), the staging Redis was
    paused (`docker pause`), three sources started and stalled, two were killed with `docker kill`, and staging was
    unpaused. The surviving source reported `16 handled, 1 retried, 2 taken over`, the job finished about 57 s later
    (the 60 s lease), with 2,000,000 distinct ids and the same checksum.
  - Three faults were found and fixed by this run: the image did not build (two separate reasons), and the planner
    could lose a race with the database when the containers start; see `docs/agent-log.md`, section 8. Each has a
    test now (`tests/e2e/test_role_image.py` builds the image and runs the command in it;
    `tests/architecture/test_queue_example.py` keeps the database health checks on TCP).
- [x] **Redis Streams queue on the real servers** (2026-10-18). `uv run pytest packages/bitdrop-queue-redis -m docker -v`:
  105 passed on Redis, Valkey and Dragonfly (including the two tests that need a blocking `XREADGROUP`). The worry that
  Dragonfly (v2.0.2) lacks `XPENDING ... IDLE` or `XCLAIM ... RETRYCOUNT`/`IDLE` did not materialise.
- [x] **Redis marker store on the real servers** (2026-10-18). `uv run pytest packages/bitdrop-marker-redis -m docker -v`:
  63 passed on all three, the 30-writers-at-once test included.
- [x] **PostgreSQL source with the new chunk ids** (2026-10-18). `uv run pytest packages/bitdrop-connector-postgres -m docker -v`:
  the source kit round-trips every planned id through `chunk_from_id`. One test still expected the old sequential
  ids; fixed in `0a45f1f`.
- [x] **End-to-end PostgreSQL to PostgreSQL** (2026-10-18). `uv run pytest tests/e2e -m docker -v`: 24 passed,
  including `test_a_loader_dying_after_its_commit_causes_no_duplicate_rows` with the 4th write killed.
- [x] **The TUI's `--wait` and `s` in a pseudo-terminal** (2026-10-18). `bitdrop run examples/demo.toml --wait` drawn
  in a real pty: the screen says `Ready. Press s to start the load`, nothing is loaded before `s`, after `s` the
  Activity panel goes from `extracting 0 of 4 busy` to `4 of 4 busy`, and `q` exits with 130.
- [x] **A full `make check` with Docker** (2026-10-18): lint, format, strict types, architecture rules, and
  1,910 passed, 25 skipped (the skips are intentional kit checks that do not apply to an adapter, and the two waiting
  tests that the fake Redis cannot run).
