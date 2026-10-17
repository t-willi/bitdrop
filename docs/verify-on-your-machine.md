# To verify on a machine with Docker

Reminder for the owner. Some work was written in a cloud session that has **no Docker**, so the parts below were
checked only against in-process fakes or not at all. Run this list where Docker works, and tell the next session
what failed. Tick a line when it passes, and delete the line when it has been green for a while.

Everything else passes `make lint typecheck arch test-fast` (no Docker needed). The one command that covers all
of it is:

```bash
make sync && make check      # about 5 minutes; starts and removes its own containers
```

If `make check` is red, the narrower commands below say where to look.

## Open items

- [ ] **Redis Streams queue on the real servers** (written 2026-10-17). `uv run pytest packages/bitdrop-queue-redis -m docker -v`.
  It runs the shared `ChunkQueue` kit on Redis, Valkey and Dragonfly. The same kit passes on a fake server here,
  except two tests that need a blocking `XREADGROUP`, which the fake does not do (they run only here). Watch for:
  Dragonfly (v2.0.2) lacking `XPENDING ... IDLE` or `XCLAIM ... RETRYCOUNT`/`IDLE`, and whether `XCLAIM` sets the
  delivery counter the way the adapter assumes (it sets it when `RETRYCOUNT` is given, else adds one).
- [ ] **Redis marker store on the real servers** (written 2026-10-17). `uv run pytest packages/bitdrop-marker-redis -m docker -v`.
  Includes the 30-writers-at-once test, which only a real server can really exercise.
- [ ] **PostgreSQL source with the new chunk ids** (changed 2026-10-17). `uv run pytest packages/bitdrop-connector-postgres -m docker -v`.
  The source kit now round-trips every planned id through `chunk_from_id`.
- [ ] **End-to-end PostgreSQL to PostgreSQL** (a test changed 2026-10-17). `uv run pytest tests/e2e -m docker -v`.
  `test_a_loader_dying_after_its_commit_causes_no_duplicate_rows` now kills the 4th write instead of the chunk
  `c000003`, which no longer exists. It should still report exactly one repeated chunk.
- [ ] **The TUI in a real terminal** (changed 2026-10-17). Run `uv run bitdrop run examples/demo.toml --wait`, look at
  the Activity panel (busy extractors and loaders, containers), press `s` to start, then `q`. It was tested only
  with Textual's headless test driver.
