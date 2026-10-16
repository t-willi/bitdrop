"""MetadataStore adapter persisting chunk states in a SQLite file.

Select it with ``sqlite:///absolute/path/to/metadata.db``, or ``sqlite://`` for a throwaway
in-memory database. Unlike the in-memory adapter the state survives restarts, so a stopped job can
resume, and several processes on one machine can share the file. It cannot be shared between
machines; use a networked adapter for that.

Every operation runs as one ``BEGIN IMMEDIATE`` transaction, which makes it atomic even across
processes. Leases are judged against the wall clock (``time.time``), because a monotonic clock is
not comparable between processes.
"""

import asyncio
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from bitdrop_api.errors import LeaseLost, UnknownChunk
from bitdrop_api.keys import validate_key
from bitdrop_api.metadata_store import (
    DEFAULT_MAX_ATTEMPTS,
    ChunkRecord,
    ChunkState,
    Claim,
    Stage,
)
from bitdrop_api.urls import parse_url

__all__ = ["SqliteMetadataStore", "create_store"]

SCHEMA_VERSION = 1
"""Stored in ``PRAGMA user_version``. Bump it when the table layout changes."""

_SCHEMA = """
CREATE TABLE IF NOT EXISTS chunks (
    job_id        TEXT    NOT NULL,
    chunk_id      TEXT    NOT NULL,
    state         TEXT    NOT NULL,
    max_attempts  INTEGER NOT NULL,
    failures      INTEGER NOT NULL DEFAULT 0,
    fence         INTEGER NOT NULL DEFAULT 0,
    worker_id     TEXT,
    lease_expires REAL,
    last_error    TEXT,
    PRIMARY KEY (job_id, chunk_id)
);
-- One saved plan per job, kept as the exact bytes it was given. A database made before plans
-- existed gains this table when opened, so the schema version stays 1.
CREATE TABLE IF NOT EXISTS plans (
    job_id TEXT PRIMARY KEY,
    plan   BLOB NOT NULL
);
-- Serves "next waiting chunk of a job" without scanning the job; rowid keeps registration order.
CREATE INDEX IF NOT EXISTS chunks_by_state ON chunks (job_id, state);
"""


class SqliteMetadataStore:
    """A :class:`~bitdrop_api.metadata_store.MetadataStore` backed by one SQLite database.

    One connection is shared by all calls and guarded by a lock; the blocking SQLite calls run in
    worker threads so the event loop stays free. Concurrency *between processes* is handled by
    SQLite's own file locking.
    """

    def __init__(
        self, path: Path | str = ":memory:", *, clock: Callable[[], float] = time.time
    ) -> None:
        """Open (creating if needed) the database at ``path``; ``:memory:`` for a private one.

        ``clock`` (seconds since the epoch) is injectable so tests can control time.

        Raises:
            ValueError: the file was written by a newer, incompatible version.
        """
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        self._lock = threading.Lock()
        # isolation_level=None: we issue BEGIN/COMMIT ourselves. The lock makes sharing the
        # connection between worker threads safe, hence check_same_thread=False.
        self._connection = sqlite3.connect(
            str(path), check_same_thread=False, isolation_level=None, timeout=30
        )
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._prepare_schema()

    async def register_chunks(
        self, job_id: str, chunk_ids: Sequence[str], *, max_attempts: int = DEFAULT_MAX_ATTEMPTS
    ) -> int:
        """Add chunks as ``PENDING``; see :meth:`MetadataStore.register_chunks`."""
        validate_key(job_id)
        for chunk_id in chunk_ids:  # validate everything first: a bad batch adds nothing
            validate_key(chunk_id)
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be at least 1, got {max_attempts}")

        def work(connection: sqlite3.Connection) -> int:
            before = connection.total_changes
            connection.executemany(
                "INSERT OR IGNORE INTO chunks (job_id, chunk_id, state, max_attempts)"
                " VALUES (?, ?, ?, ?)",
                [(job_id, chunk_id, ChunkState.PENDING, max_attempts) for chunk_id in chunk_ids],
            )
            return connection.total_changes - before

        return await self._transaction(work)

    async def save_plan(self, job_id: str, plan: bytes) -> bytes:
        """Keep the first plan of a job; see :meth:`MetadataStore.save_plan`."""
        validate_key(job_id)

        def work(connection: sqlite3.Connection) -> bytes:
            connection.execute(
                "INSERT OR IGNORE INTO plans (job_id, plan) VALUES (?, ?)", (job_id, plan)
            )
            (kept,) = connection.execute(
                "SELECT plan FROM plans WHERE job_id = ?", (job_id,)
            ).fetchone()
            return bytes(kept)

        return await self._transaction(work)

    async def load_plan(self, job_id: str) -> bytes | None:
        """Return the saved plan; see :meth:`MetadataStore.load_plan`."""
        validate_key(job_id)

        def work(connection: sqlite3.Connection) -> bytes | None:
            row = connection.execute(
                "SELECT plan FROM plans WHERE job_id = ?", (job_id,)
            ).fetchone()
            return None if row is None else bytes(row[0])

        return await self._transaction(work)

    async def claim(
        self, job_id: str, stage: Stage, *, worker_id: str, lease_seconds: float
    ) -> Claim | None:
        """Take one waiting chunk; see :meth:`MetadataStore.claim`."""
        validate_key(job_id)
        _check_lease(lease_seconds)

        def work(connection: sqlite3.Connection) -> Claim | None:
            row = connection.execute(
                "SELECT chunk_id, fence FROM chunks WHERE job_id = ? AND state = ?"
                " ORDER BY rowid LIMIT 1",
                (job_id, stage.waiting),
            ).fetchone()
            if row is None:
                return None
            chunk_id, fence = row[0], row[1] + 1
            connection.execute(
                "UPDATE chunks SET state = ?, fence = ?, worker_id = ?, lease_expires = ?"
                " WHERE job_id = ? AND chunk_id = ?",
                (
                    stage.in_flight,
                    fence,
                    worker_id,
                    self._clock() + lease_seconds,
                    job_id,
                    chunk_id,
                ),
            )
            return Claim(job_id, chunk_id, stage, worker_id, fence)

        return await self._transaction(work)

    async def heartbeat(self, claim: Claim, *, lease_seconds: float) -> None:
        """Extend the lease; see :meth:`MetadataStore.heartbeat`."""
        _check_lease(lease_seconds)

        def work(connection: sqlite3.Connection) -> None:
            cursor = connection.execute(
                f"UPDATE chunks SET lease_expires = ? WHERE {_HELD}",
                (self._clock() + lease_seconds, *_held_params(claim)),
            )
            if cursor.rowcount == 0:
                raise LeaseLost(claim)

        await self._transaction(work)

    async def complete(self, claim: Claim) -> None:
        """Finish the claimed stage; see :meth:`MetadataStore.complete`."""

        def work(connection: sqlite3.Connection) -> None:
            cursor = connection.execute(
                "UPDATE chunks SET state = ?, worker_id = NULL, lease_expires = NULL"
                f" WHERE {_HELD}",
                (claim.stage.done, *_held_params(claim)),
            )
            if cursor.rowcount == 0:
                raise LeaseLost(claim)

        await self._transaction(work)

    async def fail(self, claim: Claim, reason: str, *, reextract: bool = False) -> None:
        """Report a failure; see :meth:`MetadataStore.fail`."""
        retry_state = ChunkState.PENDING if reextract else claim.stage.waiting

        def work(connection: sqlite3.Connection) -> None:
            cursor = connection.execute(
                "UPDATE chunks SET failures = failures + 1, last_error = ?,"
                " worker_id = NULL, lease_expires = NULL,"
                " state = CASE WHEN failures + 1 >= max_attempts THEN ? ELSE ? END"
                f" WHERE {_HELD}",
                (reason, ChunkState.FAILED, retry_state, *_held_params(claim)),
            )
            if cursor.rowcount == 0:
                raise LeaseLost(claim)

        await self._transaction(work)

    async def release(self, claim: Claim) -> None:
        """Hand a claimed chunk back without a failure; see :meth:`MetadataStore.release`."""

        def work(connection: sqlite3.Connection) -> None:
            cursor = connection.execute(
                "UPDATE chunks SET state = ?, worker_id = NULL, lease_expires = NULL"
                f" WHERE {_HELD}",
                (claim.stage.waiting, *_held_params(claim)),
            )
            if cursor.rowcount == 0:
                raise LeaseLost(claim)

        await self._transaction(work)

    async def release_expired(self, job_id: str) -> int:
        """Void claims whose lease ran out; see :meth:`MetadataStore.release_expired`."""
        validate_key(job_id)

        def work(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(
                "UPDATE chunks SET failures = failures + 1, last_error = 'lease expired',"
                " worker_id = NULL, lease_expires = NULL,"
                " state = CASE WHEN failures + 1 >= max_attempts THEN :failed"
                "              WHEN state = :extracting THEN :pending"
                "              ELSE :extracted END"
                " WHERE job_id = :job AND state IN (:extracting, :loading)"
                " AND lease_expires <= :now",
                {
                    "failed": ChunkState.FAILED,
                    "extracting": ChunkState.EXTRACTING,
                    "loading": ChunkState.LOADING,
                    "pending": ChunkState.PENDING,
                    "extracted": ChunkState.EXTRACTED,
                    "job": job_id,
                    "now": self._clock(),
                },
            )
            return cursor.rowcount

        return await self._transaction(work)

    async def get_chunk(self, job_id: str, chunk_id: str) -> ChunkRecord:
        """Return one chunk's bookkeeping; see :meth:`MetadataStore.get_chunk`."""
        validate_key(job_id)
        validate_key(chunk_id)

        def work(connection: sqlite3.Connection) -> ChunkRecord:
            row = connection.execute(
                "SELECT state, failures, worker_id, last_error FROM chunks"
                " WHERE job_id = ? AND chunk_id = ?",
                (job_id, chunk_id),
            ).fetchone()
            if row is None:
                raise UnknownChunk(job_id, chunk_id)
            return ChunkRecord(job_id, chunk_id, ChunkState(row[0]), row[1], row[2], row[3])

        return await self._transaction(work)

    async def counts(self, job_id: str) -> Mapping[ChunkState, int]:
        """Count chunks per state; see :meth:`MetadataStore.counts`."""
        validate_key(job_id)

        def work(connection: sqlite3.Connection) -> Mapping[ChunkState, int]:
            counts = dict.fromkeys(ChunkState, 0)
            for state, count in connection.execute(
                "SELECT state, COUNT(*) FROM chunks WHERE job_id = ? GROUP BY state", (job_id,)
            ):
                counts[ChunkState(state)] = count
            return counts

        return await self._transaction(work)

    async def aclose(self) -> None:
        """Close the database connection."""
        await asyncio.to_thread(self._close)

    def _close(self) -> None:
        with self._lock:
            self._connection.close()

    def _prepare_schema(self) -> None:
        """Create the tables on first use; refuse files from an incompatible future version."""
        (version,) = self._connection.execute("PRAGMA user_version").fetchone()
        if version not in (0, SCHEMA_VERSION):
            self._connection.close()
            raise ValueError(
                f"unsupported schema version {version} (this bitdrop understands {SCHEMA_VERSION})"
            )
        self._connection.executescript(_SCHEMA)
        self._connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    async def _transaction[T](self, work: Callable[[sqlite3.Connection], T]) -> T:
        """Run ``work`` in a worker thread as one atomic transaction."""
        return await asyncio.to_thread(self._run_transaction, work)

    def _run_transaction[T](self, work: Callable[[sqlite3.Connection], T]) -> T:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")  # take the write lock up front
            try:
                result = work(self._connection)
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
            self._connection.execute("COMMIT")
            return result


# A claim is valid while the chunk is still in the claimed stage's in-flight state *and* the fence
# matches: re-claiming bumps the fence, which locks out the previous holder.
_HELD = "job_id = ? AND chunk_id = ? AND state = ? AND fence = ?"


def _held_params(claim: Claim) -> tuple[str, str, str, int]:
    return (claim.job_id, claim.chunk_id, claim.stage.in_flight, claim.fence)


def _check_lease(lease_seconds: float) -> None:
    if lease_seconds <= 0:
        raise ValueError(f"lease_seconds must be positive, got {lease_seconds}")


def create_store(url: str) -> SqliteMetadataStore:
    """Build a store from a ``sqlite://`` URL. This is the plugin entry point."""
    parsed = parse_url(url, scheme="sqlite", allowed_params=())
    return SqliteMetadataStore(parsed.path or ":memory:")
