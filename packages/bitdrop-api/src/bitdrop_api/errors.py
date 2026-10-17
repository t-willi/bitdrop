"""Errors shared by all bitdrop components.

Adapters translate backend-specific failures (a Redis ``OOM`` reply, a full disk, ...) into these
types, so the core can react to *what happened* without knowing *which backend* it talked to.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bitdrop_api.chunk_queue import Delivery
    from bitdrop_api.metadata_store import Claim


class BitdropError(Exception):
    """Base class of every error raised by bitdrop components."""


class InvalidKey(BitdropError):
    """A key or identifier breaks the shared rule (see :func:`bitdrop_api.keys.validate_key`).

    Chunk store keys, job ids, and chunk ids all follow that one rule, so any store raises this.
    """

    def __init__(self, key: str, reason: str) -> None:
        """Create the error for ``key``; ``reason`` says which rule it breaks."""
        super().__init__(f"invalid key: {reason} (key={key!r})")
        self.key = key


class ChunkStoreError(BitdropError):
    """Base class of errors raised by a :class:`~bitdrop_api.chunk_store.ChunkStore`."""

    def __init__(self, key: str | None, message: str) -> None:
        """Create the error for ``key`` (``None`` if no key was involved) with a ``message``."""
        super().__init__(message if key is None else f"{message} (key={key!r})")
        self.key = key


class ChunkNotFound(ChunkStoreError):
    """No value is stored under the key (never written, deleted, or evicted)."""

    def __init__(self, key: str) -> None:
        """Create the error for the missing ``key``."""
        super().__init__(key, "chunk not found")


class ChunkTooLarge(ChunkStoreError):
    """The value exceeds the store's declared ``max_value_bytes``."""

    def __init__(self, key: str, size: int, limit: int) -> None:
        """Create the error for a value of ``size`` bytes against a ``limit`` in bytes."""
        super().__init__(key, f"chunk of {size} bytes exceeds the limit of {limit} bytes")
        self.size = size
        self.limit = limit


class StoreFull(ChunkStoreError):
    """The store has no capacity left. Callers should apply backpressure, not retry blindly."""

    def __init__(self, key: str) -> None:
        """Create the error for the ``key`` that could not be stored."""
        super().__init__(key, "store is full")


class StoreUnavailable(ChunkStoreError):
    """A networked store could not be reached, or refused a request for a reason besides capacity.

    The request may be retried; the chunk-level retry logic of the runner does exactly that.
    """

    def __init__(self, detail: str, *, key: str | None = None) -> None:
        """Create the error; ``detail`` says what went wrong (never include secrets)."""
        super().__init__(key, f"chunk store unavailable: {detail}")


class MetadataStoreError(BitdropError):
    """Base class of errors raised by a :class:`~bitdrop_api.metadata_store.MetadataStore`."""


class LeaseLost(MetadataStoreError):
    """The claim is no longer valid: the chunk was released, re-claimed, or already finished.

    The holder must stop working on the chunk and discard its result. Another worker owns it now.
    """

    def __init__(self, claim: Claim) -> None:
        """Create the error for the stale ``claim``."""
        super().__init__(
            f"lease lost: {claim.stage} claim #{claim.fence} on chunk {claim.chunk_id!r} "
            f"of job {claim.job_id!r} (worker {claim.worker_id!r}) is no longer valid"
        )
        self.claim = claim


class UnknownChunk(MetadataStoreError):
    """The chunk was never registered for the job."""

    def __init__(self, job_id: str, chunk_id: str) -> None:
        """Create the error for ``chunk_id`` of ``job_id``."""
        super().__init__(f"unknown chunk {chunk_id!r} in job {job_id!r}")
        self.job_id = job_id
        self.chunk_id = chunk_id


class PlanMismatch(MetadataStoreError):
    """A job id that was planned for one table was asked to transfer another.

    A job's plan is saved when it first runs and used again when it resumes, so the id is bound to
    that table.
    """

    def __init__(self, job_id: str, planned_table: str, requested_table: str) -> None:
        """Create the error for ``job_id``, saved for ``planned_table`` but asked for another."""
        super().__init__(
            f"job {job_id!r} was planned for table {planned_table!r}, not {requested_table!r}; "
            "use a new job id to transfer a different table"
        )
        self.job_id = job_id
        self.planned_table = planned_table
        self.requested_table = requested_table


class QueueError(BitdropError):
    """Base class of errors raised by a :class:`~bitdrop_api.chunk_queue.ChunkQueue`."""


class DeliveryLost(QueueError):
    """The delivery is no longer the current one: handed back, taken over, or settled.

    The holder must stop working on the message and discard its result; another consumer owns it
    now (or it is gone).
    """

    def __init__(self, delivery: Delivery) -> None:
        """Create the error for the stale ``delivery``."""
        super().__init__(
            f"delivery lost: delivery #{delivery.deliveries} of message {delivery.message_id!r} "
            f"to consumer {delivery.consumer!r} is no longer current"
        )
        self.delivery = delivery


class ConnectorError(BitdropError):
    """Base class of errors raised by a source or destination connector."""


class InvalidChunkId(ConnectorError):
    """A source cannot turn a chunk id into a chunk: not one of its ids, or not canonical."""

    def __init__(self, chunk_id: str, reason: str) -> None:
        """Create the error for ``chunk_id``; ``reason`` says what is wrong with it."""
        super().__init__(f"invalid chunk id: {reason} (chunk_id={chunk_id!r})")
        self.chunk_id = chunk_id


class TableNotFound(ConnectorError):
    """The source table does not exist (or is not visible to the configured user)."""

    def __init__(self, table: str) -> None:
        """Create the error for ``table``."""
        super().__init__(f"table not found: {table!r}")
        self.table = table


class UnsupportedType(ConnectorError):
    """A column has a type the connector cannot move without losing information.

    Raised while planning, before any data moves: failing loudly beats silently degrading a type
    (for example turning ``numeric`` into text) in the destination.
    """

    def __init__(self, table: str, column: str, native_type: str, hint: str = "") -> None:
        """Create the error for ``column`` of ``table`` whose type is ``native_type``."""
        message = f"column {column!r} of {table!r} has unsupported type {native_type!r}"
        super().__init__(f"{message}. {hint}" if hint else message)
        self.table = table
        self.column = column
        self.native_type = native_type


class SchemaMismatch(ConnectorError):
    """The destination table exists but does not match the schema being loaded."""


class TableNotPrepared(ConnectorError):
    """A chunk was written to a destination table that ``prepare`` was never called for."""

    def __init__(self, table: str) -> None:
        """Create the error for ``table``."""
        super().__init__(f"destination table {table!r} was not prepared; call prepare() first")
        self.table = table


class CorruptChunk(BitdropError):
    """The bytes handed to a ``ChunkCodec`` are not a valid, complete chunk."""

    def __init__(self, reason: str) -> None:
        """Create the error; ``reason`` says what the codec found wrong."""
        super().__init__(f"corrupt chunk: {reason}")


class CorruptPlan(BitdropError):
    """Saved bytes are not a valid plan (see :func:`bitdrop_api.plan.decode_plan`).

    The reason never repeats the input: a plan may sit in a store that anyone can write to.
    """

    def __init__(self, reason: str) -> None:
        """Create the error; ``reason`` says what was found wrong."""
        super().__init__(f"corrupt plan: {reason}")
