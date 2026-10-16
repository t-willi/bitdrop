"""The ``Destination`` contract: where data goes.

The crucial property is **idempotent chunk writes**. A loader can crash after committing a chunk
but before telling the metadata store; the chunk is then loaded a second time. The destination
makes that harmless by recording ``(job_id, table, chunk_id)`` in a *ledger* in the same
transaction as the data, and skipping chunks it has already recorded.
"""

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class ChunkWrite:
    """The outcome of ``Destination.write_chunk``."""

    rows: int
    """Rows of the chunk."""

    already_loaded: bool
    """``True`` if the chunk was loaded before, so nothing was written this time."""


@runtime_checkable
class Destination(Protocol):
    """Writes Arrow data into tables.

    Behaviour every implementation must provide (verified by the conformance kit):

    * ``write_chunk`` is idempotent per ``(job_id, table, chunk_id)``, also under concurrent calls.
    * A chunk is written atomically with its ledger entry: both happen or neither does.
    * ``prepare`` may be called again for an existing, matching table without losing data.
    """

    async def prepare(self, table: str, schema: pa.Schema) -> None:
        """Make sure ``table`` exists with columns matching ``schema``, and that the ledger exists.

        Raises:
            SchemaMismatch: the table exists with different columns.
        """
        ...

    async def write_chunk(
        self, job_id: str, chunk_id: str, table: str, data: pa.Table
    ) -> ChunkWrite:
        """Append ``data`` to ``table`` exactly once for this ``(job_id, table, chunk_id)``.

        Raises:
            InvalidKey: a job id or chunk id breaks the key rules.
            TableNotPrepared: ``prepare`` was never called for ``table``.
        """
        ...

    async def count_rows(self, table: str) -> int:
        """Return the number of rows currently in ``table``.

        Raises:
            TableNotPrepared: ``prepare`` was never called for ``table``.
        """
        ...

    async def aclose(self) -> None:
        """Release connections and other resources. The destination must not be used afterwards."""
        ...
