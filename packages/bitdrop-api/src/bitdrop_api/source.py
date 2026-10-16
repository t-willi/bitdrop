"""The ``Source`` contract: where data comes from.

A source first *plans* a table: it inspects it, learns its schema, and cuts it into independent
chunks. Workers then read the chunks, in any order and in parallel.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import pyarrow as pa

from bitdrop_api.keys import validate_key


@dataclass(frozen=True, slots=True)
class ChunkSpec:
    """One independently readable slice of a table."""

    chunk_id: str
    """Unique within the plan; follows the shared key rule (it ends up in store keys)."""

    description: str
    """Human-readable, for logs: e.g. ``id in [1, 50001)``."""

    payload: Mapping[str, Any]
    """Opaque to the core. The source reads it back in ``read_chunk``. Keep it JSON-serializable
    so plans can later be persisted or sent to workers in other containers."""

    estimated_rows: int | None = None

    def __post_init__(self) -> None:
        """Reject ids that could not be used as store keys."""
        validate_key(self.chunk_id)


@dataclass(frozen=True, slots=True)
class SourcePlan:
    """The result of planning one table."""

    table: str
    schema: pa.Schema
    """What every chunk returned by ``read_chunk`` conforms to (field names and types)."""

    chunks: tuple[ChunkSpec, ...]
    estimated_rows: int | None = None
    estimated_row_bytes: int | None = None
    notes: tuple[str, ...] = field(default=())
    """Things the operator should know, e.g. "no integer primary key: reading as a single chunk"."""

    def __post_init__(self) -> None:
        """Reject plans that reuse a chunk id."""
        seen: set[str] = set()
        for chunk in self.chunks:
            if chunk.chunk_id in seen:
                raise ValueError(
                    f"duplicate chunk id {chunk.chunk_id!r} in the plan for {self.table!r}"
                )
            seen.add(chunk.chunk_id)


@runtime_checkable
class Source(Protocol):
    """Reads tables chunk by chunk as Arrow data.

    Behaviour every implementation must provide (verified by the conformance kit):

    * Chunks are disjoint and together cover the table exactly.
    * ``read_chunk`` is repeatable: reading a chunk twice returns the same rows.
    * Every table returned has exactly the plan's schema.
    """

    async def plan(
        self, table: str, *, target_chunk_bytes: int, split_column: str | None = None
    ) -> SourcePlan:
        """Inspect ``table`` and cut it into chunks of roughly ``target_chunk_bytes`` each.

        ``split_column`` is a hint for sources that split by a key column; sources that cannot use
        it ignore it.

        Raises:
            TableNotFound: the table does not exist.
            UnsupportedType: a column cannot be moved without losing information.
        """
        ...

    async def read_chunk(self, plan: SourcePlan, chunk: ChunkSpec) -> pa.Table:
        """Read one chunk of ``plan``."""
        ...

    async def aclose(self) -> None:
        """Release connections and other resources. The source must not be used afterwards."""
        ...
