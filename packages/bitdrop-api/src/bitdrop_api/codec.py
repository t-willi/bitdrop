"""The ``ChunkCodec`` contract: how a chunk is turned into bytes for the chunk store and back."""

from typing import Protocol, runtime_checkable

import pyarrow as pa


@runtime_checkable
class ChunkCodec(Protocol):
    """Serializes Arrow tables.

    Both methods are CPU-bound and synchronous; callers run them in a worker thread. The round trip
    must be lossless, including schema metadata (native type hints!) and extension types.
    """

    def encode(self, table: pa.Table) -> bytes:
        """Serialize ``table`` into bytes."""
        ...

    def decode(self, data: bytes) -> pa.Table:
        """Rebuild the table that ``encode`` was given."""
        ...
