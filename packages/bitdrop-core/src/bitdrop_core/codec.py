"""The default chunk codec: Arrow IPC streams.

Arrow IPC is the one canonical chunk format (see ``docs/principles.md``): it keeps the schema,
field metadata (native type hints), and extension types intact, can be compressed per buffer, and
lets connectors hand the decoded columns to database drivers without conversion.
"""

from typing import Literal

import pyarrow as pa

from bitdrop_api.errors import CorruptChunk

Compression = Literal["none", "lz4", "zstd"]
"""``none`` keeps RAM copy-free to read; ``lz4``/``zstd`` trade CPU for a smaller RAM footprint."""

_COMPRESSION: dict[str, Literal["lz4", "zstd"] | None] = {
    "none": None,
    "lz4": "lz4",
    "zstd": "zstd",
}


class ArrowIpcCodec:
    """A :class:`~bitdrop_api.codec.ChunkCodec` writing Arrow IPC streams.

    The compression is recorded inside the stream, so ``decode`` works on the output of any
    ``ArrowIpcCodec`` regardless of its own setting.
    """

    def __init__(self, compression: Compression = "none") -> None:
        """Create a codec; ``compression`` applies to ``encode`` only.

        Raises:
            ValueError: ``compression`` is not one of ``none``, ``lz4``, ``zstd``.
        """
        if compression not in _COMPRESSION:
            raise ValueError(
                f"compression must be one of {sorted(_COMPRESSION)}, got {compression!r}"
            )
        self._options = pa.ipc.IpcWriteOptions(compression=_COMPRESSION[compression])

    def encode(self, table: pa.Table) -> bytes:
        """Serialize ``table`` as one Arrow IPC stream."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema, options=self._options) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes()

    def decode(self, data: bytes) -> pa.Table:
        """Rebuild the table.

        Raises:
            CorruptChunk: ``data`` is empty, truncated, or not an Arrow IPC stream.
        """
        try:
            return pa.ipc.open_stream(pa.py_buffer(data)).read_all()
        except (pa.ArrowException, OSError) as error:  # truncated streams raise OSError
            raise CorruptChunk(str(error)) from error
