"""ChunkStore adapter keeping chunks as files in a directory.

Select it with ``file:///some/directory``. Point the directory at a memory-backed volume (tmpfs,
Kubernetes ``emptyDir`` with ``medium: Memory``) and several containers in one pod share a RAM
buffer without any network hop or database. Optional parameters:

* ``max_value_bytes``: largest accepted value.
* ``locality``: ``pod``, ``node`` (default), or ``cluster``. What the directory really is (a pod's
  tmpfs, a host path, a network share) is a deployment fact the adapter cannot discover, so the
  deployment declares it.
"""

import asyncio
import errno
import os
import uuid
from pathlib import Path

from bitdrop_api.chunk_store import Locality, StoreCapabilities, StoreStats
from bitdrop_api.errors import ChunkNotFound, ChunkTooLarge, StoreFull
from bitdrop_api.keys import validate_key
from bitdrop_api.urls import parse_url

__all__ = ["FileChunkStore", "create_store"]


class FileChunkStore:
    """A :class:`~bitdrop_api.chunk_store.ChunkStore` storing one file per chunk.

    Keys are valid file names by construction (see :func:`bitdrop_api.keys.validate_key`), so a
    chunk lives at ``<root>/<key>``. Writes go to a temporary file that is then renamed over the
    final name; the rename is atomic, so readers see the old or the new value, never a mix.
    Blocking file I/O runs in worker threads to keep the event loop free.
    """

    def __init__(
        self,
        root: Path | str,
        *,
        max_value_bytes: int | None = None,
        locality: Locality = Locality.NODE,
    ) -> None:
        """Use ``root`` as the chunk directory, creating it if needed."""
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._capabilities = StoreCapabilities(
            max_value_bytes=max_value_bytes, locality=locality, memory_stats=False
        )

    async def put(self, key: str, value: bytes) -> None:
        """Store ``value`` under ``key``; see :meth:`ChunkStore.put`."""
        validate_key(key)
        limit = self._capabilities.max_value_bytes
        if limit is not None and len(value) > limit:
            raise ChunkTooLarge(key, len(value), limit)
        await asyncio.to_thread(self._write, key, value)

    async def get(self, key: str) -> bytes:
        """Return the value under ``key``; see :meth:`ChunkStore.get`."""
        validate_key(key)
        try:
            return await asyncio.to_thread((self._root / key).read_bytes)
        except FileNotFoundError:
            raise ChunkNotFound(key) from None

    async def delete(self, key: str) -> None:
        """Remove ``key`` if present; see :meth:`ChunkStore.delete`."""
        validate_key(key)
        await asyncio.to_thread((self._root / key).unlink, missing_ok=True)

    def capabilities(self) -> StoreCapabilities:
        """Return the static self-description of this store."""
        return self._capabilities

    async def stats(self) -> StoreStats | None:
        """Not provided: a directory does not report usage cheaply. Always ``None``."""
        return None

    async def aclose(self) -> None:
        """Nothing to release; the files stay where they are."""

    def _write(self, key: str, value: bytes) -> None:
        """Write atomically: temp file first, then rename over the final name."""
        # Valid keys never start with ".", so the temp name cannot collide with a real chunk.
        temp = self._root / f".{key}.{uuid.uuid4().hex}.tmp"
        try:
            temp.write_bytes(value)
            os.replace(temp, self._root / key)
        except OSError as error:
            temp.unlink(missing_ok=True)
            if error.errno == errno.ENOSPC:
                raise StoreFull(key) from error
            raise


def create_store(url: str) -> FileChunkStore:
    """Build a store from a ``file:///directory`` URL. This is the plugin entry point."""
    parsed = parse_url(url, scheme="file", allowed_params=("max_value_bytes", "locality"))
    if not parsed.path:
        raise ValueError(
            f"file:// URLs need a directory, e.g. file:///dev/shm/bitdrop; got {url!r}"
        )

    locality = Locality.NODE
    if (raw_locality := parsed.params.get("locality")) is not None:
        try:
            locality = Locality(raw_locality)
        except ValueError:
            options = ", ".join(Locality)
            raise ValueError(
                f"file:// parameter 'locality' must be one of {options}, got {raw_locality!r}"
            ) from None

    return FileChunkStore(
        parsed.path, max_value_bytes=parsed.int_param("max_value_bytes"), locality=locality
    )
