"""ChunkStore adapter for any server speaking the Redis protocol.

Works with Redis, Valkey, Dragonfly, and Garnet: select it with ``redis://[:password@]host:port/db``.
Options (all optional):

* ``bitdrop_prefix``: every key is stored as ``<prefix><key>``, so several users can share a server
  (default ``bitdrop:``).
* ``bitdrop_max_value_bytes``: largest accepted value; set it to what the server is configured for
  (default 512 MiB, the Redis limit).
* ``bitdrop_max_connections``: size of the connection pool (default 32); excess callers wait.

Any other option (``socket_timeout``, ...) goes to the redis-py client unchanged.

Run the data-plane server **without persistence** (``--save "" --appendonly no``) and with
``maxmemory-policy noeviction``: the source is the durable copy, a snapshot fork could double memory
use, and a full server should refuse writes (``StoreFull``, which the runner turns into
backpressure) instead of silently dropping chunks.
"""

import re
from collections.abc import Awaitable
from typing import Any

import redis.asyncio as aioredis
from redis import exceptions as redis_errors

from bitdrop_api.chunk_store import Locality, StoreCapabilities, StoreStats
from bitdrop_api.errors import ChunkNotFound, ChunkTooLarge, StoreFull, StoreUnavailable
from bitdrop_api.keys import validate_key
from bitdrop_api.urls import parse_network_url

__all__ = [
    "DEFAULT_MAX_CONNECTIONS",
    "DEFAULT_MAX_VALUE_BYTES",
    "DEFAULT_PREFIX",
    "RedisChunkStore",
    "create_store",
]

DEFAULT_PREFIX = "bitdrop:"
DEFAULT_MAX_VALUE_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_CONNECTIONS = 32

_OPTIONS = ("bitdrop_prefix", "bitdrop_max_value_bytes", "bitdrop_max_connections")
# No glob characters (* ? [ ]) and no whitespace, so a prefix can safely be used in key patterns.
_PREFIX = re.compile(r"[A-Za-z0-9._:-]{1,64}")


class RedisChunkStore:
    """A :class:`~bitdrop_api.chunk_store.ChunkStore` storing chunks as Redis strings.

    ``put`` is ``SET`` (atomic, so readers never see a half-written chunk), ``get`` is ``GET``, and
    ``delete`` is ``UNLINK``, which frees large values without blocking the server. Connections
    are opened lazily and pooled.
    """

    def __init__(self, url: str) -> None:
        """Create a store for the server at ``url``. Nothing is sent until the first operation.

        Raises:
            ValueError: the URL has another scheme, or an invalid or unknown ``bitdrop_*`` option.
        """
        parsed = parse_network_url(url, schemes=("redis",), options=_OPTIONS)
        self._prefix = parsed.str_option("bitdrop_prefix", default=DEFAULT_PREFIX)
        if _PREFIX.fullmatch(self._prefix) is None:
            raise ValueError(
                f"bitdrop_prefix must be 1 to 64 characters from A-Z a-z 0-9 . _ : -, "
                f"got {self._prefix!r}"
            )
        self._max_connections = parsed.int_option(
            "bitdrop_max_connections", default=DEFAULT_MAX_CONNECTIONS
        )
        self._capabilities = StoreCapabilities(
            max_value_bytes=parsed.int_option(
                "bitdrop_max_value_bytes", default=DEFAULT_MAX_VALUE_BYTES
            ),
            locality=Locality.CLUSTER,
            memory_stats=True,
        )
        self._redacted = parsed.redacted
        # A blocking pool makes callers beyond max_connections wait their turn instead of failing.
        self._pool = aioredis.BlockingConnectionPool.from_url(
            parsed.url, max_connections=self._max_connections, timeout=None
        )
        self._client = aioredis.Redis(connection_pool=self._pool)

    @property
    def prefix(self) -> str:
        """The string prepended to every key on the server."""
        return self._prefix

    @property
    def max_connections(self) -> int:
        """The size of the connection pool."""
        return self._max_connections

    @property
    def socket_timeout(self) -> float | None:
        """The client's socket timeout in seconds, or ``None`` for none."""
        timeout: float | None = self._pool.connection_kwargs.get("socket_timeout")
        return timeout

    async def put(self, key: str, value: bytes) -> None:
        """Store ``value`` under ``key``; see :meth:`ChunkStore.put`."""
        validate_key(key)
        limit = self._capabilities.max_value_bytes
        if limit is not None and len(value) > limit:
            raise ChunkTooLarge(key, len(value), limit)
        await self._call(self._client.set(self._prefix + key, value), key)

    async def get(self, key: str) -> bytes:
        """Return the value under ``key``; see :meth:`ChunkStore.get`."""
        validate_key(key)
        value = await self._call(self._client.get(self._prefix + key), key)
        if value is None:
            raise ChunkNotFound(key)
        return bytes(value)

    async def delete(self, key: str) -> None:
        """Remove ``key`` if present; see :meth:`ChunkStore.delete`."""
        validate_key(key)
        await self._call(self._client.unlink(self._prefix + key), key)

    def capabilities(self) -> StoreCapabilities:
        """Return the static self-description of this store."""
        return self._capabilities

    async def stats(self) -> StoreStats:
        """Return the server's ``used_memory`` and ``maxmemory`` (``None`` if unlimited)."""
        info = await self._call(self._client.info("memory"), None)
        return StoreStats(
            used_bytes=int(info["used_memory"]), max_bytes=int(info.get("maxmemory", 0)) or None
        )

    async def aclose(self) -> None:
        """Close the client and its connections."""
        await self._client.aclose(close_connection_pool=True)

    async def _call(self, command: Awaitable[Any], key: str | None) -> Any:
        """Await a Redis command and translate its failures into bitdrop errors."""
        try:
            return await command
        except redis_errors.ResponseError as error:
            if _is_out_of_memory(error) and key is not None:
                raise StoreFull(key) from error
            raise StoreUnavailable(self._describe(error), key=key) from error
        except redis_errors.RedisError as error:
            raise StoreUnavailable(self._describe(error), key=key) from error

    def _describe(self, error: Exception) -> str:
        return f"{self._redacted}: {type(error).__name__}: {error}"


def _is_out_of_memory(error: redis_errors.ResponseError) -> bool:
    """Redis and Valkey say ``OOM command not allowed ...``; Dragonfly says ``Out of memory``."""
    return isinstance(error, redis_errors.OutOfMemoryError) or "out of memory" in str(error).lower()


def create_store(url: str) -> RedisChunkStore:
    """Build a store from a ``redis://`` URL. This is the plugin entry point."""
    return RedisChunkStore(url)
