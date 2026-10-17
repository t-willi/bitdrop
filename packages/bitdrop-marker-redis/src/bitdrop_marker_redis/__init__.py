"""MarkerStore adapter for any server speaking the Redis protocol.

Works with Redis, Valkey, and Dragonfly: select it with ``redis://[:password@]host:port/db``.
Options (all optional):

* ``bitdrop_prefix``: every key is stored as ``<prefix>marker:<key>``, so several users can share a
  server (default ``bitdrop:``).
* ``bitdrop_max_connections``: size of the connection pool (default 32); excess callers wait.

``set_if_absent`` is ``SET key value NX`` (atomic on the server), followed by ``GET`` when the key
was already set. Markers are the coordination of a job, so run the server **with persistence** if
the job must survive a restart of it.
"""

import re
from collections.abc import Awaitable
from typing import Any

import redis.asyncio as aioredis
from redis import exceptions as redis_errors

from bitdrop_api.errors import StoreUnavailable
from bitdrop_api.keys import validate_key
from bitdrop_api.urls import parse_network_url

__all__ = ["DEFAULT_PREFIX", "RedisMarkerStore", "create_store"]

DEFAULT_PREFIX = "bitdrop:"
DEFAULT_MAX_CONNECTIONS = 32

_OPTIONS = ("bitdrop_prefix", "bitdrop_max_connections")
_PREFIX = re.compile(r"[A-Za-z0-9._:-]{1,64}")
_ATTEMPTS = 5
"""How often to look again when the key that blocked a write is gone by the time it is read."""


class RedisMarkerStore:
    """A :class:`~bitdrop_api.marker_store.MarkerStore` keeping each marker in one Redis string."""

    def __init__(self, url: str, *, client: aioredis.Redis | None = None) -> None:
        """Create a store for the server at ``url``. Nothing is sent until the first operation.

        ``client`` replaces the connection built from the URL; tests hand in an in-process fake.

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
        self._redacted = parsed.redacted
        if client is None:
            pool = aioredis.BlockingConnectionPool.from_url(
                parsed.url, max_connections=self._max_connections, timeout=None
            )
            client = aioredis.Redis(connection_pool=pool)
        self._client = client

    @property
    def prefix(self) -> str:
        """The string prepended to every key on the server (before ``marker:``)."""
        return self._prefix

    async def set_if_absent(self, key: str, value: bytes) -> bytes:
        """Store ``value`` unless ``key`` is set; return the value stored afterwards."""
        validate_key(key)
        name = self._name(key)
        for _ in range(_ATTEMPTS):
            if await self._call(self._client.set(name, bytes(value), nx=True), key):
                return bytes(value)
            stored = await self._call(self._client.get(name), key)
            if stored is not None:
                return bytes(stored)
            # The key that blocked the write was deleted before we read it: try again.
        raise StoreUnavailable(f"{self._redacted}: marker {key!r} kept changing", key=key)

    async def get(self, key: str) -> bytes | None:
        """Return the value under ``key``, or ``None``."""
        validate_key(key)
        stored = await self._call(self._client.get(self._name(key)), key)
        return None if stored is None else bytes(stored)

    async def aclose(self) -> None:
        """Close the client and its connections."""
        await self._client.aclose(close_connection_pool=True)

    def _name(self, key: str) -> str:
        return f"{self._prefix}marker:{key}"

    async def _call(self, command: Awaitable[Any], key: str) -> Any:
        try:
            return await command
        except redis_errors.RedisError as error:
            detail = f"{self._redacted}: {type(error).__name__}: {error}"
            raise StoreUnavailable(detail, key=key) from error


def create_store(url: str) -> RedisMarkerStore:
    """Build a store from a ``redis://`` URL. This is the plugin entry point."""
    return RedisMarkerStore(url)
