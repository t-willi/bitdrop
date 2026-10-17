"""ChunkQueue adapter on Redis Streams.

Works with Redis, Valkey, and Dragonfly: select it with ``redis://[:password@]host:port/db``.
Options (all optional):

* ``bitdrop_queue``: the name of this queue (default ``default``). A job uses one queue for the
  work and one for the files, say ``work`` and ``files``; they are two URLs for one server.
* ``bitdrop_prefix``: every key is stored as ``<prefix>queue:<name>...`` so several users can share
  a server (default ``bitdrop:``).
* ``bitdrop_max_connections``: size of the connection pool (default 32); excess callers wait.

How the contract maps to Redis Streams. The stream ``<prefix>queue:<name>`` holds the messages
(one field, the body) and has one consumer group. A message that is waiting is an entry nobody has
read yet; one in flight is in the group's pending list under its holder; ``XACK`` plus ``XDEL``
settle it. A message that is handed back (``release``) is moved to a reserved consumer called
``__released__`` with a large idle time and its delivery counter lowered by one, so that the next
``receive`` finds it, claims it, and the counter reads as if the hand-back never happened. A name
starting with ``_`` cannot be a consumer name, so nobody can collide with it. Dead letters go to a
second stream ``<prefix>queue:<name>:dead``.

Run the server **with persistence** if the queue must survive a restart (``--appendonly yes``); a
queue that is lost can be rebuilt from the staged chunks (``docs/decoupled-queue-workflow.md``).
A blocked ``receive`` wakes for new messages at once but notices a message handed back by
``release`` only on its next call, so keep ``block_seconds`` short (about a second).
"""

import re
from collections.abc import Awaitable, Callable
from typing import Any

import redis.asyncio as aioredis
from redis import exceptions as redis_errors

from bitdrop_api.chunk_queue import Delivery, QueueStats
from bitdrop_api.errors import DeliveryLost, QueueUnavailable
from bitdrop_api.keys import validate_key
from bitdrop_api.urls import parse_network_url

__all__ = ["DEFAULT_PREFIX", "DEFAULT_QUEUE", "RedisChunkQueue", "create_queue"]

DEFAULT_PREFIX = "bitdrop:"
DEFAULT_QUEUE = "default"
DEFAULT_MAX_CONNECTIONS = 32

_OPTIONS = ("bitdrop_queue", "bitdrop_prefix", "bitdrop_max_connections")
# No glob characters and no whitespace, so the names can safely be used in key patterns.
_NAME = re.compile(r"[A-Za-z0-9._:-]{1,64}")

_GROUP = "bitdrop"
_BODY = b"b"
_RELEASED = "__released__"
"""The reserved consumer that holds messages which wait again after a ``release``."""

_RELEASED_IDLE_MS = 24 * 3600 * 1000
_RELEASED_MIN_IDLE_MS = 3600 * 1000
"""A handed-back message looks a day idle, and is only claimed from ``__released__`` when it is at
least an hour idle: two consumers racing for it cannot both win (the winner's claim resets the
idle time)."""


class RedisChunkQueue:
    """A :class:`~bitdrop_api.chunk_queue.ChunkQueue` on a Redis stream and one consumer group."""

    def __init__(self, url: str, *, client: aioredis.Redis | None = None) -> None:
        """Create a queue for the server at ``url``. Nothing is sent until the first operation.

        ``client`` replaces the connection built from the URL; tests hand in an in-process fake.

        Raises:
            ValueError: the URL has another scheme, or an invalid or unknown ``bitdrop_*`` option.
        """
        parsed = parse_network_url(url, schemes=("redis",), options=_OPTIONS)
        prefix = parsed.str_option("bitdrop_prefix", default=DEFAULT_PREFIX)
        name = parsed.str_option("bitdrop_queue", default=DEFAULT_QUEUE)
        for option, value in (("bitdrop_prefix", prefix), ("bitdrop_queue", name)):
            if _NAME.fullmatch(value) is None:
                raise ValueError(
                    f"{option} must be 1 to 64 characters from A-Z a-z 0-9 . _ : -, got {value!r}"
                )
        self._prefix = prefix
        self._name = name
        self._stream = f"{prefix}queue:{name}"
        self._dead_stream = f"{self._stream}:dead"
        self._max_connections = parsed.int_option(
            "bitdrop_max_connections", default=DEFAULT_MAX_CONNECTIONS
        )
        self._redacted = parsed.redacted
        if client is None:
            pool = aioredis.BlockingConnectionPool.from_url(
                parsed.url, max_connections=self._max_connections, timeout=None
            )
            client = aioredis.Redis(connection_pool=pool)
        # redis-py's type stubs do not describe the replies of the stream commands (they allow
        # several shapes for each), so the client is used untyped; the tests cover every call.
        self._client: Any = client
        self._group_ready = False

    @property
    def stream(self) -> str:
        """The key of the stream that holds the messages."""
        return self._stream

    @property
    def max_connections(self) -> int:
        """The size of the connection pool."""
        return self._max_connections

    # -- the contract ---------------------------------------------------------------------------

    async def publish(self, body: bytes) -> None:
        """Append the message to the stream."""
        await self._call(lambda: self._client.xadd(self._stream, {_BODY: bytes(body)}))

    async def receive(
        self, consumer: str, *, max_messages: int = 1, block_seconds: float = 0.0
    ) -> list[Delivery]:
        """Take messages that were handed back first, then new ones (waiting up to the time)."""
        validate_key(consumer)
        _check_count(max_messages)
        if block_seconds < 0:
            raise ValueError(f"block_seconds cannot be negative, got {block_seconds}")
        return await self._call(lambda: self._receive(consumer, max_messages, block_seconds))

    async def reclaim(
        self, consumer: str, *, idle_seconds: float, max_messages: int = 1
    ) -> list[Delivery]:
        """Take over in-flight messages that have been idle at least ``idle_seconds``."""
        validate_key(consumer)
        _check_count(max_messages)
        return await self._call(lambda: self._reclaim(consumer, idle_seconds, max_messages))

    async def touch(self, delivery: Delivery) -> None:
        """Restart the idle time of a message the caller still holds."""

        async def operation() -> None:
            await self._require_current(delivery)
            await self._client.xclaim(
                self._stream, _GROUP, delivery.consumer, 0, [delivery.message_id], justid=True
            )

        await self._call(operation)

    async def ack(self, delivery: Delivery) -> None:
        """Remove the message if it is in flight, whoever holds it; leave a handed-back one."""

        async def operation() -> None:
            entry = await self._pending_entry(delivery.message_id)
            if entry is None or _text(entry["consumer"]) == _RELEASED:
                return
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.xack(self._stream, _GROUP, delivery.message_id)
                pipe.xdel(self._stream, delivery.message_id)
                await pipe.execute()

        await self._call(operation)

    async def release(self, delivery: Delivery) -> None:
        """Hand the message back: it waits again and the hand-back is not counted."""

        async def operation() -> None:
            await self._require_current(delivery)
            await self._client.xclaim(
                self._stream,
                _GROUP,
                _RELEASED,
                0,
                [delivery.message_id],
                idle=_RELEASED_IDLE_MS,
                retrycount=delivery.deliveries - 1,
                justid=True,
            )

        await self._call(operation)

    async def dead_letter(self, delivery: Delivery, reason: str) -> None:
        """Move the message to the dead-letter stream, with the reason."""

        async def operation() -> None:
            await self._require_current(delivery)
            body = await self._body_of(delivery.message_id)
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.xadd(
                    self._dead_stream,
                    {
                        _BODY: body,
                        b"reason": reason.encode(),
                        b"deliveries": str(delivery.deliveries).encode(),
                    },
                )
                pipe.xack(self._stream, _GROUP, delivery.message_id)
                pipe.xdel(self._stream, delivery.message_id)
                await pipe.execute()

        await self._call(operation)

    async def stats(self) -> QueueStats:
        """Count waiting (never read, or handed back), in flight, and dead messages."""

        async def operation() -> QueueStats:
            length = int(await self._client.xlen(self._stream))
            summary = await self._client.xpending(self._stream, _GROUP)
            pending = int(summary["pending"])
            released = _pending_of(summary, _RELEASED)
            dead = int(await self._client.xlen(self._dead_stream))
            return QueueStats(
                waiting=length - pending + released, in_flight=pending - released, dead=dead
            )

        return await self._call(operation)

    async def aclose(self) -> None:
        """Close the client and its connections."""
        await self._client.aclose(close_connection_pool=True)

    # -- internals ------------------------------------------------------------------------------

    async def _receive(
        self, consumer: str, max_messages: int, block_seconds: float
    ) -> list[Delivery]:
        delivered = await self._take_released(consumer, max_messages)
        wanted = max_messages - len(delivered)
        if wanted > 0:
            block = None if delivered or block_seconds <= 0 else max(1, int(block_seconds * 1000))
            reply = await self._client.xreadgroup(
                _GROUP, consumer, {self._stream: ">"}, count=wanted, block=block
            )
            for _stream, entries in reply or []:
                for entry_id, fields in entries:
                    delivered.append(await self._delivery(_text(entry_id), fields, consumer))
        return delivered

    async def _take_released(self, consumer: str, limit: int) -> list[Delivery]:
        pending = await self._client.xpending_range(
            self._stream, _GROUP, "-", "+", limit, consumername=_RELEASED
        )
        ids = [_text(entry["message_id"]) for entry in pending]
        if not ids:
            return []
        claimed = await self._client.xclaim(
            self._stream, _GROUP, consumer, _RELEASED_MIN_IDLE_MS, ids
        )
        return [await self._delivery(_text(i), fields, consumer) for i, fields in claimed if fields]

    async def _reclaim(
        self, consumer: str, idle_seconds: float, max_messages: int
    ) -> list[Delivery]:
        summary = await self._client.xpending(self._stream, _GROUP)
        if not summary["pending"]:
            return []
        idle_ms = max(0, int(idle_seconds * 1000))
        # Handed-back messages look very idle: ask for enough entries to skip past them.
        entries = await self._client.xpending_range(
            self._stream,
            _GROUP,
            "-",
            "+",
            max_messages + _pending_of(summary, _RELEASED),
            idle=idle_ms,
        )
        ids = [_text(e["message_id"]) for e in entries if _text(e["consumer"]) != _RELEASED]
        if not ids:
            return []
        claimed = await self._client.xclaim(
            self._stream, _GROUP, consumer, idle_ms, ids[:max_messages]
        )
        return [await self._delivery(_text(i), fields, consumer) for i, fields in claimed if fields]

    async def _delivery(self, entry_id: str, fields: dict[bytes, bytes], consumer: str) -> Delivery:
        """Describe a message just handed to ``consumer``; its number is the server's counter."""
        entry = await self._pending_entry(entry_id)
        deliveries = int(entry["times_delivered"]) if entry else 1
        return Delivery(entry_id, bytes(fields[_BODY]), consumer, deliveries)

    async def _pending_entry(self, entry_id: str) -> dict[str, Any] | None:
        entries = await self._client.xpending_range(self._stream, _GROUP, entry_id, entry_id, 1)
        return entries[0] if entries else None

    async def _require_current(self, delivery: Delivery) -> None:
        """Raise DeliveryLost unless ``delivery`` is its message's current delivery."""
        entry = await self._pending_entry(delivery.message_id)
        if (
            entry is None
            or _text(entry["consumer"]) != delivery.consumer
            or int(entry["times_delivered"]) != delivery.deliveries
        ):
            raise DeliveryLost(delivery)

    async def _body_of(self, entry_id: str) -> bytes:
        entries = await self._client.xrange(self._stream, entry_id, entry_id, count=1)
        return bytes(entries[0][1][_BODY]) if entries else b""

    async def _call[T](self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run ``operation`` with the consumer group in place, translating Redis failures."""
        for attempt in (1, 2):
            try:
                await self._ensure_group()
                return await operation()
            except redis_errors.ResponseError as error:
                if "NOGROUP" in str(error) and attempt == 1:
                    self._group_ready = False  # the stream was deleted under us: create it again
                    continue
                raise QueueUnavailable(self._describe(error)) from error
            except redis_errors.RedisError as error:
                raise QueueUnavailable(self._describe(error)) from error
        raise AssertionError("unreachable")  # pragma: no cover

    async def _ensure_group(self) -> None:
        if self._group_ready:
            return
        try:
            await self._client.xgroup_create(self._stream, _GROUP, id="0", mkstream=True)
        except redis_errors.ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise
        self._group_ready = True

    def _describe(self, error: Exception) -> str:
        return f"{self._redacted}: {type(error).__name__}: {error}"


def _check_count(max_messages: int) -> None:
    if max_messages < 1:
        raise ValueError(f"max_messages must be at least 1, got {max_messages}")


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _pending_of(summary: dict[str, Any], consumer: str) -> int:
    """How many pending entries ``consumer`` holds, from an ``XPENDING`` summary."""
    for entry in summary.get("consumers") or []:
        if _text(entry["name"]) == consumer:
            return int(entry["pending"])
    return 0


def create_queue(url: str) -> RedisChunkQueue:
    """Build a queue from a ``redis://`` URL. This is the plugin entry point."""
    return RedisChunkQueue(url)
