"""ChunkQueue adapter keeping messages in this process's memory.

Useful for tests and single-process runs; nothing survives a restart, and separate queues opened
with ``memory://`` are independent of each other (it takes no parameters).
"""

import asyncio
import itertools
import time
from collections.abc import Callable
from dataclasses import dataclass

from bitdrop_api.chunk_queue import Delivery, QueueStats
from bitdrop_api.errors import DeliveryLost
from bitdrop_api.keys import validate_key
from bitdrop_api.urls import parse_url

__all__ = ["MemoryChunkQueue", "create_queue"]


@dataclass(slots=True)
class _Message:
    """One message and where it stands."""

    message_id: str
    body: bytes
    deliveries: int = 0
    holder: str | None = None
    touched: float = 0.0
    dead_reason: str | None = None


class MemoryChunkQueue:
    """A :class:`~bitdrop_api.chunk_queue.ChunkQueue` backed by plain dicts.

    Safe under asyncio: no method awaits while it changes state, so every operation is atomic with
    respect to other tasks. Insertion-ordered dicts serve as ordered sets, so messages are handed
    out oldest first (the contract does not promise that).
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        """Create an empty queue. ``clock`` (seconds) is injectable so tests can control time."""
        self._clock = clock
        self._ids = itertools.count(1)
        self._waiting: dict[str, _Message] = {}
        self._in_flight: dict[str, _Message] = {}
        self._dead: dict[str, _Message] = {}
        self._sleepers: list[asyncio.Future[None]] = []

    async def publish(self, body: bytes) -> None:
        """Add a message and wake the consumers that wait for one."""
        message = _Message(message_id=str(next(self._ids)), body=bytes(body))
        self._waiting[message.message_id] = message
        self._wake()

    async def receive(
        self, consumer: str, *, max_messages: int = 1, block_seconds: float = 0.0
    ) -> list[Delivery]:
        """Take waiting messages; see :meth:`ChunkQueue.receive`."""
        validate_key(consumer)
        if max_messages < 1:
            raise ValueError(f"max_messages must be at least 1, got {max_messages}")
        if block_seconds < 0:
            raise ValueError(f"block_seconds cannot be negative, got {block_seconds}")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + block_seconds
        while True:
            if self._waiting:
                return self._hand_out(self._waiting, consumer, max_messages)
            remaining = deadline - loop.time()
            if remaining <= 0:
                return []
            sleeper = loop.create_future()
            self._sleepers.append(sleeper)
            try:
                await asyncio.wait_for(sleeper, remaining)
            except TimeoutError:
                return []
            finally:
                if sleeper in self._sleepers:
                    self._sleepers.remove(sleeper)

    async def reclaim(
        self, consumer: str, *, idle_seconds: float, max_messages: int = 1
    ) -> list[Delivery]:
        """Take over in-flight messages that have been idle long enough."""
        validate_key(consumer)
        if max_messages < 1:
            raise ValueError(f"max_messages must be at least 1, got {max_messages}")
        now = self._clock()
        quiet = {
            message_id: message
            for message_id, message in self._in_flight.items()
            if now - message.touched >= idle_seconds
        }
        return self._hand_out(quiet, consumer, max_messages)

    async def touch(self, delivery: Delivery) -> None:
        """Restart the idle time of a message the caller still holds."""
        self._current(delivery).touched = self._clock()

    async def ack(self, delivery: Delivery) -> None:
        """Remove the message if it is in flight, whoever holds it now.

        A message that was handed back and waits again stays: its holder said it was not finished.
        """
        self._in_flight.pop(delivery.message_id, None)

    async def release(self, delivery: Delivery) -> None:
        """Put the message back to wait, without counting the delivery."""
        message = self._current(delivery)
        del self._in_flight[message.message_id]
        message.deliveries -= 1
        message.holder = None
        self._waiting[message.message_id] = message
        self._wake()

    async def dead_letter(self, delivery: Delivery, reason: str) -> None:
        """Set the message aside for good."""
        message = self._current(delivery)
        del self._in_flight[message.message_id]
        message.holder = None
        message.dead_reason = reason
        self._dead[message.message_id] = message

    async def stats(self) -> QueueStats:
        """Count the messages in each place."""
        return QueueStats(
            waiting=len(self._waiting), in_flight=len(self._in_flight), dead=len(self._dead)
        )

    async def aclose(self) -> None:
        """Forget every message and release the consumers that wait."""
        self._waiting.clear()
        self._in_flight.clear()
        self._dead.clear()
        self._wake()

    # -- internals ------------------------------------------------------------------------------

    def _hand_out(
        self, candidates: dict[str, _Message], consumer: str, limit: int
    ) -> list[Delivery]:
        """Give up to ``limit`` of ``candidates`` to ``consumer`` as their next delivery."""
        now = self._clock()
        delivered: list[Delivery] = []
        for message_id, message in list(candidates.items())[:limit]:
            self._waiting.pop(message_id, None)
            message.deliveries += 1
            message.holder = consumer
            message.touched = now
            self._in_flight[message_id] = message
            delivered.append(Delivery(message_id, message.body, consumer, message.deliveries))
        return delivered

    def _current(self, delivery: Delivery) -> _Message:
        """Return the message if ``delivery`` is its current delivery, else raise DeliveryLost."""
        message = self._in_flight.get(delivery.message_id)
        if (
            message is None
            or message.holder != delivery.consumer
            or message.deliveries != delivery.deliveries
        ):
            raise DeliveryLost(delivery)
        return message

    def _wake(self) -> None:
        for sleeper in self._sleepers:
            if not sleeper.done():
                sleeper.set_result(None)


def create_queue(url: str) -> MemoryChunkQueue:
    """Build a queue from a ``memory://`` URL. This is the plugin entry point."""
    parsed = parse_url(url, scheme="memory", allowed_params=())
    if parsed.path:
        raise ValueError(f"memory:// URLs take no path, found {parsed.path!r} in {url!r}")
    return MemoryChunkQueue()
