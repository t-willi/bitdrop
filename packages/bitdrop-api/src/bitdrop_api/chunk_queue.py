"""The ``ChunkQueue`` contract: a queue of small messages that many consumers share.

In the queue workflow (``docs/decoupled-queue-workflow.md``) two queues connect the parts: a *work
queue* of chunk ids, from which source containers read, and a *file queue* of staged chunk names,
from which destination containers load. Both are a ``ChunkQueue``. A message is a few bytes that
name a chunk; the data itself never travels through the queue.

The queue delivers each message **at least once**: a consumer that takes a message and then dies
leaves it in flight, and after a while another consumer takes it over. So a consumer finishes its
work and only then ``ack``s. Doing the work twice must be harmless; for chunks the destination's
ledger makes it so.

How a message moves::

    publish --> WAITING --receive--> IN FLIGHT --ack--------------> gone
                   ^                    |  |--- dead_letter -----> DEAD (never delivered again)
                   |                    |
                   +------ release -----+   (hands back; the hand-back is not counted)
                   (reclaim: IN FLIGHT --> IN FLIGHT with a new holder, when the old one went quiet)

Every handing out of a message is a *delivery*, numbered from 1. Only the latest delivery is
current. Once a message is taken over, the earlier holder's delivery is stale and ``touch``,
``release`` and ``dead_letter`` fail with ``DeliveryLost``: a slow consumer can never undo the new
holder's claim. ``ack`` never fails, and it settles a message in flight whoever holds it, because
"this message's work is done" stays true whoever did it.
"""

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

__all__ = ["ChunkQueue", "Delivery", "QueueStats"]


@dataclass(frozen=True, slots=True)
class Delivery:
    """One handing out of a message to a consumer. Pass it back to ``ack``, ``touch``, ..."""

    message_id: str
    """Identifies the message across its deliveries. Assigned by the queue."""

    body: bytes
    """What was published, byte for byte."""

    consumer: str
    """Who holds this delivery."""

    deliveries: int
    """How many times the message has been handed out, this one included. A message that keeps
    coming back (a consumer dies on it every time) shows up here; the consumer decides when to
    ``dead_letter`` it."""


@dataclass(frozen=True, slots=True)
class QueueStats:
    """How many messages are in each place right now."""

    waiting: int
    in_flight: int
    dead: int


@runtime_checkable
class ChunkQueue(Protocol):
    """A shared queue with at-least-once delivery.

    Behaviour every implementation must provide (verified by the conformance kit):

    * A published message is kept until a consumer settles it (``ack``, ``dead_letter``).
    * A message is held by at most one consumer at a time.
    * The order of delivery is unspecified.
    * Consumer names follow :func:`bitdrop_api.keys.validate_key`; anything else raises
      ``InvalidKey``.
    * Message bodies are returned byte for byte, including empty ones.
    """

    async def publish(self, body: bytes) -> None:
        """Add a message. It is safely stored when this returns."""
        ...

    async def receive(
        self, consumer: str, *, max_messages: int = 1, block_seconds: float = 0.0
    ) -> list[Delivery]:
        """Take up to ``max_messages`` waiting messages for ``consumer``.

        With ``block_seconds`` above 0 and nothing waiting, wait up to that long for a message to
        arrive and return as soon as one does. An empty list means nothing came: it is a normal
        result, not an error.
        """
        ...

    async def reclaim(
        self, consumer: str, *, idle_seconds: float, max_messages: int = 1
    ) -> list[Delivery]:
        """Take over up to ``max_messages`` in-flight messages idle for at least ``idle_seconds``.

        This is how a message survives the death of its consumer. The previous holder's delivery
        becomes stale, and the new delivery's number is one higher.
        """
        ...

    async def touch(self, delivery: Delivery) -> None:
        """Say that the holder is still working on the message, which restarts its idle time.

        Raises:
            DeliveryLost: ``delivery`` is no longer the current delivery of its message.
        """
        ...

    async def ack(self, delivery: Delivery) -> None:
        """Say that the message is done; it is removed. Idempotent, and never fails.

        An acknowledgement from a holder whose message was taken over still removes it: the work
        was done, and doing it twice is harmless. A message that was handed back with ``release``
        and waits again is not removed by a late acknowledgement, because its holder said it was
        not finished.
        """
        ...

    async def release(self, delivery: Delivery) -> None:
        """Hand the message back to the queue because the holder stops on purpose.

        The message waits again, and the hand-back does not count: the next delivery has the same
        number this one had.

        Raises:
            DeliveryLost: ``delivery`` is no longer the current delivery of its message.
        """
        ...

    async def dead_letter(self, delivery: Delivery, reason: str) -> None:
        """Set the message aside for good: it will not be delivered again. ``reason`` is kept.

        Raises:
            DeliveryLost: ``delivery`` is no longer the current delivery of its message.
        """
        ...

    async def stats(self) -> QueueStats:
        """Count the messages that wait, are in flight, and were set aside."""
        ...

    async def aclose(self) -> None:
        """Release connections and other resources. The queue must not be used afterwards."""
        ...
