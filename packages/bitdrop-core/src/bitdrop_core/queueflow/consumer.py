"""The loop every container of the queue workflow runs: take a message, do the work, settle it.

A source container and a destination container differ only in their *handler*, the function that
does the work for one message. This loop supplies the rest:

* It takes messages from a queue, waiting up to ``poll_interval`` for new ones, and when there are
  none it takes over messages whose holder went quiet for ``idle_seconds`` (a container that died).
* While the handler works it *touches* the message every third of ``idle_seconds``, so a slow
  handler keeps its message and only a dead container loses it.
* A handler that raises is tried again, up to ``max_attempts`` times; then the message is dead
  lettered (set aside, counted, never delivered again). A message already delivered more often than
  that (it kept killing the containers that held it) is dead lettered without being tried.
* A handler may raise :class:`GiveUp` (the message can never succeed: dead letter at once) or
  :class:`Defer` (not now, e.g. the staging store is full: hand the message back, which does not
  use an attempt, and wait ``defer_delay``).
* When the loop is cancelled, the message in hand is handed back so another container gets it
  without losing an attempt.

The message is acknowledged only after the handler returned. Doing the work twice must be harmless,
which the destination's ledger guarantees for chunks. A handler may return a *cleanup*, run after
the acknowledgement: deleting a staged chunk belongs there, because a chunk deleted before the
acknowledgement could not be told from a chunk that was lost if the container died in between.
"""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from bitdrop_api.chunk_queue import ChunkQueue, Delivery
from bitdrop_api.errors import DeliveryLost, QueueError

__all__ = ["Cleanup", "ConsumerConfig", "ConsumerStats", "Defer", "GiveUp", "Handler", "consume"]

log = logging.getLogger("bitdrop.queueflow")

Cleanup = Callable[[], Awaitable[None]]
"""Work to do after the message is acknowledged, whose failure does not matter (it is logged)."""

Handler = Callable[[bytes], Awaitable[Cleanup | None]]
"""Does the work for one message body. Raising is a failed attempt. It may return a cleanup."""


class GiveUp(Exception):
    """Raised by a handler: this message can never succeed, set it aside now."""


class Defer(Exception):
    """Raised by a handler: not now. The message is handed back and tried later, uncounted."""


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    """How a consumer behaves."""

    max_attempts: int = 3
    """How many times a message may fail (or be delivered) before it is dead lettered."""

    poll_interval: float = 1.0
    """Seconds to wait for a new message before looking for silent holders to take over from."""

    idle_seconds: float = 60.0
    """A message nobody touched for this long is taken over. Touches come every third of it."""

    retry_delay: float = 0.5
    """Seconds to wait before trying a failed message again."""

    defer_delay: float = 1.0
    """Seconds to wait after handing a deferred message back."""

    def __post_init__(self) -> None:
        """Reject settings that would make the loop spin or never give up."""
        for name in ("max_attempts",):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1, got {getattr(self, name)}")
        for name in ("poll_interval", "idle_seconds"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive, got {getattr(self, name)}")
        for name in ("retry_delay", "defer_delay"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} cannot be negative, got {getattr(self, name)}")


@dataclass(slots=True)
class ConsumerStats:
    """What a consumer did."""

    handled: int = 0
    """Messages whose handler succeeded."""

    retried: int = 0
    """Failed attempts that were tried again."""

    deferred: int = 0
    """Messages handed back because the handler said not now."""

    dead: int = 0
    """Messages dead lettered."""

    taken_over: int = 0
    """Messages taken over from a silent holder."""


async def consume(
    queue: ChunkQueue,
    consumer: str,
    handler: Handler,
    config: ConsumerConfig,
    stop: asyncio.Event,
    stats: ConsumerStats | None = None,
) -> ConsumerStats:
    """Handle messages from ``queue`` as ``consumer`` until ``stop`` is set; return what was done.

    The message in hand when ``stop`` is set is finished first. Cancelling the task hands it back.
    """
    stats = stats if stats is not None else ConsumerStats()
    while not stop.is_set():
        deliveries = await queue.receive(consumer, block_seconds=config.poll_interval)
        if not deliveries:
            deliveries = await queue.reclaim(consumer, idle_seconds=config.idle_seconds)
            stats.taken_over += len(deliveries)
        for delivery in deliveries:
            await _process(queue, delivery, handler, config, stats)
    return stats


async def _process(
    queue: ChunkQueue,
    delivery: Delivery,
    handler: Handler,
    config: ConsumerConfig,
    stats: ConsumerStats,
) -> None:
    if delivery.deliveries > config.max_attempts:
        await _dead(queue, delivery, f"delivered {delivery.deliveries} times", stats)
        return
    attempts = config.max_attempts - (delivery.deliveries - 1)  # earlier deliveries used some
    holder = asyncio.create_task(_hold(queue, delivery, config.idle_seconds / 3))
    try:
        failure = ""
        for attempt in range(attempts):
            try:
                cleanup = await handler(delivery.body)
            except GiveUp as signal:
                await _dead(queue, delivery, str(signal) or "given up", stats)
                return
            except Defer:
                stats.deferred += 1
                await _hand_back(queue, delivery)
                await asyncio.sleep(config.defer_delay)
                return
            except Exception as error:
                failure = f"{type(error).__name__}: {error}"
                log.warning("message failed (attempt %d of %d): %s", attempt + 1, attempts, failure)
                if attempt + 1 < attempts:
                    stats.retried += 1
                    await asyncio.sleep(config.retry_delay)
            else:
                await queue.ack(delivery)
                stats.handled += 1
                if cleanup is not None:
                    await _clean_up(cleanup)
                return
        await _dead(queue, delivery, f"failed {attempts} times, last: {failure}", stats)
    except asyncio.CancelledError:
        await asyncio.shield(_hand_back(queue, delivery))
        raise
    finally:
        holder.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await holder


async def _clean_up(cleanup: Cleanup) -> None:
    try:
        await cleanup()
    except Exception:
        log.warning("cleaning up after a message failed; the work is done", exc_info=True)


async def _hold(queue: ChunkQueue, delivery: Delivery, interval: float) -> None:
    """Touch the message until cancelled, so nobody takes over a message being worked on."""
    while True:
        await asyncio.sleep(interval)
        try:
            await queue.touch(delivery)
        except DeliveryLost:
            return  # taken over after all: the work goes on, and the ledger absorbs the repeat
        except QueueError:
            log.warning("could not touch a message; it may be taken over", exc_info=True)


async def _dead(queue: ChunkQueue, delivery: Delivery, reason: str, stats: ConsumerStats) -> None:
    with contextlib.suppress(DeliveryLost):
        await queue.dead_letter(delivery, reason)
    stats.dead += 1
    log.error("message set aside: %s", reason)


async def _hand_back(queue: ChunkQueue, delivery: Delivery) -> None:
    with contextlib.suppress(DeliveryLost):
        await queue.release(delivery)
