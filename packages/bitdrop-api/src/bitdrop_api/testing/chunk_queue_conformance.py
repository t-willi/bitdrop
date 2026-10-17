"""Conformance tests for the :class:`~bitdrop_api.chunk_queue.ChunkQueue` contract.

An adapter is *supported* if and only if it passes this kit (see ``docs/principles.md``, P1).

How to use it in an adapter package::

    class TestMyQueueConformance(ChunkQueueConformance):
        @pytest.fixture
        async def queue(self) -> AsyncIterator[ChunkQueue]:
            queue = MyQueue(...)       # empty, and independent of the other tests' queues
            yield queue
            await queue.aclose()

The timing tests use real, very short idle times and real sleeping, so they work against any
backend without a controllable clock. Loops are bounded, so a broken queue fails a test instead of
hanging it.
"""

import asyncio
import time
from collections.abc import AsyncIterator

import pytest

from bitdrop_api.chunk_queue import ChunkQueue, Delivery
from bitdrop_api.errors import DeliveryLost, InvalidKey

IDLE = 0.05
"""A short idle time to reclaim after."""

PAST_IDLE = 0.15
"""How long to sleep so that ``IDLE`` has surely passed."""

LONG_IDLE = 60.0
"""An idle time no test reaches."""

BODIES = [
    b"",
    b"\x00",
    b"\x00middle\x00",
    b'{"job":"j","chunk":"id.1.10000"}',
    bytes(range(256)),
    b"\xff" * 1000 + b"\x00",
    "ümläut ✓".encode(),
]
"""Bodies that a careless adapter would change: empty, NULs at both ends, every byte, UTF-8."""


async def _publish_all(queue: ChunkQueue, count: int) -> list[bytes]:
    bodies = [f"message-{i}".encode() for i in range(count)]
    for body in bodies:
        await queue.publish(body)
    return bodies


async def _take(queue: ChunkQueue, consumer: str = "consumer-1") -> Delivery:
    delivered = await queue.receive(consumer)
    assert len(delivered) == 1, f"expected one waiting message, received {len(delivered)}"
    return delivered[0]


async def _drain(queue: ChunkQueue, consumer: str, *, limit: int = 500) -> list[Delivery]:
    """Receive and ack until nothing is left, at most ``limit`` messages."""
    taken: list[Delivery] = []
    while len(taken) < limit:
        delivered = await queue.receive(consumer)
        if not delivered:
            break
        for delivery in delivered:
            taken.append(delivery)
            await queue.ack(delivery)
        await asyncio.sleep(0)  # let other consumers interleave
    return taken


class ChunkQueueConformance:
    """Base class of the contract tests. Subclass it and override the ``queue`` fixture."""

    @pytest.fixture
    async def queue(self) -> AsyncIterator[ChunkQueue]:
        """Yield an empty queue, independent of every other test's."""
        raise NotImplementedError("override the `queue` fixture in your Test subclass")
        yield  # pragma: no cover  # noqa: PT022

    # -- publishing and receiving ---------------------------------------------------------------

    async def test_queue_satisfies_the_protocol(self, queue: ChunkQueue) -> None:
        """The object structurally implements every method of ``ChunkQueue``."""
        assert isinstance(queue, ChunkQueue)

    async def test_a_published_message_is_received_once_with_its_body(
        self, queue: ChunkQueue
    ) -> None:
        """A published message is received once with its body."""
        await queue.publish(b"hello")

        delivery = await _take(queue, "consumer-1")

        assert delivery.body == b"hello"
        assert delivery.consumer == "consumer-1"
        assert delivery.deliveries == 1
        assert delivery.message_id

    @pytest.mark.parametrize("body", BODIES)
    async def test_bodies_come_back_byte_for_byte(self, queue: ChunkQueue, body: bytes) -> None:
        """Bodies come back byte for byte."""
        await queue.publish(body)

        assert (await _take(queue)).body == body

    async def test_an_empty_queue_gives_an_empty_list_at_once(self, queue: ChunkQueue) -> None:
        """An empty queue gives an empty list at once."""
        started = time.monotonic()

        assert await queue.receive("consumer-1") == []
        assert time.monotonic() - started < 0.5

    async def test_receiving_can_wait_for_a_message_that_arrives(self, queue: ChunkQueue) -> None:
        """Receiving can wait for a message that arrives."""

        async def publish_soon() -> None:
            await asyncio.sleep(0.1)
            await queue.publish(b"late")

        publisher = asyncio.create_task(publish_soon())
        started = time.monotonic()

        delivered = await queue.receive("consumer-1", block_seconds=5.0)

        await publisher
        assert [d.body for d in delivered] == [b"late"]
        assert time.monotonic() - started < 2.0, "it waited for the whole time instead of waking"

    async def test_waiting_for_nothing_gives_up_after_the_time(self, queue: ChunkQueue) -> None:
        """Waiting for nothing gives up after the time."""
        started = time.monotonic()

        delivered = await queue.receive("consumer-1", block_seconds=0.2)

        assert delivered == []
        assert 0.15 <= time.monotonic() - started < 2.0

    async def test_max_messages_bounds_a_batch(self, queue: ChunkQueue) -> None:
        """Max messages bounds a batch."""
        await _publish_all(queue, 5)

        first = await queue.receive("consumer-1", max_messages=3)
        second = await queue.receive("consumer-1", max_messages=3)

        assert (len(first), len(second)) == (3, 2)
        assert len({d.message_id for d in first + second}) == 5

    async def test_every_message_is_delivered_exactly_once_to_competing_consumers(
        self, queue: ChunkQueue
    ) -> None:
        """Every message is delivered exactly once to competing consumers."""
        published = await _publish_all(queue, 120)

        results = await asyncio.gather(*(_drain(queue, f"consumer-{i}") for i in range(4)))

        received = [d.body for taken in results for d in taken]
        assert sorted(received) == sorted(published), "messages were lost or handed out twice"

    async def test_a_message_in_flight_is_not_handed_to_anyone_else(
        self, queue: ChunkQueue
    ) -> None:
        """A message in flight is not handed to anyone else."""
        await queue.publish(b"one")
        await _take(queue, "consumer-1")

        assert await queue.receive("consumer-2") == []

    # -- settling -------------------------------------------------------------------------------

    async def test_an_acknowledged_message_is_gone(self, queue: ChunkQueue) -> None:
        """An acknowledged message is gone."""
        await queue.publish(b"one")
        delivery = await _take(queue)

        await queue.ack(delivery)

        assert await queue.receive("consumer-2") == []
        assert await queue.reclaim("consumer-2", idle_seconds=0.0) == []
        stats = await queue.stats()
        assert (stats.waiting, stats.in_flight, stats.dead) == (0, 0, 0)

    async def test_acknowledging_twice_is_harmless(self, queue: ChunkQueue) -> None:
        """Acknowledging twice is harmless."""
        await queue.publish(b"one")
        delivery = await _take(queue)

        await queue.ack(delivery)
        await queue.ack(delivery)

    async def test_a_released_message_waits_again_and_the_hand_back_is_not_counted(
        self, queue: ChunkQueue
    ) -> None:
        """A released message waits again and the hand back is not counted."""
        await queue.publish(b"one")
        first = await _take(queue, "consumer-1")

        await queue.release(first)
        again = await _take(queue, "consumer-2")

        assert again.body == b"one"
        assert again.message_id == first.message_id
        assert again.deliveries == 1, "a hand-back must not count as a failed attempt"

    async def test_a_delivery_cannot_be_released_twice(self, queue: ChunkQueue) -> None:
        """A delivery cannot be released twice."""
        await queue.publish(b"one")
        delivery = await _take(queue)
        await queue.release(delivery)
        await _take(queue, "consumer-2")

        with pytest.raises(DeliveryLost):
            await queue.release(delivery)

    async def test_a_dead_lettered_message_is_never_delivered_again(
        self, queue: ChunkQueue
    ) -> None:
        """A dead lettered message is never delivered again."""
        await queue.publish(b"poison")
        delivery = await _take(queue)

        await queue.dead_letter(delivery, "cannot be read")

        assert await queue.receive("consumer-2") == []
        assert await queue.reclaim("consumer-2", idle_seconds=0.0) == []
        stats = await queue.stats()
        assert (stats.waiting, stats.in_flight, stats.dead) == (0, 0, 1)

    async def test_stats_count_each_place(self, queue: ChunkQueue) -> None:
        """Stats count each place."""
        await _publish_all(queue, 4)
        held = await _take(queue)
        await _take(queue, "consumer-2")
        await queue.dead_letter(held, "bad")

        stats = await queue.stats()

        assert (stats.waiting, stats.in_flight, stats.dead) == (2, 1, 1)

    # -- consumers that go quiet ----------------------------------------------------------------

    async def test_a_message_is_not_reclaimed_before_it_has_been_idle_long_enough(
        self, queue: ChunkQueue
    ) -> None:
        """A message is not reclaimed before it has been idle long enough."""
        await queue.publish(b"one")
        await _take(queue)

        assert await queue.reclaim("consumer-2", idle_seconds=LONG_IDLE) == []

    async def test_a_message_whose_holder_went_quiet_is_taken_over(self, queue: ChunkQueue) -> None:
        """A message whose holder went quiet is taken over."""
        await queue.publish(b"one")
        first = await _take(queue, "consumer-1")
        await asyncio.sleep(PAST_IDLE)

        taken = await queue.reclaim("consumer-2", idle_seconds=IDLE)

        assert len(taken) == 1
        assert taken[0].body == b"one"
        assert taken[0].message_id == first.message_id
        assert taken[0].consumer == "consumer-2"
        assert taken[0].deliveries == first.deliveries + 1

    async def test_the_old_holder_is_locked_out_after_a_takeover(self, queue: ChunkQueue) -> None:
        """The old holder is locked out after a takeover."""
        await queue.publish(b"one")
        first = await _take(queue, "consumer-1")
        await asyncio.sleep(PAST_IDLE)
        (second,) = await queue.reclaim("consumer-2", idle_seconds=IDLE)

        with pytest.raises(DeliveryLost):
            await queue.touch(first)
        with pytest.raises(DeliveryLost):
            await queue.release(first)
        with pytest.raises(DeliveryLost):
            await queue.dead_letter(first, "too late")

        await queue.touch(second)  # the new holder is unaffected
        assert (await queue.stats()).in_flight == 1

    async def test_an_acknowledgement_from_the_old_holder_still_settles_the_message(
        self, queue: ChunkQueue
    ) -> None:
        """The old holder finished the work after all; doing it twice is harmless by contract."""
        await queue.publish(b"one")
        first = await _take(queue, "consumer-1")
        await asyncio.sleep(PAST_IDLE)
        await queue.reclaim("consumer-2", idle_seconds=IDLE)

        await queue.ack(first)

        stats = await queue.stats()
        assert (stats.waiting, stats.in_flight, stats.dead) == (0, 0, 0)

    async def test_a_late_acknowledgement_does_not_remove_a_message_that_was_handed_back(
        self, queue: ChunkQueue
    ) -> None:
        """Its holder said it was not finished; the message must still be there for the next one."""
        await queue.publish(b"one")
        first = await _take(queue, "consumer-1")
        await queue.release(first)

        await queue.ack(first)

        assert [d.body for d in await queue.receive("consumer-2")] == [b"one"]

    async def test_a_consumer_that_takes_over_its_own_message_locks_out_its_old_delivery(
        self, queue: ChunkQueue
    ) -> None:
        """A consumer that takes over its own message locks out its old delivery."""
        await queue.publish(b"one")
        first = await _take(queue, "consumer-1")
        await asyncio.sleep(PAST_IDLE)
        await queue.reclaim("consumer-1", idle_seconds=IDLE)

        with pytest.raises(DeliveryLost):
            await queue.touch(first)

    async def test_touching_restarts_the_idle_time(self, queue: ChunkQueue) -> None:
        """Touching restarts the idle time."""
        await queue.publish(b"one")
        delivery = await _take(queue)
        await asyncio.sleep(PAST_IDLE)

        await queue.touch(delivery)

        assert await queue.reclaim("consumer-2", idle_seconds=PAST_IDLE) == []

    async def test_a_message_that_keeps_coming_back_shows_how_often(
        self, queue: ChunkQueue
    ) -> None:
        """The consumer decides when to give up on it; the count is what it decides by."""
        await queue.publish(b"poison")
        await _take(queue, "consumer-1")
        for expected in (2, 3):
            await asyncio.sleep(PAST_IDLE)
            (again,) = await queue.reclaim("consumer-2", idle_seconds=IDLE)
            assert again.deliveries == expected

    # -- names ----------------------------------------------------------------------------------

    @pytest.mark.parametrize("name", ["", "has space", "a/b", "../x", "x" * 300])
    async def test_a_consumer_name_must_obey_the_key_rule(
        self, queue: ChunkQueue, name: str
    ) -> None:
        """A consumer name must obey the key rule."""
        with pytest.raises(InvalidKey):
            await queue.receive(name)
        with pytest.raises(InvalidKey):
            await queue.reclaim(name, idle_seconds=0.0)
