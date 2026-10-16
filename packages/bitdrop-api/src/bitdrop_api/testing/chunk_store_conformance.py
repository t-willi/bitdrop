"""Conformance tests for the :class:`~bitdrop_api.chunk_store.ChunkStore` contract.

An adapter is *supported* if and only if it passes this kit (see ``docs/principles.md``, P1).

How to use it in an adapter package::

    class TestMyStoreConformance(ChunkStoreConformance):
        @pytest.fixture
        async def store(self) -> AsyncIterator[ChunkStore]:
            store = MyStore(...)
            yield store
            await store.aclose()

pytest collects the inherited ``test_*`` methods under your class. Override ``capped_store`` as well
if your backend can be limited to a small capacity; otherwise that one test is skipped.
"""

import asyncio
import random
from collections.abc import AsyncIterator
from hashlib import sha256

import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.errors import ChunkNotFound, ChunkTooLarge, InvalidKey, StoreFull

MIB = 1024 * 1024
LARGEST_VALUE_TESTED = 64 * MIB
"""Declared limits above this are not exercised, to keep test memory use sane."""


def make_payload(size: int, seed: int = 0) -> bytes:
    """Return ``size`` deterministic pseudo-random bytes (so failures are reproducible)."""
    return random.Random(seed).randbytes(size)


def _assert_same_bytes(actual: bytes, expected: bytes) -> None:
    """Compare by length and digest so a failure prints a readable message, not megabytes."""
    assert len(actual) == len(expected), f"length differs: {len(actual)} != {len(expected)}"
    assert sha256(actual).digest() == sha256(expected).digest(), "content differs"


class ChunkStoreConformance:
    """Base class of the contract tests. Subclass it and override the ``store`` fixture."""

    # -- fixtures an adapter must (or may) override --------------------------------------------

    @pytest.fixture
    async def store(self) -> AsyncIterator[ChunkStore]:
        """Yield a fresh, empty store. Required: override this in your subclass."""
        raise NotImplementedError("override the `store` fixture in your Test subclass")
        yield  # pragma: no cover  # noqa: PT022  (makes this an async generator like overrides)

    @pytest.fixture
    async def capped_store(self) -> AsyncIterator[ChunkStore]:
        """Yield a fresh store holding well under 64 MiB, that still accepts 1 MiB values.

        Optional: without an override the capacity test is skipped.
        """
        pytest.skip("adapter provides no capacity-limited store")
        yield  # pragma: no cover  # noqa: PT022

    # -- protocol and capabilities --------------------------------------------------------------

    async def test_store_satisfies_the_protocol(self, store: ChunkStore) -> None:
        """The object structurally implements every method of ``ChunkStore``."""
        assert isinstance(store, ChunkStore)

    async def test_capabilities_and_stats_agree(self, store: ChunkStore) -> None:
        """``stats()`` returns data exactly when ``memory_stats`` is declared."""
        stats = await store.stats()

        if store.capabilities().memory_stats:
            assert stats is not None, "memory_stats=True but stats() returned None"
        else:
            assert stats is None, "memory_stats=False but stats() returned data"

    async def test_stats_grow_by_at_least_the_stored_bytes(self, store: ChunkStore) -> None:
        """Reported usage grows by at least the size of what was stored (overhead is allowed)."""
        if not store.capabilities().memory_stats:
            pytest.skip("store declares no memory stats")
        limit = store.capabilities().max_value_bytes
        size = MIB if limit is None else min(MIB, limit)
        before = await store.stats()
        await store.put("stats-probe", make_payload(size))
        after = await store.stats()

        assert before is not None
        assert after is not None
        assert after.used_bytes - before.used_bytes >= size

    # -- basic semantics ------------------------------------------------------------------------

    @pytest.mark.parametrize("size", [0, 1, 1024, MIB, 16 * MIB])
    async def test_value_roundtrips_exactly(self, store: ChunkStore, size: int) -> None:
        """What goes in comes out byte for byte, from empty up to 16 MiB."""
        limit = store.capabilities().max_value_bytes
        if limit is not None and size > limit:
            pytest.skip(f"{size} bytes exceeds the declared limit of {limit}")
        payload = make_payload(size, seed=size)

        await store.put("roundtrip", payload)

        _assert_same_bytes(await store.get("roundtrip"), payload)

    async def test_put_overwrites_the_previous_value(self, store: ChunkStore) -> None:
        """A second ``put`` replaces the first completely, even with a different length."""
        await store.put("overwrite", b"first")
        await store.put("overwrite", b"second, and longer")

        assert await store.get("overwrite") == b"second, and longer"

    async def test_get_of_an_unknown_key_raises_chunk_not_found(self, store: ChunkStore) -> None:
        """Absence is reported as ``ChunkNotFound`` carrying the key, not as empty data."""
        with pytest.raises(ChunkNotFound) as excinfo:
            await store.get("never-written")

        assert excinfo.value.key == "never-written"

    async def test_delete_removes_the_value(self, store: ChunkStore) -> None:
        """After ``delete`` the key is gone."""
        await store.put("doomed", b"data")

        await store.delete("doomed")

        with pytest.raises(ChunkNotFound):
            await store.get("doomed")

    async def test_delete_of_an_unknown_key_is_a_no_op(self, store: ChunkStore) -> None:
        """``delete`` is idempotent: deleting nothing, even twice, is not an error."""
        await store.delete("never-written")
        await store.delete("never-written")

    async def test_keys_do_not_interfere(self, store: ChunkStore) -> None:
        """Operations on one key leave other keys alone."""
        await store.put("job:1:chunk:1", b"one")
        await store.put("job:1:chunk:2", b"two")
        await store.delete("job:1:chunk:1")

        assert await store.get("job:1:chunk:2") == b"two"

    @pytest.mark.parametrize(
        "bad_key",
        [
            pytest.param("", id="empty"),
            pytest.param("a/b", id="slash"),
            pytest.param(".hidden", id="leading-dot"),
            pytest.param("has space", id="space"),
            pytest.param("x" * 201, id="too-long"),
        ],
    )
    async def test_every_operation_rejects_invalid_keys(
        self, store: ChunkStore, bad_key: str
    ) -> None:
        """``put``, ``get`` and ``delete`` all raise ``InvalidKey`` for keys breaking the rules."""
        with pytest.raises(InvalidKey):
            await store.put(bad_key, b"x")
        with pytest.raises(InvalidKey):
            await store.get(bad_key)
        with pytest.raises(InvalidKey):
            await store.delete(bad_key)

    # -- declared value limit -------------------------------------------------------------------

    async def test_value_at_the_declared_limit_is_accepted(self, store: ChunkStore) -> None:
        """A value of exactly ``max_value_bytes`` is stored (skipped if no limit is declared)."""
        limit = store.capabilities().max_value_bytes
        if limit is None or limit > LARGEST_VALUE_TESTED:
            pytest.skip("no testable value limit declared")
        payload = make_payload(limit, seed=1)

        await store.put("at-limit", payload)

        _assert_same_bytes(await store.get("at-limit"), payload)

    async def test_value_over_the_declared_limit_is_rejected_cleanly(
        self, store: ChunkStore
    ) -> None:
        """One byte over the limit raises ``ChunkTooLarge`` and stores nothing."""
        limit = store.capabilities().max_value_bytes
        if limit is None or limit > LARGEST_VALUE_TESTED:
            pytest.skip("no testable value limit declared")

        with pytest.raises(ChunkTooLarge):
            await store.put("over-limit", make_payload(limit + 1))

        with pytest.raises(ChunkNotFound):
            await store.get("over-limit")

    # -- concurrency ----------------------------------------------------------------------------

    async def test_many_concurrent_writers_and_readers(self, store: ChunkStore) -> None:
        """32 chunks written at once, then read at once, all come back intact."""
        payloads = {f"concurrent-{i}": make_payload(256 * 1024, seed=i) for i in range(32)}

        await asyncio.gather(*(store.put(key, value) for key, value in payloads.items()))
        results = await asyncio.gather(*(store.get(key) for key in payloads))

        for expected, actual in zip(payloads.values(), results, strict=True):
            _assert_same_bytes(actual, expected)

    async def test_concurrent_overwrites_leave_exactly_one_complete_value(
        self, store: ChunkStore
    ) -> None:
        """Racing writers to one key leave one of their values whole, never a blend."""
        candidates = [make_payload(512 * 1024, seed=i) for i in range(8)]

        await asyncio.gather(*(store.put("contended", value) for value in candidates))

        final = await store.get("contended")
        assert sha256(final).digest() in {sha256(value).digest() for value in candidates}

    async def test_readers_never_observe_a_partially_written_value(self, store: ChunkStore) -> None:
        """While a writer flips a key between two values, readers only ever see one of them."""
        # A race can only be provoked, not proven absent: many readers hammer a large value while
        # a writer flips it. A non-atomic implementation shows a torn value in nearly every run.
        limit = store.capabilities().max_value_bytes
        size = 2 * MIB if limit is None else min(2 * MIB, limit)
        old, new = make_payload(size, seed=1), make_payload(size, seed=2)
        allowed = {sha256(old).digest(), sha256(new).digest()}
        await store.put("atomic", old)

        async def writer() -> None:
            for _ in range(30):
                await store.put("atomic", new)
                await asyncio.sleep(0)
                await store.put("atomic", old)
                await asyncio.sleep(0)

        async def reader() -> set[bytes]:
            digests: set[bytes] = set()
            for _ in range(50):
                digests.add(sha256(await store.get("atomic")).digest())
                await asyncio.sleep(0)
            return digests

        writing = asyncio.create_task(writer())
        observed = await asyncio.gather(*(reader() for _ in range(4)))
        await writing

        for digests in observed:
            assert digests <= allowed, "a reader saw a value that was never written"

    # -- capacity -------------------------------------------------------------------------------

    async def test_a_full_store_fails_loudly_or_forgets_but_never_corrupts(
        self, capped_store: ChunkStore
    ) -> None:
        """Past capacity ``put`` raises ``StoreFull`` or old keys vanish; data is never wrong."""
        stored: dict[str, bytes] = {}
        capacity_reached = False

        for i in range(64):  # 64 MiB in total: more than a capped store may hold
            key, payload = f"fill-{i:03d}", make_payload(MIB, seed=i)
            try:
                await capped_store.put(key, payload)
            except StoreFull:
                capacity_reached = True
                continue
            stored[key] = payload

        for key, expected in stored.items():
            try:
                actual = await capped_store.get(key)
            except ChunkNotFound:
                capacity_reached = True  # evicted: acceptable, callers re-extract
                continue
            _assert_same_bytes(actual, expected)

        assert capacity_reached, (
            "capacity was never exceeded: the capped_store fixture must hold less than 64 MiB"
        )
