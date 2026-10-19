"""Conformance tests for the :class:`~bitdrop_api.marker_store.MarkerStore` contract.

An adapter is *supported* if and only if it passes this kit (see ``docs/principles.md``, P1).

How to use it in an adapter package::

    class TestMyMarkerStoreConformance(MarkerStoreConformance):
        @pytest.fixture
        async def store(self) -> AsyncIterator[MarkerStore]:
            store = MyStore(...)       # empty, and independent of the other tests' stores
            yield store
            await store.aclose()
"""

import asyncio
from collections.abc import AsyncIterator

import pytest

from bitdrop_api.errors import InvalidKey
from bitdrop_api.marker_store import MarkerStore

KEY = "job-1:settings"

VALUES = [
    b"",
    b"\x00",
    b"\x00middle\x00",
    b'{"table":"people","chunk_bytes":1048576}',
    bytes(range(256)),
    b"\xff" * 100_000 + b"\x00",
    "ümläut ✓".encode(),
]
"""Values that a careless adapter would change: empty, NULs at both ends, every byte, 100 kB."""


class MarkerStoreConformance:
    """Base class of the contract tests. Subclass it and override the ``store`` fixture."""

    @pytest.fixture
    async def store(self) -> AsyncIterator[MarkerStore]:
        """Yield an empty store, independent of every other test's."""
        raise NotImplementedError("override the `store` fixture in your Test subclass")
        yield  # pragma: no cover  # noqa: PT022

    async def test_store_satisfies_the_protocol(self, store: MarkerStore) -> None:
        """The object structurally implements every method of ``MarkerStore``."""
        assert isinstance(store, MarkerStore)

    async def test_a_key_nobody_set_is_absent(self, store: MarkerStore) -> None:
        """Reading a key that was never set gives None."""
        assert await store.get(KEY) is None

    async def test_the_first_writer_stores_its_value_and_is_told_so(
        self, store: MarkerStore
    ) -> None:
        """The winner's result is its own value, and everyone reads it back."""
        assert await store.set_if_absent(KEY, b"first") == b"first"
        assert await store.get(KEY) == b"first"

    async def test_a_later_writer_changes_nothing_and_learns_the_first_value(
        self, store: MarkerStore
    ) -> None:
        """First writer wins: the second is answered with what the first wrote."""
        await store.set_if_absent(KEY, b"first")

        assert await store.set_if_absent(KEY, b"second") == b"first"
        assert await store.get(KEY) == b"first"

    async def test_setting_the_same_value_again_is_harmless(self, store: MarkerStore) -> None:
        """A planner that runs twice with the same settings finds them unchanged."""
        await store.set_if_absent(KEY, b"same")

        assert await store.set_if_absent(KEY, b"same") == b"same"

    @pytest.mark.parametrize("value", VALUES, ids=range(len(VALUES)))
    async def test_values_are_kept_byte_for_byte(self, store: MarkerStore, value: bytes) -> None:
        """Nothing is trimmed, re-encoded, or turned into text."""
        assert await store.set_if_absent(KEY, value) == value
        assert await store.get(KEY) == value

    async def test_an_empty_value_is_a_value_not_an_absence(self, store: MarkerStore) -> None:
        """b"" is stored, it blocks later writers, and it is not None."""
        await store.set_if_absent(KEY, b"")

        assert await store.get(KEY) == b""
        assert await store.set_if_absent(KEY, b"later") == b""

    async def test_keys_are_independent(self, store: MarkerStore) -> None:
        """Setting one key leaves the others alone."""
        await store.set_if_absent("job-1:settings", b"one")
        await store.set_if_absent("job-2:settings", b"two")

        assert await store.get("job-1:settings") == b"one"
        assert await store.get("job-2:settings") == b"two"
        assert await store.get("job-1:done") is None

    async def test_of_many_writers_at_once_exactly_one_wins_and_all_are_told_its_value(
        self, store: MarkerStore
    ) -> None:
        """The atomic step: two planners started together end with one set of settings."""
        candidates = [f"planner-{i}".encode() for i in range(30)]

        answers = await asyncio.gather(*(store.set_if_absent(KEY, value) for value in candidates))

        winner = await store.get(KEY)
        assert winner in candidates
        assert set(answers) == {winner}, "writers were told different values"

    @pytest.mark.parametrize(
        "key",
        ["", "has space", "a/b", "../x", "x" * 300, ".hidden"],
        ids=["empty", "space", "slash", "dotdot", "too-long", "hidden"],
    )
    async def test_a_key_must_obey_the_key_rule(self, store: MarkerStore, key: str) -> None:
        """Keys end up in Redis keys and file names, so they follow the one shared rule."""
        with pytest.raises(InvalidKey):
            await store.set_if_absent(key, b"x")
        with pytest.raises(InvalidKey):
            await store.get(key)
