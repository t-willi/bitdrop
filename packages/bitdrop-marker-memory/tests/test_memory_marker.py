"""Behaviour specific to the in-memory marker store (the contract is covered by the kit)."""

import pytest

from bitdrop_marker_memory import MemoryMarkerStore, create_store


class TestKeptValues:
    async def test_a_mutable_buffer_handed_in_cannot_change_the_stored_value_later(self) -> None:
        store = MemoryMarkerStore()
        buffer = bytearray(b"abc")

        await store.set_if_absent("key", buffer)  # type: ignore[arg-type]
        buffer[0] = ord("X")

        assert await store.get("key") == b"abc"


class TestTheUrl:
    def test_memory_gives_an_empty_store(self) -> None:
        assert isinstance(create_store("memory://"), MemoryMarkerStore)

    @pytest.mark.parametrize("url", ["memory:///somewhere", "memory://?x=1", "file://"])
    def test_anything_else_is_refused(self, url: str) -> None:
        with pytest.raises(ValueError, match="memory"):
            create_store(url)
