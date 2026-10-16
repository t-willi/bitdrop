"""Behaviour specific to the in-memory adapter (the shared contract is covered by the kit)."""

import pytest

from bitdrop_api.chunk_store import ChunkStore, Locality, StoreCapabilities, StoreStats
from bitdrop_api.errors import ChunkNotFound, StoreFull
from bitdrop_store_memory import MemoryChunkStore, create_store


class TestAccounting:
    async def test_used_bytes_follow_puts_overwrites_and_deletes(self) -> None:
        store = MemoryChunkStore()

        await store.put("a", b"12345")
        await store.put("b", b"123")
        assert await store.stats() == StoreStats(used_bytes=8)

        await store.put("a", b"12")  # overwrite with a smaller value
        assert await store.stats() == StoreStats(used_bytes=5)

        await store.delete("b")
        assert await store.stats() == StoreStats(used_bytes=2)

    async def test_stats_report_the_configured_capacity(self) -> None:
        store = MemoryChunkStore(max_bytes=100)

        assert await store.stats() == StoreStats(used_bytes=0, max_bytes=100)


class TestCapacity:
    async def test_put_beyond_capacity_raises_store_full(self) -> None:
        store = MemoryChunkStore(max_bytes=10)
        await store.put("a", b"123456")

        with pytest.raises(StoreFull):
            await store.put("b", b"12345")

    async def test_failed_put_leaves_everything_unchanged(self) -> None:
        store = MemoryChunkStore(max_bytes=10)
        await store.put("a", b"123456")

        with pytest.raises(StoreFull):
            await store.put("b", b"12345")

        assert await store.get("a") == b"123456"
        with pytest.raises(ChunkNotFound):
            await store.get("b")
        assert await store.stats() == StoreStats(used_bytes=6, max_bytes=10)

    async def test_overwrite_is_checked_against_the_net_growth_only(self) -> None:
        store = MemoryChunkStore(max_bytes=10)
        await store.put("a", b"123456789")

        await store.put("a", b"ABCDEFGHIJ")  # 10 bytes replacing 9: fits exactly

        assert await store.get("a") == b"ABCDEFGHIJ"

    async def test_overwrite_that_does_not_fit_keeps_the_old_value(self) -> None:
        store = MemoryChunkStore(max_bytes=10)
        await store.put("a", b"12345")

        with pytest.raises(StoreFull):
            await store.put("a", b"12345678901")

        assert await store.get("a") == b"12345"


class TestCapabilities:
    def test_defaults(self) -> None:
        assert MemoryChunkStore().capabilities() == StoreCapabilities(
            max_value_bytes=None, locality=Locality.NODE, memory_stats=True
        )

    def test_value_limit_is_declared(self) -> None:
        assert MemoryChunkStore(max_value_bytes=7).capabilities().max_value_bytes == 7


class TestFactory:
    """`create_store` is what the plugin registry calls with the URL from the configuration."""

    async def test_bare_url_gives_an_unbounded_store(self) -> None:
        store = create_store("memory://")

        assert isinstance(store, ChunkStore)
        assert await store.stats() == StoreStats(used_bytes=0, max_bytes=None)

    async def test_query_parameters_configure_the_store(self) -> None:
        store = create_store("memory://?max_bytes=1000&max_value_bytes=100")

        assert await store.stats() == StoreStats(used_bytes=0, max_bytes=1000)
        assert store.capabilities().max_value_bytes == 100

    @pytest.mark.parametrize(
        "url",
        [
            pytest.param("redis://", id="wrong-scheme"),
            pytest.param("memory://somehost", id="host-not-allowed"),
            pytest.param("memory://?max_bytes=lots", id="not-an-integer"),
            pytest.param("memory://?colour=blue", id="unknown-parameter"),
        ],
    )
    def test_bad_urls_are_rejected_with_a_clear_error(self, url: str) -> None:
        with pytest.raises(ValueError, match="memory"):
            create_store(url)
