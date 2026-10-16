"""Redis-protocol specifics: namespacing, memory limits, errors, auth (needs Docker)."""

import asyncio
from typing import TYPE_CHECKING

import pytest

from bitdrop_api.errors import ChunkNotFound, ChunkStoreError, StoreFull
from bitdrop_api.testing import MIB, make_payload
from bitdrop_store_redis import RedisChunkStore

if TYPE_CHECKING:
    from conftest import KvServer, KvServers

pytestmark = pytest.mark.docker


class TestNamespacing:
    async def test_keys_live_under_the_prefix(self, kv: KvServer) -> None:
        store = RedisChunkStore(kv.url)

        await store.put("job-1:c000001", b"data")

        assert kv.client().keys("*") == [b"bitdrop:job-1:c000001"]
        await store.aclose()

    async def test_two_prefixes_share_a_server_without_seeing_each_other(
        self, kv: KvServer
    ) -> None:
        first = RedisChunkStore(f"{kv.url}?bitdrop_prefix=app1:")
        second = RedisChunkStore(f"{kv.url}?bitdrop_prefix=app2:")

        await first.put("k", b"one")
        await second.put("k", b"two")

        assert await first.get("k") == b"one"
        assert await second.get("k") == b"two"
        await first.delete("k")
        assert await second.get("k") == b"two"
        await first.aclose()
        await second.aclose()

    async def test_foreign_keys_on_the_server_are_left_alone(self, kv: KvServer) -> None:
        kv.client().set("someone-elses-key", "precious")
        store = RedisChunkStore(kv.url)

        await store.put("someone-elses-key", b"mine")
        await store.delete("someone-elses-key")

        assert kv.client().get("someone-elses-key") == b"precious"
        await store.aclose()

    async def test_the_database_number_in_the_url_is_honoured(self, kv: KvServer) -> None:
        store = RedisChunkStore(kv.url.removesuffix("/0") + "/1")

        await store.put("k", b"v")

        import redis

        db0 = redis.Redis(host=kv.host, port=kv.port, db=0)
        db1 = redis.Redis(host=kv.host, port=kv.port, db=1)
        assert db0.dbsize() == 0
        assert db1.dbsize() == 1
        db1.flushdb()
        await store.aclose()


class TestValues:
    async def test_every_byte_value_and_protocol_lookalikes_survive(self, kv: KvServer) -> None:
        nasty = bytes(range(256)) * 4 + b"\r\n$5\r\nhello\r\n*1\r\n+OK\r\n" + b"\x00" * 10
        store = RedisChunkStore(kv.url)

        await store.put("nasty", nasty)

        assert await store.get("nasty") == nasty
        await store.aclose()

    async def test_a_64_mib_chunk_roundtrips(self, kv: KvServer) -> None:
        payload = make_payload(64 * MIB, seed=7)
        store = RedisChunkStore(kv.url)

        await store.put("big", payload)

        assert await store.get("big") == payload
        await store.aclose()

    async def test_delete_frees_the_memory(self, kv: KvServer) -> None:
        store = RedisChunkStore(kv.url)
        await store.put("big", make_payload(8 * MIB))
        before = (await store.stats()).used_bytes

        await store.delete("big")
        await asyncio.sleep(0.2)  # UNLINK frees large values in the background

        after = (await store.stats()).used_bytes
        assert after < before - 4 * MIB
        await store.aclose()


class TestConcurrency:
    async def test_a_small_connection_pool_queues_instead_of_failing(self, kv: KvServer) -> None:
        store = RedisChunkStore(f"{kv.url}?bitdrop_max_connections=2")
        payloads = {f"k{i}": make_payload(256 * 1024, seed=i) for i in range(20)}

        await asyncio.gather(*(store.put(key, value) for key, value in payloads.items()))
        results = await asyncio.gather(*(store.get(key) for key in payloads))

        assert results == list(payloads.values())
        await store.aclose()


class TestMemoryLimits:
    async def test_a_refused_write_leaves_the_previous_value_intact(self, kv: KvServer) -> None:
        client = kv.client()
        original = client.config_get("maxmemory")["maxmemory"]
        store = RedisChunkStore(kv.url)
        await store.put("k", b"old value")
        client.config_set("maxmemory", "8mb")
        try:
            with pytest.raises(StoreFull):
                await store.put("k", make_payload(16 * MIB))
            assert await store.get("k") == b"old value"
        finally:
            client.config_set("maxmemory", original)
            await store.aclose()

    async def test_stats_report_the_servers_configured_limit(self, kv: KvServer) -> None:
        client = kv.client()
        original = client.config_get("maxmemory")["maxmemory"]
        client.config_set("maxmemory", str(100 * MIB))
        store = RedisChunkStore(kv.url)
        try:
            stats = await store.stats()
            assert stats is not None
            assert stats.max_bytes == 100 * MIB
            assert 0 < stats.used_bytes < 100 * MIB
        finally:
            client.config_set("maxmemory", original)
            await store.aclose()

    async def test_a_server_without_a_limit_reports_no_maximum(self, kv: KvServer) -> None:
        client = kv.client()
        original = client.config_get("maxmemory")["maxmemory"]
        client.config_set("maxmemory", "0")
        store = RedisChunkStore(kv.url)
        try:
            stats = await store.stats()
            assert stats is not None
            if kv.flavor == "dragonfly":
                pytest.skip("Dragonfly derives its own limit when none is set")
            assert stats.max_bytes is None
        finally:
            client.config_set("maxmemory", original)
            await store.aclose()

    async def test_an_evicting_server_forgets_but_never_corrupts(self, kv: KvServer) -> None:
        """With an eviction policy the server drops old keys instead of refusing writes."""
        client = kv.client()
        original = client.config_get("maxmemory")["maxmemory"]
        try:
            client.config_set("maxmemory-policy", "allkeys-lru")
        except Exception:
            pytest.skip(f"{kv.flavor} has no runtime eviction policy")
        client.config_set("maxmemory", "8mb")
        store = RedisChunkStore(kv.url)
        stored = {f"k{i:02d}": make_payload(MIB, seed=i) for i in range(40)}
        try:
            for key, value in stored.items():
                await store.put(key, value)  # never StoreFull: the server makes room
            missing = 0
            for key, value in stored.items():
                try:
                    assert await store.get(key) == value
                except ChunkNotFound:
                    missing += 1
            assert missing > 0, "nothing was evicted: the test proves nothing"
        finally:
            client.config_set("maxmemory-policy", "noeviction")
            client.config_set("maxmemory", original)
            await store.aclose()


class TestConnectionProblems:
    async def test_an_unreachable_server_is_a_chunk_store_error_without_the_password(self) -> None:
        store = RedisChunkStore("redis://:hunter2@127.0.0.1:1/0")

        for operation in (
            store.put("k", b"v"),
            store.get("k"),
            store.delete("k"),
            store.stats(),
        ):
            with pytest.raises(ChunkStoreError) as excinfo:
                await operation
            assert "hunter2" not in str(excinfo.value)
        await store.aclose()

    async def test_passwords_are_sent_and_wrong_ones_are_refused(
        self, kv_servers: KvServers
    ) -> None:
        server = kv_servers.get("redis", password="s3cret-pw")

        good = RedisChunkStore(server.url)
        await good.put("k", b"v")
        assert await good.get("k") == b"v"
        await good.aclose()

        for url in (
            server.url.replace("s3cret-pw", "bad-pw-xyz"),
            server.url.replace(":s3cret-pw@", ""),
        ):
            bad = RedisChunkStore(url)
            with pytest.raises(ChunkStoreError) as excinfo:
                await bad.put("k", b"v")
            assert "s3cret-pw" not in str(excinfo.value)
            assert "bad-pw-xyz" not in str(excinfo.value)
            await bad.aclose()
