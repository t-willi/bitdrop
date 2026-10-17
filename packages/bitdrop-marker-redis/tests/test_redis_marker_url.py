"""URL handling of the Redis marker store, and the keys it writes: no server needed."""

import fakeredis
import pytest
from fakeredis import aioredis as fake_aioredis

from bitdrop_marker_redis import DEFAULT_PREFIX, RedisMarkerStore, create_store


class TestOptions:
    def test_the_default_prefix(self) -> None:
        assert create_store("redis://localhost:6379/0").prefix == DEFAULT_PREFIX

    def test_options_are_read(self) -> None:
        assert (
            create_store("redis://h/0?bitdrop_prefix=app1:&bitdrop_max_connections=3").prefix
            == "app1:"
        )

    @pytest.mark.parametrize("bad", ["", "has space", "star*", "q?", "br[ack]", "x" * 65])
    def test_prefixes_must_be_safe_and_non_empty(self, bad: str) -> None:
        with pytest.raises(ValueError, match="bitdrop_prefix"):
            create_store(f"redis://h/0?bitdrop_prefix={bad}")

    def test_misspelled_options_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="bitdrop_prefx"):
            create_store("redis://h/0?bitdrop_prefx=a:")

    @pytest.mark.parametrize("url", ["postgresql://h/db", "http://h", "memory://", "h:6379"])
    def test_other_schemes_are_rejected(self, url: str) -> None:
        with pytest.raises(ValueError, match="redis"):
            create_store(url)


class TestKeys:
    async def test_a_marker_is_one_string_under_prefix_marker_key(self) -> None:
        client = fake_aioredis.FakeRedis(server=fakeredis.FakeServer())
        store = RedisMarkerStore("redis://fake/0?bitdrop_prefix=app1:", client=client)

        await store.set_if_absent("job-1:settings", b"abc")

        assert await client.get("app1:marker:job-1:settings") == b"abc"
        await store.aclose()
