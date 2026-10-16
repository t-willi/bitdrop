"""URL handling of the Redis chunk store: no server needed."""

import pytest

from bitdrop_store_redis import (
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_VALUE_BYTES,
    DEFAULT_PREFIX,
    RedisChunkStore,
    create_store,
)


class TestDefaults:
    def test_a_plain_url_gives_a_ready_but_unconnected_store(self) -> None:
        store = create_store("redis://localhost:6379/0")

        assert isinstance(store, RedisChunkStore)
        assert store.prefix == DEFAULT_PREFIX
        assert store.capabilities().max_value_bytes == DEFAULT_MAX_VALUE_BYTES
        assert store.max_connections == DEFAULT_MAX_CONNECTIONS

    def test_it_is_a_network_store_with_memory_stats(self) -> None:
        capabilities = create_store("redis://localhost/0").capabilities()

        assert capabilities.locality.value == "cluster"
        assert capabilities.memory_stats is True


class TestOptions:
    def test_options_are_read(self) -> None:
        store = create_store(
            "redis://h/0?bitdrop_prefix=app1:&bitdrop_max_value_bytes=1048576&bitdrop_max_connections=3"
        )

        assert store.prefix == "app1:"
        assert store.capabilities().max_value_bytes == 1_048_576
        assert store.max_connections == 3

    @pytest.mark.parametrize(
        "prefix", ["", "has space", "star*", "q?", "br[ack]", "new%0Aline", "x" * 65]
    )
    def test_prefixes_must_be_safe_and_non_empty(self, prefix: str) -> None:
        with pytest.raises(ValueError, match="bitdrop_prefix"):
            create_store(f"redis://h/0?bitdrop_prefix={prefix}")

    @pytest.mark.parametrize("option", ["bitdrop_max_value_bytes", "bitdrop_max_connections"])
    @pytest.mark.parametrize("bad", ["0", "-5", "lots"])
    def test_numeric_options_must_be_positive_integers(self, option: str, bad: str) -> None:
        with pytest.raises(ValueError, match=option):
            create_store(f"redis://h/0?{option}={bad}")

    def test_misspelled_options_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="bitdrop_prefx"):
            create_store("redis://h/0?bitdrop_prefx=a:")

    @pytest.mark.parametrize("url", ["postgresql://h/db", "http://h", "memory://", "h:6379"])
    def test_other_schemes_are_rejected(self, url: str) -> None:
        with pytest.raises(ValueError, match="redis"):
            create_store(url)

    def test_client_options_pass_through_to_the_client(self) -> None:
        store = create_store("redis://h/0?socket_timeout=3")

        assert store.socket_timeout == 3
