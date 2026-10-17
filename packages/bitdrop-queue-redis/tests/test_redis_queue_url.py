"""URL handling of the Redis queue: no server needed."""

import pytest

from bitdrop_queue_redis import DEFAULT_PREFIX, DEFAULT_QUEUE, RedisChunkQueue, create_queue


class TestNames:
    def test_the_stream_is_named_from_prefix_and_queue(self) -> None:
        queue = create_queue("redis://localhost:6379/0")

        assert isinstance(queue, RedisChunkQueue)
        assert queue.stream == f"{DEFAULT_PREFIX}queue:{DEFAULT_QUEUE}"

    def test_options_are_read(self) -> None:
        queue = create_queue("redis://h/0?bitdrop_prefix=app1:&bitdrop_queue=work:j1")

        assert queue.stream == "app1:queue:work:j1"

    def test_two_queues_of_one_server_have_different_streams(self) -> None:
        work = create_queue("redis://h/0?bitdrop_queue=work")
        files = create_queue("redis://h/0?bitdrop_queue=files")

        assert work.stream != files.stream


class TestOptions:
    @pytest.mark.parametrize("option", ["bitdrop_prefix", "bitdrop_queue"])
    @pytest.mark.parametrize("bad", ["", "has space", "star*", "q?", "br[ack]", "x" * 65])
    def test_names_must_be_safe_and_non_empty(self, option: str, bad: str) -> None:
        with pytest.raises(ValueError, match=option):
            create_queue(f"redis://h/0?{option}={bad}")

    def test_misspelled_options_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="bitdrop_quee"):
            create_queue("redis://h/0?bitdrop_quee=a")

    @pytest.mark.parametrize("url", ["postgresql://h/db", "http://h", "memory://", "h:6379"])
    def test_other_schemes_are_rejected(self, url: str) -> None:
        with pytest.raises(ValueError, match="redis"):
            create_queue(url)
