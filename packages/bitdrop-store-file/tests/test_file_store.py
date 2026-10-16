"""Behaviour specific to the filesystem adapter (the shared contract is covered by the kit)."""

import errno
from pathlib import Path

import pytest

from bitdrop_api.chunk_store import ChunkStore, Locality, StoreCapabilities
from bitdrop_api.errors import ChunkNotFound, StoreFull
from bitdrop_store_file import FileChunkStore, create_store


class TestLayoutOnDisk:
    async def test_each_chunk_is_one_file_named_after_its_key(self, tmp_path: Path) -> None:
        store = FileChunkStore(tmp_path)

        await store.put("job:1:chunk:7", b"payload")

        assert (tmp_path / "job:1:chunk:7").read_bytes() == b"payload"

    async def test_missing_directories_are_created(self, tmp_path: Path) -> None:
        root = tmp_path / "does" / "not" / "exist"

        FileChunkStore(root)

        assert root.is_dir()

    async def test_delete_removes_the_file(self, tmp_path: Path) -> None:
        store = FileChunkStore(tmp_path)
        await store.put("gone", b"x")

        await store.delete("gone")

        assert list(tmp_path.iterdir()) == []

    async def test_no_temporary_files_are_left_behind(self, tmp_path: Path) -> None:
        store = FileChunkStore(tmp_path)

        await store.put("a", b"1")
        await store.put("a", b"2")

        assert [path.name for path in tmp_path.iterdir()] == ["a"]


class TestSharingBetweenInstances:
    """The point of this adapter: separate processes/containers share one directory."""

    async def test_a_second_instance_sees_what_the_first_wrote(self, tmp_path: Path) -> None:
        writer, reader = FileChunkStore(tmp_path), FileChunkStore(tmp_path)

        await writer.put("shared", b"hello")

        assert await reader.get("shared") == b"hello"


class TestDiskFull:
    async def test_out_of_space_becomes_store_full_and_keeps_the_old_value(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = FileChunkStore(tmp_path)
        await store.put("a", b"old")

        def disk_full(self: Path, data: bytes) -> int:
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(Path, "write_bytes", disk_full)

        with pytest.raises(StoreFull):
            await store.put("a", b"new")

        monkeypatch.undo()
        assert await store.get("a") == b"old"
        assert [path.name for path in tmp_path.iterdir()] == ["a"]  # no temp file left

    async def test_other_os_errors_are_not_disguised(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = FileChunkStore(tmp_path)

        def broken(self: Path, data: bytes) -> int:
            raise OSError(errno.EIO, "I/O error")

        monkeypatch.setattr(Path, "write_bytes", broken)

        with pytest.raises(OSError, match="I/O error"):
            await store.put("a", b"x")


class TestCapabilities:
    def test_defaults(self, tmp_path: Path) -> None:
        assert FileChunkStore(tmp_path).capabilities() == StoreCapabilities(
            max_value_bytes=None, locality=Locality.NODE, memory_stats=False
        )

    async def test_stats_are_not_provided(self, tmp_path: Path) -> None:
        assert await FileChunkStore(tmp_path).stats() is None

    def test_locality_can_be_declared_by_the_deployment(self, tmp_path: Path) -> None:
        store = FileChunkStore(tmp_path, locality=Locality.POD)

        assert store.capabilities().locality is Locality.POD


class TestFactory:
    """`create_store` is what the plugin registry calls with the URL from the configuration."""

    async def test_the_path_selects_the_directory(self, tmp_path: Path) -> None:
        store = create_store(f"file://{tmp_path}/chunks")

        assert isinstance(store, ChunkStore)
        await store.put("k", b"v")
        assert (tmp_path / "chunks" / "k").read_bytes() == b"v"

    async def test_query_parameters_configure_the_store(self, tmp_path: Path) -> None:
        store = create_store(f"file://{tmp_path}?max_value_bytes=100&locality=pod")

        assert store.capabilities().max_value_bytes == 100
        assert store.capabilities().locality is Locality.POD

    @pytest.mark.parametrize(
        "url",
        [
            pytest.param("memory://", id="wrong-scheme"),
            pytest.param("file://", id="path-is-required"),
            pytest.param("file://somehost/tmp/x", id="host-not-allowed"),
            pytest.param("file:///tmp/x?locality=moon", id="unknown-locality"),
            pytest.param("file:///tmp/x?max_value_bytes=big", id="not-an-integer"),
            pytest.param("file:///tmp/x?colour=blue", id="unknown-parameter"),
        ],
    )
    def test_bad_urls_are_rejected_with_a_clear_error(self, url: str) -> None:
        with pytest.raises(ValueError, match="file"):
            create_store(url)

    async def test_get_of_a_missing_key_still_raises_chunk_not_found(self, tmp_path: Path) -> None:
        store = create_store(f"file://{tmp_path}")

        with pytest.raises(ChunkNotFound):
            await store.get("absent")
