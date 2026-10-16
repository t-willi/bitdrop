"""Behaviour specific to the SQLite adapter (the shared contract is covered by the kit)."""

import asyncio
import sqlite3
from pathlib import Path

import pytest

from bitdrop_api.metadata_store import ChunkState, MetadataStore, Stage
from bitdrop_metadata_sqlite import SqliteMetadataStore, create_store


class FakeClock:
    """A clock the test moves by hand, so lease timing needs no sleeping."""

    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestPersistence:
    """The reason to use SQLite: a restarted master picks up where the job stopped."""

    async def test_state_survives_reopening_the_database(self, tmp_path: Path) -> None:
        path = tmp_path / "metadata.db"
        first = SqliteMetadataStore(path)
        await first.register_chunks("job", ["a", "b"], max_attempts=5)
        await first.claim("job", Stage.EXTRACT, worker_id="w", lease_seconds=30)
        await first.aclose()

        second = SqliteMetadataStore(path)

        counts = await second.counts("job")
        assert (counts[ChunkState.PENDING], counts[ChunkState.EXTRACTING]) == (1, 1)
        await second.aclose()

    async def test_a_claim_taken_before_a_restart_is_still_valid_after_it(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "metadata.db"
        first = SqliteMetadataStore(path)
        await first.register_chunks("job", ["a"])
        claim = await first.claim("job", Stage.EXTRACT, worker_id="w", lease_seconds=30)
        assert claim is not None
        await first.aclose()

        second = SqliteMetadataStore(path)
        await second.complete(claim)

        assert (await second.get_chunk("job", "a")).state is ChunkState.EXTRACTED
        await second.aclose()

    async def test_a_saved_plan_survives_reopening_the_database(self, tmp_path: Path) -> None:
        path = tmp_path / "metadata.db"
        first = SqliteMetadataStore(path)
        await first.save_plan("job", b"the plan")
        await first.aclose()

        second = SqliteMetadataStore(path)

        assert await second.load_plan("job") == b"the plan"
        assert await second.save_plan("job", b"another plan") == b"the plan"
        await second.aclose()

    async def test_a_database_made_before_plans_existed_gains_the_table(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "metadata.db"
        with sqlite3.connect(path) as connection:
            connection.executescript(
                "CREATE TABLE chunks (job_id TEXT NOT NULL, chunk_id TEXT NOT NULL,"
                " state TEXT NOT NULL, max_attempts INTEGER NOT NULL,"
                " failures INTEGER NOT NULL DEFAULT 0, fence INTEGER NOT NULL DEFAULT 0,"
                " worker_id TEXT, lease_expires REAL, last_error TEXT,"
                " PRIMARY KEY (job_id, chunk_id));"
                "INSERT INTO chunks (job_id, chunk_id, state, max_attempts)"
                " VALUES ('job', 'a', 'pending', 3);"
                "PRAGMA user_version = 1;"
            )
        connection.close()

        store = SqliteMetadataStore(path)

        assert await store.load_plan("job") is None
        assert await store.save_plan("job", b"plan") == b"plan"
        assert (await store.counts("job"))[ChunkState.PENDING] == 1
        await store.aclose()

    async def test_missing_parent_directories_are_created(self, tmp_path: Path) -> None:
        path = tmp_path / "does" / "not" / "exist" / "metadata.db"

        store = SqliteMetadataStore(path)

        assert path.is_file()
        await store.aclose()

    async def test_a_database_from_a_newer_version_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "metadata.db"
        SqliteMetadataStore(path)
        with sqlite3.connect(path) as connection:
            connection.execute("PRAGMA user_version = 99")

        with pytest.raises(ValueError, match="schema version 99"):
            SqliteMetadataStore(path)


class TestSeveralProcessesOnOneFile:
    """Two store objects on one file stand in for two worker processes."""

    async def test_two_instances_never_hand_out_the_same_chunk(self, tmp_path: Path) -> None:
        path = tmp_path / "metadata.db"
        first, second = SqliteMetadataStore(path), SqliteMetadataStore(path)
        chunk_ids = [f"c{i:03d}" for i in range(40)]
        await first.register_chunks("job", chunk_ids)

        async def drain(store: MetadataStore, name: str) -> list[str]:
            received = []
            while claim := await store.claim(
                "job", Stage.EXTRACT, worker_id=name, lease_seconds=30
            ):
                received.append(claim.chunk_id)
            return received

        taken_by_first, taken_by_second = await asyncio.gather(
            drain(first, "one"), drain(second, "two")
        )

        assert sorted(taken_by_first + taken_by_second) == chunk_ids
        await first.aclose()
        await second.aclose()


class TestSeveralInstancesAgreeOnThePlan:
    async def test_two_instances_planning_at_once_end_with_one_plan(self, tmp_path: Path) -> None:
        path = tmp_path / "metadata.db"
        stores = [SqliteMetadataStore(path), SqliteMetadataStore(path)]

        answers = await asyncio.gather(
            *(store.save_plan("job", f"plan from {i}".encode()) for i, store in enumerate(stores))
        )

        assert answers[0] == answers[1]
        assert await stores[0].load_plan("job") == await stores[1].load_plan("job") == answers[0]
        for store in stores:
            await store.aclose()


class TestLeasesWithAFakeClock:
    async def test_a_lease_expires_exactly_at_its_deadline(self) -> None:
        clock = FakeClock()
        store = SqliteMetadataStore(clock=clock)
        await store.register_chunks("job", ["a"])
        await store.claim("job", Stage.EXTRACT, worker_id="w", lease_seconds=10)

        clock.advance(9)
        assert await store.release_expired("job") == 0

        clock.advance(1)
        assert await store.release_expired("job") == 1
        await store.aclose()


class TestFactory:
    """`create_store` is what the plugin registry calls with the URL from the configuration."""

    async def test_a_path_selects_the_database_file(self, tmp_path: Path) -> None:
        store = create_store(f"sqlite://{tmp_path}/meta.db")
        await store.register_chunks("job", ["a"])
        await store.aclose()

        assert (tmp_path / "meta.db").is_file()

    async def test_no_path_means_an_in_memory_database(self) -> None:
        store = create_store("sqlite://")

        assert isinstance(store, MetadataStore)
        await store.aclose()

    @pytest.mark.parametrize(
        "url",
        [
            pytest.param("memory://", id="wrong-scheme"),
            pytest.param("sqlite://somehost/x.db", id="host-not-allowed"),
            pytest.param("sqlite:///x.db?colour=blue", id="unknown-parameter"),
        ],
    )
    def test_bad_urls_are_rejected_with_a_clear_error(self, url: str) -> None:
        with pytest.raises(ValueError, match="sqlite"):
            create_store(url)
