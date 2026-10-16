"""Behaviour specific to the in-memory adapter (the shared contract is covered by the kit)."""

import pytest

from bitdrop_api.metadata_store import ChunkState, MetadataStore, Stage
from bitdrop_metadata_memory import MemoryMetadataStore, create_store


class FakeClock:
    """A clock the test moves by hand, so lease timing needs no sleeping."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestLeasesWithAFakeClock:
    async def test_a_lease_expires_exactly_at_its_deadline(self) -> None:
        clock = FakeClock()
        store = MemoryMetadataStore(clock=clock)
        await store.register_chunks("job", ["a"])
        await store.claim("job", Stage.EXTRACT, worker_id="w", lease_seconds=10)

        clock.advance(9)
        assert await store.release_expired("job") == 0

        clock.advance(1)
        assert await store.release_expired("job") == 1

    async def test_a_heartbeat_restarts_the_lease_from_now(self) -> None:
        clock = FakeClock()
        store = MemoryMetadataStore(clock=clock)
        await store.register_chunks("job", ["a"])
        claim = await store.claim("job", Stage.EXTRACT, worker_id="w", lease_seconds=10)
        assert claim is not None
        clock.advance(9)

        await store.heartbeat(claim, lease_seconds=10)
        clock.advance(9)  # 18s after the claim, but only 9s after the heartbeat

        assert await store.release_expired("job") == 0

    async def test_released_chunks_can_be_claimed_by_someone_else_immediately(self) -> None:
        clock = FakeClock()
        store = MemoryMetadataStore(clock=clock)
        await store.register_chunks("job", ["a"])
        await store.claim("job", Stage.EXTRACT, worker_id="dead", lease_seconds=1)
        clock.advance(2)
        await store.release_expired("job")

        claim = await store.claim("job", Stage.EXTRACT, worker_id="alive", lease_seconds=1)

        assert claim is not None
        assert claim.worker_id == "alive"


class TestOrdering:
    """Not part of the contract, but this adapter is predictable: first in, first claimed."""

    async def test_chunks_are_claimed_in_registration_order(self) -> None:
        store = MemoryMetadataStore()
        await store.register_chunks("job", ["z", "a", "m"])

        claimed = [
            (await store.claim("job", Stage.EXTRACT, worker_id="w", lease_seconds=30)).chunk_id  # type: ignore[union-attr]
            for _ in range(3)
        ]

        assert claimed == ["z", "a", "m"]


class TestAccounting:
    async def test_counts_follow_every_transition(self) -> None:
        store = MemoryMetadataStore()
        await store.register_chunks("job", ["a", "b"])

        claim = await store.claim("job", Stage.EXTRACT, worker_id="w", lease_seconds=30)
        assert claim is not None
        await store.complete(claim)

        counts = await store.counts("job")
        assert counts[ChunkState.PENDING] == 1
        assert counts[ChunkState.EXTRACTED] == 1
        assert sum(counts.values()) == 2


class TestFactory:
    """`create_store` is what the plugin registry calls with the URL from the configuration."""

    def test_bare_url_gives_a_working_store(self) -> None:
        assert isinstance(create_store("memory://"), MetadataStore)

    @pytest.mark.parametrize(
        "url",
        [
            pytest.param("sqlite://", id="wrong-scheme"),
            pytest.param("memory://somehost", id="host-not-allowed"),
            pytest.param("memory:///some/path", id="path-not-allowed"),
            pytest.param("memory://?colour=blue", id="unknown-parameter"),
        ],
    )
    def test_bad_urls_are_rejected_with_a_clear_error(self, url: str) -> None:
        with pytest.raises(ValueError, match="memory"):
            create_store(url)
