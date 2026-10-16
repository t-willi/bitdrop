"""Conformance tests for the :class:`~bitdrop_api.metadata_store.MetadataStore` contract.

An adapter is *supported* if and only if it passes this kit (see ``docs/principles.md``, P1).

How to use it in an adapter package::

    class TestMyStoreConformance(MetadataStoreConformance):
        @pytest.fixture
        async def store(self) -> AsyncIterator[MetadataStore]:
            store = MyStore(...)
            yield store
            await store.aclose()

The lease tests use real (but very short) leases and real sleeping, so they work against any
backend without a controllable clock.
"""

import asyncio
import dataclasses
from collections.abc import AsyncIterator

import pytest

from bitdrop_api.errors import InvalidKey, LeaseLost, UnknownChunk
from bitdrop_api.metadata_store import ChunkState, Claim, MetadataStore, Stage

JOB = "job-1"
LONG_LEASE = 30.0
SHORT_LEASE = 0.03
PAST_SHORT_LEASE = 0.1
"""How long to sleep so that a ``SHORT_LEASE`` has surely expired."""


async def _register(
    store: MetadataStore, count: int, *, job: str = JOB, max_attempts: int = 3
) -> list[str]:
    chunk_ids = [f"c{i:03d}" for i in range(count)]
    await store.register_chunks(job, chunk_ids, max_attempts=max_attempts)
    return chunk_ids


async def _claim(
    store: MetadataStore,
    stage: Stage = Stage.EXTRACT,
    *,
    job: str = JOB,
    worker: str = "worker-1",
    lease: float = LONG_LEASE,
) -> Claim:
    claim = await store.claim(job, stage, worker_id=worker, lease_seconds=lease)
    assert claim is not None, f"expected a chunk waiting for {stage}, found none"
    return claim


async def _extract_one(store: MetadataStore) -> Claim:
    """Claim a pending chunk and complete its extraction. Returns the (now spent) claim."""
    claim = await _claim(store)
    await store.complete(claim)
    return claim


async def _drain(store: MetadataStore, job: str, worker: str, *, limit: int = 200) -> list[str]:
    """Claim extraction work until none is left; return the chunk ids this worker received.

    Bounded by ``limit``: a broken store that never runs out of work (handing the same chunk out
    forever) must fail the test with a clear assertion rather than hang it.
    """
    received: list[str] = []
    while len(received) < limit:
        claim = await store.claim(job, Stage.EXTRACT, worker_id=worker, lease_seconds=30)
        if claim is None:
            break
        received.append(claim.chunk_id)
        await asyncio.sleep(0)  # let other workers interleave
    return received


class MetadataStoreConformance:
    """Base class of the contract tests. Subclass it and override the ``store`` fixture."""

    @pytest.fixture
    async def store(self) -> AsyncIterator[MetadataStore]:
        """Yield a fresh, empty store. Required: override this in your subclass."""
        raise NotImplementedError("override the `store` fixture in your Test subclass")
        yield  # pragma: no cover  # noqa: PT022  (makes this an async generator like overrides)

    # -- protocol -------------------------------------------------------------------------------

    async def test_store_satisfies_the_protocol(self, store: MetadataStore) -> None:
        """The object structurally implements every method of ``MetadataStore``."""
        assert isinstance(store, MetadataStore)

    # -- registration ---------------------------------------------------------------------------

    async def test_registered_chunks_start_pending_with_a_clean_record(
        self, store: MetadataStore
    ) -> None:
        """New chunks are ``PENDING`` with no failures, holder, or error."""
        await _register(store, 3)

        record = await store.get_chunk(JOB, "c001")

        assert (record.state, record.failures) == (ChunkState.PENDING, 0)
        assert (record.worker_id, record.last_error) == (None, None)
        assert (await store.counts(JOB))[ChunkState.PENDING] == 3

    async def test_register_returns_how_many_chunks_were_new(self, store: MetadataStore) -> None:
        """Re-registering known ids adds nothing and reports only the genuinely new ones."""
        assert await store.register_chunks(JOB, ["a", "b"]) == 2
        assert await store.register_chunks(JOB, ["b", "c"]) == 1
        assert (await store.counts(JOB))[ChunkState.PENDING] == 3

    async def test_registering_again_never_resets_progress(self, store: MetadataStore) -> None:
        """A restarted planner may re-register its plan without disturbing running work."""
        chunk_ids = await _register(store, 2)
        await _claim(store)

        added = await store.register_chunks(JOB, chunk_ids)

        counts = await store.counts(JOB)
        assert added == 0
        assert (counts[ChunkState.EXTRACTING], counts[ChunkState.PENDING]) == (1, 1)

    async def test_registering_nothing_is_fine(self, store: MetadataStore) -> None:
        """An empty plan registers zero chunks."""
        assert await store.register_chunks(JOB, []) == 0

    @pytest.mark.parametrize("bad_id", ["", "a/b", ".hidden", "has space"])
    async def test_invalid_ids_are_rejected(self, store: MetadataStore, bad_id: str) -> None:
        """Job ids and chunk ids follow the shared key rule."""
        with pytest.raises(InvalidKey):
            await store.register_chunks(bad_id, ["ok"])
        with pytest.raises(InvalidKey):
            await store.register_chunks(JOB, ["ok", bad_id])

    async def test_a_rejected_registration_adds_nothing(self, store: MetadataStore) -> None:
        """One bad id in a batch means none of the batch is registered."""
        with pytest.raises(InvalidKey):
            await store.register_chunks(JOB, ["good", "a/b"])

        assert (await store.counts(JOB))[ChunkState.PENDING] == 0

    async def test_max_attempts_must_be_at_least_one(self, store: MetadataStore) -> None:
        """Zero attempts would make a chunk unusable from the start."""
        with pytest.raises(ValueError, match="max_attempts"):
            await store.register_chunks(JOB, ["a"], max_attempts=0)

    async def test_jobs_are_isolated(self, store: MetadataStore) -> None:
        """The same chunk id in two jobs is two different chunks."""
        await _register(store, 1, job="job-a")
        await _register(store, 1, job="job-b")

        claim = await _claim(store, job="job-a")

        assert claim.job_id == "job-a"
        assert (await store.get_chunk("job-a", "c000")).state is ChunkState.EXTRACTING
        assert (await store.get_chunk("job-b", "c000")).state is ChunkState.PENDING
        assert await store.claim("job-c", Stage.EXTRACT, worker_id="w", lease_seconds=30) is None

    async def test_counts_include_every_state_even_for_unknown_jobs(
        self, store: MetadataStore
    ) -> None:
        """``counts`` always has one entry per state, so callers never need ``.get``."""
        assert dict(await store.counts("nobody")) == dict.fromkeys(ChunkState, 0)

    async def test_unknown_chunks_raise_unknown_chunk(self, store: MetadataStore) -> None:
        """Asking about a chunk that was never registered is an error, not a default."""
        await _register(store, 1)

        with pytest.raises(UnknownChunk) as excinfo:
            await store.get_chunk(JOB, "nope")
        with pytest.raises(UnknownChunk):
            await store.get_chunk("other-job", "c000")

        assert (excinfo.value.job_id, excinfo.value.chunk_id) == (JOB, "nope")

    # -- claiming -------------------------------------------------------------------------------

    async def test_claim_takes_a_pending_chunk_for_extraction(self, store: MetadataStore) -> None:
        """A claim names its chunk and worker; the chunk is then ``EXTRACTING`` under them."""
        chunk_ids = await _register(store, 3)

        claim = await _claim(store, worker="extractor-7")

        record = await store.get_chunk(JOB, claim.chunk_id)
        assert claim.chunk_id in chunk_ids
        assert (claim.job_id, claim.stage, claim.worker_id) == (JOB, Stage.EXTRACT, "extractor-7")
        assert (record.state, record.worker_id) == (ChunkState.EXTRACTING, "extractor-7")

    async def test_claim_returns_none_when_nothing_waits(self, store: MetadataStore) -> None:
        """No chunks (or none waiting) means ``None``, not an error."""
        assert await store.claim(JOB, Stage.EXTRACT, worker_id="w", lease_seconds=30) is None

    async def test_every_chunk_is_claimed_exactly_once(self, store: MetadataStore) -> None:
        """Five chunks yield five distinct claims, then nothing."""
        chunk_ids = await _register(store, 5)

        claimed = [(await _claim(store)).chunk_id for _ in chunk_ids]

        assert sorted(claimed) == sorted(chunk_ids)
        assert await store.claim(JOB, Stage.EXTRACT, worker_id="w", lease_seconds=30) is None

    async def test_loading_waits_until_extraction_completed(self, store: MetadataStore) -> None:
        """A chunk becomes loadable only after its extraction completed."""
        await _register(store, 1)
        assert await store.claim(JOB, Stage.LOAD, worker_id="w", lease_seconds=30) is None

        await _extract_one(store)
        claim = await _claim(store, Stage.LOAD, worker="loader-1")

        record = await store.get_chunk(JOB, claim.chunk_id)
        assert (claim.stage, record.state, record.worker_id) == (
            Stage.LOAD,
            ChunkState.LOADING,
            "loader-1",
        )

    async def test_concurrent_workers_never_receive_the_same_chunk(
        self, store: MetadataStore
    ) -> None:
        """The core guarantee: under contention each chunk is handed out exactly once."""
        chunk_ids = await _register(store, 60)

        received = await asyncio.gather(*(_drain(store, JOB, f"w{i}") for i in range(12)))

        everything = [chunk_id for batch in received for chunk_id in batch]
        assert len(everything) == len(chunk_ids), "a chunk was handed out twice or never"
        assert set(everything) == set(chunk_ids)

    @pytest.mark.parametrize("lease", [0, -1.0])
    async def test_leases_must_be_positive(self, store: MetadataStore, lease: float) -> None:
        """A lease that is over the moment it is granted is a caller bug, and changes nothing."""
        await _register(store, 1)
        with pytest.raises(ValueError, match="lease_seconds"):
            await store.claim(JOB, Stage.EXTRACT, worker_id="w", lease_seconds=lease)
        claim = await _claim(store)

        with pytest.raises(ValueError, match="lease_seconds"):
            await store.heartbeat(claim, lease_seconds=lease)

    async def test_fence_grows_with_every_claim_of_a_chunk(self, store: MetadataStore) -> None:
        """Re-claiming a chunk after a failure gives a strictly larger fence."""
        await _register(store, 1)
        first = await _claim(store)
        await store.fail(first, "boom")

        second = await _claim(store)

        assert second.chunk_id == first.chunk_id
        assert second.fence > first.fence

    # -- completing -----------------------------------------------------------------------------

    async def test_completing_extraction_makes_the_chunk_extracted(
        self, store: MetadataStore
    ) -> None:
        """``EXTRACTING -> EXTRACTED`` and the holder is cleared."""
        await _register(store, 1)

        claim = await _extract_one(store)

        record = await store.get_chunk(JOB, claim.chunk_id)
        assert (record.state, record.worker_id) == (ChunkState.EXTRACTED, None)

    async def test_the_happy_path_ends_in_loaded(self, store: MetadataStore) -> None:
        """Register, extract, load: the chunk ends ``LOADED`` and the counts agree."""
        await _register(store, 1)
        await _extract_one(store)
        claim = await _claim(store, Stage.LOAD)

        await store.complete(claim)

        assert (await store.get_chunk(JOB, claim.chunk_id)).state is ChunkState.LOADED
        counts = await store.counts(JOB)
        assert counts[ChunkState.LOADED] == 1
        assert sum(counts.values()) == 1

    async def test_completing_twice_raises_lease_lost(self, store: MetadataStore) -> None:
        """The second ``complete`` of one claim is rejected; the chunk stays as it is."""
        await _register(store, 1)
        claim = await _extract_one(store)

        with pytest.raises(LeaseLost):
            await store.complete(claim)

        assert (await store.get_chunk(JOB, claim.chunk_id)).state is ChunkState.EXTRACTED

    async def test_a_forged_claim_is_rejected(self, store: MetadataStore) -> None:
        """A claim with a fence the store never issued changes nothing."""
        await _register(store, 1)
        claim = await _claim(store)
        forged = dataclasses.replace(claim, fence=claim.fence + 1)

        with pytest.raises(LeaseLost):
            await store.complete(forged)

        assert (await store.get_chunk(JOB, claim.chunk_id)).state is ChunkState.EXTRACTING

    # -- failing and retrying -------------------------------------------------------------------

    async def test_failed_extraction_returns_the_chunk_to_pending(
        self, store: MetadataStore
    ) -> None:
        """One failure is recorded with its reason and the chunk can be claimed again."""
        await _register(store, 1)
        claim = await _claim(store)

        await store.fail(claim, "source timed out")

        record = await store.get_chunk(JOB, claim.chunk_id)
        assert (record.state, record.failures) == (ChunkState.PENDING, 1)
        assert (record.worker_id, record.last_error) == (None, "source timed out")
        assert (await _claim(store)).chunk_id == claim.chunk_id

    async def test_failed_load_returns_the_chunk_to_extracted(self, store: MetadataStore) -> None:
        """A load failure keeps the extracted data: the chunk goes back to waiting for a loader."""
        await _register(store, 1)
        await _extract_one(store)
        claim = await _claim(store, Stage.LOAD)

        await store.fail(claim, "destination unavailable")

        record = await store.get_chunk(JOB, claim.chunk_id)
        assert (record.state, record.failures) == (ChunkState.EXTRACTED, 1)

    async def test_failed_load_with_reextract_returns_the_chunk_to_pending(
        self, store: MetadataStore
    ) -> None:
        """If the data vanished from the chunk store, the chunk must be extracted again."""
        await _register(store, 1)
        await _extract_one(store)
        claim = await _claim(store, Stage.LOAD)

        await store.fail(claim, "chunk missing from store", reextract=True)

        assert (await store.get_chunk(JOB, claim.chunk_id)).state is ChunkState.PENDING

    async def test_a_chunk_fails_for_good_after_max_attempts(self, store: MetadataStore) -> None:
        """After ``max_attempts`` failures the chunk is ``FAILED`` and is never handed out again."""
        await _register(store, 1, max_attempts=2)

        await store.fail(await _claim(store), "first")
        await store.fail(await _claim(store), "second")

        record = await store.get_chunk(JOB, "c000")
        assert (record.state, record.failures, record.last_error) == (
            ChunkState.FAILED,
            2,
            "second",
        )
        assert await store.claim(JOB, Stage.EXTRACT, worker_id="w", lease_seconds=30) is None
        assert (await store.counts(JOB))[ChunkState.FAILED] == 1

    async def test_failing_with_a_stale_claim_raises_lease_lost(self, store: MetadataStore) -> None:
        """A worker that already completed cannot then report a failure."""
        await _register(store, 1)
        claim = await _extract_one(store)

        with pytest.raises(LeaseLost):
            await store.fail(claim, "too late")

    # -- leases ---------------------------------------------------------------------------------

    async def test_release_expired_requeues_chunks_whose_lease_ran_out(
        self, store: MetadataStore
    ) -> None:
        """A dead worker's chunk goes back to ``PENDING`` and counts as a failure."""
        await _register(store, 1)
        claim = await _claim(store, lease=SHORT_LEASE)
        await asyncio.sleep(PAST_SHORT_LEASE)

        released = await store.release_expired(JOB)

        record = await store.get_chunk(JOB, claim.chunk_id)
        assert released == 1
        assert (record.state, record.failures, record.worker_id) == (ChunkState.PENDING, 1, None)
        assert record.last_error == "lease expired"
        assert (await _claim(store)).chunk_id == claim.chunk_id

    async def test_expired_load_leases_return_to_extracted(self, store: MetadataStore) -> None:
        """A loader that died leaves its extracted data waiting for another loader."""
        await _register(store, 1)
        await _extract_one(store)
        await _claim(store, Stage.LOAD, lease=SHORT_LEASE)
        await asyncio.sleep(PAST_SHORT_LEASE)

        await store.release_expired(JOB)

        assert (await store.get_chunk(JOB, "c000")).state is ChunkState.EXTRACTED

    async def test_release_expired_leaves_live_leases_alone(self, store: MetadataStore) -> None:
        """Only expired claims are voided."""
        await _register(store, 1)
        await _claim(store, lease=LONG_LEASE)

        assert await store.release_expired(JOB) == 0
        assert (await store.get_chunk(JOB, "c000")).state is ChunkState.EXTRACTING

    async def test_release_expired_only_touches_its_own_job(self, store: MetadataStore) -> None:
        """Releasing job B does not void job A's expired claim."""
        await _register(store, 1, job="job-a")
        await _claim(store, job="job-a", lease=SHORT_LEASE)
        await asyncio.sleep(PAST_SHORT_LEASE)

        assert await store.release_expired("job-b") == 0
        assert (await store.get_chunk("job-a", "c000")).state is ChunkState.EXTRACTING

    async def test_a_zombie_holder_is_fenced_out(self, store: MetadataStore) -> None:
        """After release and re-claim, the old holder can do nothing; the new one can."""
        await _register(store, 1)
        zombie = await _claim(store, worker="zombie", lease=SHORT_LEASE)
        await asyncio.sleep(PAST_SHORT_LEASE)
        await store.release_expired(JOB)
        owner = await _claim(store, worker="owner")

        with pytest.raises(LeaseLost):
            await store.complete(zombie)
        with pytest.raises(LeaseLost):
            await store.heartbeat(zombie, lease_seconds=30)
        with pytest.raises(LeaseLost):
            await store.fail(zombie, "late")
        await store.complete(owner)

        record = await store.get_chunk(JOB, "c000")
        assert (record.state, record.failures) == (ChunkState.EXTRACTED, 1)

    async def test_heartbeat_extends_the_lease(self, store: MetadataStore) -> None:
        """A renewed lease survives past its original expiry."""
        await _register(store, 1)
        claim = await _claim(store, lease=SHORT_LEASE)
        await store.heartbeat(claim, lease_seconds=LONG_LEASE)
        await asyncio.sleep(PAST_SHORT_LEASE)

        assert await store.release_expired(JOB) == 0

    async def test_heartbeat_with_a_stale_claim_raises_lease_lost(
        self, store: MetadataStore
    ) -> None:
        """Only the current holder may renew."""
        await _register(store, 1)
        claim = await _extract_one(store)

        with pytest.raises(LeaseLost):
            await store.heartbeat(claim, lease_seconds=30)

    async def test_an_expired_lease_on_the_last_attempt_fails_the_chunk(
        self, store: MetadataStore
    ) -> None:
        """Expiry counts as a failure, so it can exhaust the attempts too."""
        await _register(store, 1, max_attempts=1)
        await _claim(store, lease=SHORT_LEASE)
        await asyncio.sleep(PAST_SHORT_LEASE)

        assert await store.release_expired(JOB) == 1

        assert (await store.get_chunk(JOB, "c000")).state is ChunkState.FAILED
