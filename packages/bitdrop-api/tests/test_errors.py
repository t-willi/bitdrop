"""Error hierarchy: callers catch by *meaning*, never by backend."""

import pytest

from bitdrop_api.errors import (
    BitdropError,
    ChunkNotFound,
    ChunkStoreError,
    ChunkTooLarge,
    InvalidKey,
    LeaseLost,
    MetadataStoreError,
    StoreFull,
    StoreUnavailable,
    UnknownChunk,
)
from bitdrop_api.metadata_store import Claim, Stage


class TestHierarchy:
    @pytest.mark.parametrize("error_type", [ChunkNotFound, ChunkTooLarge, StoreFull])
    def test_chunk_store_errors_share_a_base(self, error_type: type[BitdropError]) -> None:
        assert issubclass(error_type, ChunkStoreError)

    @pytest.mark.parametrize("error_type", [LeaseLost, UnknownChunk])
    def test_metadata_store_errors_share_a_base(self, error_type: type[BitdropError]) -> None:
        assert issubclass(error_type, MetadataStoreError)

    def test_everything_derives_from_the_bitdrop_base(self) -> None:
        for error_type in (ChunkStoreError, MetadataStoreError, InvalidKey):
            assert issubclass(error_type, BitdropError)

    def test_invalid_key_belongs_to_neither_store_family(self) -> None:
        """Keys and ids are validated by both kinds of store, so the error is shared."""
        assert not issubclass(InvalidKey, ChunkStoreError)
        assert not issubclass(InvalidKey, MetadataStoreError)


class TestStoreUnavailable:
    def test_is_a_chunk_store_error(self) -> None:
        assert issubclass(StoreUnavailable, ChunkStoreError)

    def test_names_the_key_when_one_was_involved(self) -> None:
        error = StoreUnavailable("connection refused", key="job-1:c7")

        assert error.key == "job-1:c7"
        assert "connection refused" in str(error)
        assert "job-1:c7" in str(error)

    def test_needs_no_key_for_operations_without_one(self) -> None:
        error = StoreUnavailable("timed out")

        assert error.key is None
        assert "timed out" in str(error)
        assert "key=" not in str(error)


class TestMessages:
    def test_lease_lost_names_the_claim(self) -> None:
        claim = Claim(job_id="job1", chunk_id="c7", stage=Stage.LOAD, worker_id="w", fence=3)

        error = LeaseLost(claim)

        assert error.claim == claim
        assert "job1" in str(error)
        assert "c7" in str(error)

    def test_unknown_chunk_names_job_and_chunk(self) -> None:
        error = UnknownChunk("job1", "c7")

        assert (error.job_id, error.chunk_id) == ("job1", "c7")
        assert "job1" in str(error)
        assert "c7" in str(error)
