"""The MetadataStore contract: the chunk state machine, value objects, and the protocol."""

import dataclasses

import pytest

from bitdrop_api.metadata_store import (
    DEFAULT_MAX_ATTEMPTS,
    ChunkRecord,
    ChunkState,
    Claim,
    MetadataStore,
    Stage,
)


class TestChunkState:
    def test_values_are_stable_strings_usable_in_storage(self) -> None:
        assert [state.value for state in ChunkState] == [
            "pending",
            "extracting",
            "extracted",
            "loading",
            "loaded",
            "failed",
        ]

    def test_loaded_and_failed_are_the_only_terminal_states(self) -> None:
        terminal = {state for state in ChunkState if state.is_terminal}

        assert terminal == {ChunkState.LOADED, ChunkState.FAILED}


class TestStage:
    """A stage is one hop of the pipeline: waiting -> in_flight -> done."""

    def test_extraction_moves_pending_chunks_to_extracted(self) -> None:
        assert Stage.EXTRACT.waiting is ChunkState.PENDING
        assert Stage.EXTRACT.in_flight is ChunkState.EXTRACTING
        assert Stage.EXTRACT.done is ChunkState.EXTRACTED

    def test_loading_moves_extracted_chunks_to_loaded(self) -> None:
        assert Stage.LOAD.waiting is ChunkState.EXTRACTED
        assert Stage.LOAD.in_flight is ChunkState.LOADING
        assert Stage.LOAD.done is ChunkState.LOADED

    def test_the_stages_chain_together(self) -> None:
        assert Stage.EXTRACT.done is Stage.LOAD.waiting


class TestValueObjects:
    def test_claim_is_immutable(self) -> None:
        claim = Claim(job_id="j", chunk_id="c", stage=Stage.EXTRACT, worker_id="w", fence=1)

        with pytest.raises(dataclasses.FrozenInstanceError):
            claim.fence = 2  # type: ignore[misc]

    def test_chunk_record_is_immutable(self) -> None:
        record = ChunkRecord(
            job_id="j",
            chunk_id="c",
            state=ChunkState.PENDING,
            failures=0,
            worker_id=None,
            last_error=None,
        )

        with pytest.raises(dataclasses.FrozenInstanceError):
            record.state = ChunkState.LOADED  # type: ignore[misc]

    def test_default_attempt_limit_is_positive(self) -> None:
        assert DEFAULT_MAX_ATTEMPTS >= 1


class TestMetadataStoreProtocol:
    def test_object_with_all_methods_is_a_metadata_store(self) -> None:
        class Complete:
            async def register_chunks(self, *args: object, **kwargs: object) -> int:
                return 0

            async def claim(self, *args: object, **kwargs: object) -> Claim | None:
                return None

            async def heartbeat(self, *args: object, **kwargs: object) -> None: ...
            async def complete(self, *args: object, **kwargs: object) -> None: ...
            async def fail(self, *args: object, **kwargs: object) -> None: ...
            async def release(self, *args: object, **kwargs: object) -> None: ...

            async def release_expired(self, *args: object, **kwargs: object) -> int:
                return 0

            async def get_chunk(self, *args: object, **kwargs: object) -> ChunkRecord:
                raise NotImplementedError

            async def counts(self, *args: object, **kwargs: object) -> dict[ChunkState, int]:
                return {}

            async def aclose(self) -> None: ...

        assert isinstance(Complete(), MetadataStore)

    def test_object_missing_a_method_is_not_a_metadata_store(self) -> None:
        class OnlyClaim:
            async def claim(self, *args: object, **kwargs: object) -> Claim | None:
                return None

        assert not isinstance(OnlyClaim(), MetadataStore)
