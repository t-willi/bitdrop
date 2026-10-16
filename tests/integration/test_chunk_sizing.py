"""How big a chunk may be: the job's wish, shrunk to what the staging store can hold."""

from bitdrop_core.transfer import TransferConfig, effective_chunk_bytes
from bitdrop_store_memory import MemoryChunkStore

MIB = 1024 * 1024


def config(target: int) -> TransferConfig:
    return TransferConfig(
        job_id="j", source_table="a", destination_table="b", target_chunk_bytes=target
    )


def test_a_store_without_a_limit_gets_the_requested_size() -> None:
    assert effective_chunk_bytes(config(64 * MIB), MemoryChunkStore()) == 64 * MIB


def test_a_store_with_a_limit_gets_chunks_of_half_that_limit() -> None:
    """Encoding adds overhead and sizes are estimates, so there is headroom."""
    store = MemoryChunkStore(max_value_bytes=1 * MIB)

    assert effective_chunk_bytes(config(64 * MIB), store) == MIB // 2


def test_a_request_below_the_limit_is_left_alone() -> None:
    store = MemoryChunkStore(max_value_bytes=100 * MIB)

    assert effective_chunk_bytes(config(8 * MIB), store) == 8 * MIB
