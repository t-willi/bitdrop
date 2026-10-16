"""P1 in action: the same code runs against every installed chunk store, chosen only by URL.

These tests use the *real* installed packages and their real entry points (no fakes), so they also
catch packaging mistakes such as a missing or misspelled ``[project.entry-points]`` table.
"""

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from bitdrop_api.chunk_store import ChunkStore
from bitdrop_api.errors import ChunkNotFound
from bitdrop_api.testing import make_payload
from bitdrop_core.registry import available_chunk_stores, open_chunk_store

if TYPE_CHECKING:
    from conftest import KvServer

# One entry per adapter: how to build a working URL for it. Add a line when you add an adapter.
URL_BUILDERS: dict[str, Callable[[Path], str]] = {
    "memory": lambda tmp_path: "memory://",
    "file": lambda tmp_path: f"file://{tmp_path}/chunks",
}
DOCKER_ADAPTERS = {"redis"}
"""Adapters that need a server; they are exercised by the Docker test at the end of this file."""


async def shuttle_chunks(store: ChunkStore, count: int) -> None:
    """A miniature extract/load round that only knows the ChunkStore contract."""
    payloads = {f"job:1:chunk:{i:04d}": make_payload(64 * 1024, seed=i) for i in range(count)}

    await asyncio.gather(*(store.put(key, value) for key, value in payloads.items()))  # extract
    for key, expected in payloads.items():  # load, then free the memory
        assert await store.get(key) == expected
        await store.delete(key)

    for key in payloads:
        with pytest.raises(ChunkNotFound):
            await store.get(key)


def test_every_installed_adapter_has_a_url_builder_in_this_test() -> None:
    """Fails when a new adapter is installed but not yet covered by the swap test."""
    assert set(available_chunk_stores()) == set(URL_BUILDERS) | DOCKER_ADAPTERS


@pytest.mark.parametrize("scheme", sorted(URL_BUILDERS))
async def test_the_same_scenario_runs_unchanged_on_every_adapter(
    scheme: str, tmp_path: Path
) -> None:
    store = open_chunk_store(URL_BUILDERS[scheme](tmp_path))

    try:
        await shuttle_chunks(store, count=16)
    finally:
        await store.aclose()


@pytest.mark.docker
async def test_the_same_scenario_runs_unchanged_on_a_network_store(kv: KvServer) -> None:
    """Redis, Valkey, and Dragonfly are all reached as ``redis://``; the scenario cannot tell."""
    store = open_chunk_store(kv.url)

    try:
        await shuttle_chunks(store, count=16)
    finally:
        await store.aclose()
