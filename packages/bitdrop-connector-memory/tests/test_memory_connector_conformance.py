"""The in-memory connectors must pass the shared Source and Destination conformance kits."""

from collections.abc import AsyncIterator

import pyarrow as pa
import pytest

from bitdrop_api.destination import Destination
from bitdrop_api.source import Source
from bitdrop_api.testing import (
    SAMPLE_SCHEMA,
    DestinationConformance,
    ReadBack,
    SourceConformance,
    make_rows,
)
from bitdrop_connector_memory import MemoryDestination, MemorySource


class TestMemorySourceConformance(SourceConformance):
    @pytest.fixture
    async def source(self) -> AsyncIterator[Source]:
        source = MemorySource({"people": make_rows(1, 1000), "nobody": SAMPLE_SCHEMA.empty_table()})
        yield source
        await source.aclose()

    @pytest.fixture
    def table_name(self) -> str:
        return "people"

    @pytest.fixture
    def expected(self) -> pa.Table:
        return make_rows(1, 1000)

    @pytest.fixture
    def empty_table_name(self) -> str:
        return "nobody"


class TestMemoryDestinationConformance(DestinationConformance):
    @pytest.fixture
    async def destination(self) -> AsyncIterator[Destination]:
        destination = MemoryDestination()
        yield destination
        await destination.aclose()

    @pytest.fixture
    def read_back(self, destination: Destination) -> ReadBack:
        assert isinstance(destination, MemoryDestination)

        async def read(table: str) -> pa.Table:
            return destination.contents(table)

        return read
