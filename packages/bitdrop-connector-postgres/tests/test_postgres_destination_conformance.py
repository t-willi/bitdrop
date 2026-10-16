"""The PostgreSQL destination must pass the shared Destination conformance kit (needs Docker)."""

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from bitdrop_api.destination import Destination
from bitdrop_api.testing import DestinationConformance, ReadBack
from bitdrop_connector_postgres import PostgresDestination

if TYPE_CHECKING:
    from conftest import PostgresClient

pytestmark = pytest.mark.docker


class TestPostgresDestinationConformance(DestinationConformance):
    @pytest.fixture
    async def destination(self, postgres_url: str) -> AsyncIterator[Destination]:
        destination = PostgresDestination(postgres_url)
        yield destination
        await destination.aclose()

    @pytest.fixture
    def read_back(self, postgres_url: str, pg: PostgresClient) -> ReadBack:
        async def read(table: str) -> pa.Table:
            return pg.read_sample(postgres_url, table)

        return read
