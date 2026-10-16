"""The PostgreSQL source must pass the shared Source conformance kit (needs Docker)."""

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from bitdrop_api.source import Source
from bitdrop_api.testing import SourceConformance, make_rows
from bitdrop_connector_postgres import PostgresSource

if TYPE_CHECKING:
    from conftest import PostgresClient

pytestmark = pytest.mark.docker


class TestPostgresSourceConformance(SourceConformance):
    @pytest.fixture
    async def source(self, postgres_url: str, pg: PostgresClient) -> AsyncIterator[Source]:
        pg.create_sample_table(postgres_url, "people", rows=1000)
        pg.create_sample_table(postgres_url, "nobody")
        source = PostgresSource(postgres_url)
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
