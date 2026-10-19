"""PostgreSQL chunk ids carry the chunk: column and key range, or the whole table (no database)."""

import base64
import json

import pytest

from bitdrop_api.errors import InvalidChunkId
from bitdrop_api.keys import validate_key
from bitdrop_connector_postgres._chunk_ids import chunk_id_for, payload_from_id
from bitdrop_connector_postgres.source import PostgresSource


class TestWritingAnId:
    def test_a_key_range_reads_column_then_first_and_last_key(self) -> None:
        assert chunk_id_for({"column": "id", "lo": 1, "hi": 10_000}) == "id.1.10000"

    def test_negative_keys_keep_their_sign(self) -> None:
        assert chunk_id_for({"column": "id", "lo": -50, "hi": -1}) == "id.-50.-1"

    def test_the_whole_table_is_all(self) -> None:
        assert chunk_id_for({}) == "all"

    @pytest.mark.parametrize("column", ["order id", "_id", "Größe", 'a"b', "a.b"])
    def test_a_column_the_key_rule_forbids_is_encoded_instead(self, column: str) -> None:
        chunk_id = chunk_id_for({"column": column, "lo": 1, "hi": 5})

        assert chunk_id.startswith("x.")
        validate_key(chunk_id)
        assert payload_from_id(chunk_id) == {"column": column, "lo": 1, "hi": 5}

    def test_the_same_chunk_always_gets_the_same_id(self) -> None:
        payload = {"column": "id", "lo": 7, "hi": 9}

        assert chunk_id_for(payload) == chunk_id_for(dict(reversed(payload.items())))


class TestReadingAnIdBack:
    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"column": "id", "lo": 1, "hi": 10_000},
            {"column": "id", "lo": 0, "hi": 0},
            {"column": "Order_2", "lo": -(2**62), "hi": 2**62},
            {"column": "order id", "lo": -3, "hi": 3},
        ],
    )
    def test_every_id_gives_back_the_payload_it_was_made_from(
        self, payload: dict[str, object]
    ) -> None:
        assert payload_from_id(chunk_id_for(payload)) == payload

    @pytest.mark.parametrize(
        "chunk_id",
        [
            "",
            "nonsense",
            "c000001",
            "id.1",  # a range needs both ends
            "id.1.2.3",
            "id.01.5",  # leading zero: a second spelling of id.1.5
            "id.+1.5",
            "id.-0.5",
            "id.5.1",  # first key after the last
            "id.1.5 ",
            "id.a.b",
            "_id.1.5",  # not a spelling chunk_id_for produces
            "ALL",
            "x.",
            "x.%%%%",
            "x.bm90LWpzb24",  # base64 of "not-json"
        ],
    )
    def test_anything_that_is_not_an_id_of_this_source_is_refused(self, chunk_id: str) -> None:
        with pytest.raises(InvalidChunkId) as excinfo:
            payload_from_id(chunk_id)

        assert excinfo.value.chunk_id == chunk_id

    def test_an_encoded_id_for_a_column_that_has_a_readable_id_is_refused(self) -> None:
        """One chunk, one id: the ledger would otherwise load the same rows twice."""
        document = json.dumps({"column": "id", "hi": 5, "lo": 1}, separators=(",", ":"))
        second_spelling = "x." + base64.urlsafe_b64encode(document.encode()).decode().rstrip("=")

        with pytest.raises(InvalidChunkId):
            payload_from_id(second_spelling)


class TestTheSource:
    """``chunk_from_id`` needs no database, so a source that was never connected answers it."""

    def test_an_id_gives_back_the_chunk_to_read(self) -> None:
        source = PostgresSource("postgresql://user:secret@localhost/db")

        chunk = source.chunk_from_id("people", "id.1.10000")

        assert chunk.chunk_id == "id.1.10000"
        assert chunk.payload == {"column": "id", "lo": 1, "hi": 10_000}
        assert chunk.description == "id in [1, 10000]"

    def test_all_gives_the_whole_table(self) -> None:
        chunk = PostgresSource("postgresql://user:secret@localhost/db").chunk_from_id("t", "all")

        assert chunk.payload == {}
        assert chunk.description == "whole table"

    def test_a_foreign_id_is_refused(self) -> None:
        with pytest.raises(InvalidChunkId):
            PostgresSource("postgresql://user:secret@localhost/db").chunk_from_id("t", "c000001")
