"""The queue workflow's messages: written as JSON, read back exactly, anything else refused."""

import json

import pytest

from bitdrop_core.queueflow import (
    CorruptMessage,
    FileMessage,
    WorkMessage,
    chunking_key,
    decode_file,
    decode_work,
    encode_file,
    encode_work,
    planned_key,
    schema_key,
    settings_key,
)


class TestWorkMessages:
    def test_a_message_reads_back_as_written(self) -> None:
        message = WorkMessage("job-1", "public.orders", "id.1.10000")

        assert decode_work(encode_work(message)) == message

    def test_it_is_json_a_person_can_read(self) -> None:
        body = json.loads(encode_work(WorkMessage("job-1", "t", "all")))

        assert body == {"format": 1, "job": "job-1", "table": "t", "chunk": "all"}

    @pytest.mark.parametrize(
        "body",
        [
            b"",
            b"not json",
            b"[]",
            b'{"format":2,"job":"j","table":"t","chunk":"c"}',
            b'{"job":"j","table":"t","chunk":"c"}',
            b'{"format":1,"job":"j","table":"t"}',
            b'{"format":1,"job":"has space","table":"t","chunk":"c"}',
            b'{"format":1,"job":"j","table":"","chunk":"c"}',
            b'{"format":1,"job":"j","table":"t","chunk":5}',
            b"\xff\xfe",
        ],
    )
    def test_anything_else_is_refused(self, body: bytes) -> None:
        with pytest.raises(CorruptMessage):
            decode_work(body)


class TestFileMessages:
    def test_a_message_reads_back_as_written(self) -> None:
        message = FileMessage(
            "job-1", "public.orders", "id.1.10000", "job-1:id.1.10000", 10000, 123
        )

        assert decode_file(encode_file(message)) == message

    @pytest.mark.parametrize("field", ["rows", "bytes"])
    @pytest.mark.parametrize("bad", [-1, "5", None, True, 1.5])
    def test_counts_must_be_non_negative_integers(self, field: str, bad: object) -> None:
        document = json.loads(encode_file(FileMessage("j", "t", "c", "k", 1, 2)))
        document[field] = bad

        with pytest.raises(CorruptMessage, match=field):
            decode_file(json.dumps(document).encode())

    def test_a_work_message_is_not_a_file_message(self) -> None:
        with pytest.raises(CorruptMessage):
            decode_file(encode_work(WorkMessage("j", "t", "c")))


class TestMarkerKeys:
    def test_each_marker_of_a_job_has_its_own_key(self) -> None:
        keys = {f("job-1") for f in (settings_key, schema_key, chunking_key, planned_key)}

        assert keys == {"job-1:settings", "job-1:schema", "job-1:chunking", "job-1:planned"}

    def test_jobs_do_not_share_keys(self) -> None:
        assert planned_key("job-1") != planned_key("job-2")
