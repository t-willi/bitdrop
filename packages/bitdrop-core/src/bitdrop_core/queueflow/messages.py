"""The messages and marker keys of the queue workflow.

Two queues connect the parts (``docs/decoupled-queue-workflow.md``): the *work queue* of chunk ids
the planner fills and source containers read, and the *file queue* of staged chunks the source
containers fill and destination containers read. A message is a little JSON naming a chunk; the
data never travels through a queue. The ``format`` number is bumped when the layout changes, and a
build refuses a number it does not know instead of guessing.

The markers are small values every container can read (a ``MarkerStore``): the settings a job was
chunked with, its schema, the fingerprint of its chunk ids, and the "planning is complete" marker.
"""

import json
from dataclasses import dataclass
from typing import Any

from bitdrop_api.errors import BitdropError
from bitdrop_api.keys import validate_key

__all__ = [
    "FORMAT",
    "CorruptMessage",
    "FileMessage",
    "WorkMessage",
    "chunking_key",
    "decode_file",
    "decode_work",
    "encode_file",
    "encode_work",
    "planned_key",
    "schema_key",
    "settings_key",
]

FORMAT = 1
"""The layout of messages and markers this build writes and reads."""


class CorruptMessage(BitdropError):
    """A queue message or marker is not one this build can read."""


@dataclass(frozen=True, slots=True)
class WorkMessage:
    """Read this chunk of the table: one message per chunk, published by the planner."""

    job: str
    table: str
    chunk: str
    """The chunk id; the source turns it back into the chunk with ``Source.chunk_from_id``."""


@dataclass(frozen=True, slots=True)
class FileMessage:
    """This chunk is staged in the chunk store: load it. Published by a source container."""

    job: str
    table: str
    chunk: str
    key: str
    """Where the encoded chunk sits in the chunk store."""

    rows: int
    bytes: int


def encode_work(message: WorkMessage) -> bytes:
    """Write a work message."""
    return _encode({"job": message.job, "table": message.table, "chunk": message.chunk})


def decode_work(body: bytes) -> WorkMessage:
    """Read a work message.

    Raises:
        CorruptMessage: ``body`` is not a work message of this build's format.
    """
    document = _decode(body)
    return WorkMessage(
        job=_key(document, "job"), table=_text(document, "table"), chunk=_key(document, "chunk")
    )


def encode_file(message: FileMessage) -> bytes:
    """Write a file message."""
    return _encode(
        {
            "job": message.job,
            "table": message.table,
            "chunk": message.chunk,
            "key": message.key,
            "rows": message.rows,
            "bytes": message.bytes,
        }
    )


def decode_file(body: bytes) -> FileMessage:
    """Read a file message.

    Raises:
        CorruptMessage: ``body`` is not a file message of this build's format.
    """
    document = _decode(body)
    return FileMessage(
        job=_key(document, "job"),
        table=_text(document, "table"),
        chunk=_key(document, "chunk"),
        key=_key(document, "key"),
        rows=_count(document, "rows"),
        bytes=_count(document, "bytes"),
    )


def settings_key(job: str) -> str:
    """The marker holding the settings the job was chunked with."""
    return f"{validate_key(job)}:settings"


def schema_key(job: str) -> str:
    """The marker holding the table's schema as the planner saw it."""
    return f"{validate_key(job)}:schema"


def chunking_key(job: str) -> str:
    """The marker holding the count and fingerprint of the job's chunk ids."""
    return f"{validate_key(job)}:chunking"


def planned_key(job: str) -> str:
    """The marker that appears, last, when every chunk id has been published."""
    return f"{validate_key(job)}:planned"


def _encode(document: dict[str, Any]) -> bytes:
    return json.dumps(
        {"format": FORMAT, **document}, separators=(",", ":"), sort_keys=True
    ).encode()


def _decode(body: bytes) -> dict[str, Any]:
    try:
        document = json.loads(body)
    except ValueError:  # includes UnicodeDecodeError
        raise CorruptMessage("not valid JSON") from None
    if not isinstance(document, dict):
        raise CorruptMessage("the top level is not an object")
    if document.get("format") != FORMAT:
        raise CorruptMessage(
            f"format {document.get('format')!r} is not supported (this build reads {FORMAT})"
        )
    return document


def _text(document: dict[str, Any], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value:
        raise CorruptMessage(f"{name!r} must be non-empty text")
    return value


def _key(document: dict[str, Any], name: str) -> str:
    value = _text(document, name)
    try:
        return validate_key(value)
    except BitdropError:
        raise CorruptMessage(f"{name!r} breaks the key rules") from None


def _count(document: dict[str, Any], name: str) -> int:
    value = document.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise CorruptMessage(f"{name!r} must be a non-negative integer")
    return value
