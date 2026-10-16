"""Turning a :class:`~bitdrop_api.source.SourcePlan` into bytes and back.

A job's plan (the schema and the list of chunks) is saved in the metadata store, so a job can be
resumed from exactly the chunks it registered, and so extractors and loaders in other processes can
read it. The metadata store keeps the bytes without looking inside (it must not know about Arrow or
sources); this module is the one place that knows the format.

The format is JSON, so a person can read a saved plan with any tool. The Arrow schema is carried as
base64 of its IPC form, which keeps field metadata (the native type hints of
:mod:`bitdrop_api.schema`). The ``format`` number is bumped whenever the layout changes; a build
refuses a number it does not know instead of guessing.
"""

import base64
import binascii
import json
from typing import Any

import pyarrow as pa

from bitdrop_api.errors import CorruptPlan, InvalidKey
from bitdrop_api.keys import validate_key
from bitdrop_api.source import ChunkSpec, SourcePlan

__all__ = ["FORMAT_VERSION", "decode_plan", "encode_plan"]

FORMAT_VERSION = 1
"""The layout this build writes and reads."""


def encode_plan(plan: SourcePlan) -> bytes:
    """Return ``plan`` as bytes that :func:`decode_plan` turns back into the same plan.

    Raises:
        ValueError: a chunk payload cannot be kept exactly by JSON (a date, a tuple, a key that is
            not text, ...). It is refused instead of being stored in a changed form; the source
            should put only JSON values in its payloads.
    """
    document = {
        "format": FORMAT_VERSION,
        "table": plan.table,
        "schema": base64.b64encode(plan.schema.serialize().to_pybytes()).decode("ascii"),
        "chunks": [_encode_chunk(chunk) for chunk in plan.chunks],
        "estimated_rows": plan.estimated_rows,
        "estimated_row_bytes": plan.estimated_row_bytes,
        "notes": list(plan.notes),
    }
    return json.dumps(document, separators=(",", ":"), allow_nan=False).encode("utf-8")


def decode_plan(data: bytes) -> SourcePlan:
    """Read a plan written by :func:`encode_plan`.

    Raises:
        CorruptPlan: ``data`` is not a plan, or was written by a build with a newer format.
    """
    try:
        document = json.loads(data)
    except ValueError:  # includes UnicodeDecodeError
        raise CorruptPlan("not valid JSON") from None
    if not isinstance(document, dict):
        raise CorruptPlan("the top level is not an object")

    version = document.get("format")
    if version != FORMAT_VERSION:
        shown = version if isinstance(version, int) and not isinstance(version, bool) else "missing"
        raise CorruptPlan(f"format {shown} is not supported (this build reads {FORMAT_VERSION})")

    chunks = _decode_chunks(document.get("chunks"))
    return SourcePlan(
        table=_text(document, "table"),
        schema=_decode_schema(_text(document, "schema")),
        chunks=chunks,
        estimated_rows=_optional_int(document, "estimated_rows"),
        estimated_row_bytes=_optional_int(document, "estimated_row_bytes"),
        notes=_texts(document, "notes"),
    )


def _encode_chunk(chunk: ChunkSpec) -> dict[str, Any]:
    payload = dict(chunk.payload)
    try:
        restored = json.loads(json.dumps(payload, allow_nan=False))
    except (TypeError, ValueError) as error:
        raise ValueError(f"chunk {chunk.chunk_id!r}: the payload is not JSON ({error})") from error
    if restored != payload:
        raise ValueError(
            f"chunk {chunk.chunk_id!r}: the payload would change in JSON (tuples become lists, "
            "keys become text); use lists and text keys"
        )
    return {
        "chunk_id": chunk.chunk_id,
        "description": chunk.description,
        "payload": payload,
        "estimated_rows": chunk.estimated_rows,
    }


def _decode_schema(encoded: str) -> pa.Schema:
    try:
        return pa.ipc.read_schema(pa.py_buffer(base64.b64decode(encoded, validate=True)))
    except binascii.Error, ValueError, pa.ArrowException:
        raise CorruptPlan("the schema is not a valid Arrow schema") from None


def _decode_chunks(value: object) -> tuple[ChunkSpec, ...]:
    if not isinstance(value, list):
        raise CorruptPlan("chunks must be a list")
    chunks: list[ChunkSpec] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            raise CorruptPlan("a chunk must be an object")
        chunk_id = _text(item, "chunk_id")
        try:
            validate_key(chunk_id)
        except InvalidKey:
            raise CorruptPlan("a chunk id breaks the key rules") from None
        if chunk_id in seen:
            raise CorruptPlan(f"chunk id {chunk_id!r} appears twice")
        seen.add(chunk_id)
        payload = item.get("payload")
        if not isinstance(payload, dict):
            raise CorruptPlan("a chunk payload must be an object")
        chunks.append(
            ChunkSpec(
                chunk_id=chunk_id,
                description=_text(item, "description"),
                payload=payload,
                estimated_rows=_optional_int(item, "estimated_rows"),
            )
        )
    return tuple(chunks)


def _text(document: dict[str, Any], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str):
        raise CorruptPlan(f"{key!r} must be text")
    return value


def _optional_int(document: dict[str, Any], key: str) -> int | None:
    value = document.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise CorruptPlan(f"{key!r} must be a whole number or null")
    return value


def _texts(document: dict[str, Any], key: str) -> tuple[str, ...]:
    value = document.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise CorruptPlan(f"{key!r} must be a list of text")
    return tuple(value)
