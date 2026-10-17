"""Chunk ids that say what to read, so a worker handed only an id can read the chunk.

Three spellings, each the only spelling of its chunk (the destination's ledger is keyed by the id,
so a chunk with two ids would be loaded twice):

* ``all``: the whole table (no integer key to split on).
* ``<column>.<first>.<last>``: rows whose key is between the two integers, ends included, for a
  column named with letters, digits and underscores only, e.g. ``id.1.10000`` or ``id.-50.-1``.
* ``x.<text>``: any other column name (spaces, accents, quotes, a leading underscore) would break
  the key rule, so the payload is written as compact JSON with sorted keys, then URL-safe base64
  without padding.
"""

import base64
import binascii
import json
import re
from collections.abc import Mapping
from typing import Any

from bitdrop_api.errors import InvalidChunkId

__all__ = ["chunk_id_for", "payload_from_id"]

_INTEGER = r"-?[0-9]+"  # other spellings (01, -0) are refused by the round trip in payload_from_id
_READABLE_COLUMN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_]*")
_RANGE = re.compile(
    rf"(?P<column>{_READABLE_COLUMN.pattern})\.(?P<lo>{_INTEGER})\.(?P<hi>{_INTEGER})"
)
_ENCODED = re.compile(r"x\.(?P<text>[A-Za-z0-9_-]+)")
_WHOLE_TABLE = "all"


def chunk_id_for(payload: Mapping[str, Any]) -> str:
    """Write the id of the chunk with this payload (``{}`` or ``{column, lo, hi}``)."""
    if not payload:
        return _WHOLE_TABLE
    column, lo, hi = payload["column"], payload["lo"], payload["hi"]
    if _READABLE_COLUMN.fullmatch(column):
        return f"{column}.{lo}.{hi}"
    document = json.dumps(
        {"column": column, "hi": hi, "lo": lo}, separators=(",", ":"), ensure_ascii=True
    )
    text = base64.urlsafe_b64encode(document.encode("ascii")).decode("ascii").rstrip("=")
    return f"x.{text}"


def payload_from_id(chunk_id: str) -> dict[str, Any]:
    """Read the payload out of an id written by :func:`chunk_id_for`.

    Raises:
        InvalidChunkId: ``chunk_id`` is not exactly the id :func:`chunk_id_for` writes for some
            chunk (a second spelling of a chunk is refused, not accepted).
    """
    payload = _parse(chunk_id)
    if payload is None or chunk_id_for(payload) != chunk_id:
        raise InvalidChunkId(chunk_id, "not an id this source writes")
    return payload


def _parse(chunk_id: str) -> dict[str, Any] | None:
    if chunk_id == _WHOLE_TABLE:
        return {}
    if found := _RANGE.fullmatch(chunk_id):
        payload: dict[str, Any] = {
            "column": found["column"],
            "lo": int(found["lo"]),
            "hi": int(found["hi"]),
        }
    elif found := _ENCODED.fullmatch(chunk_id):
        payload = _decode(found["text"])
    else:
        return None
    if not _is_range(payload):
        return None
    return payload


def _decode(text: str) -> dict[str, Any]:
    try:
        document = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)).decode("ascii")
        decoded = json.loads(document)
    except binascii.Error, UnicodeDecodeError, ValueError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _is_range(payload: Mapping[str, Any]) -> bool:
    """``True`` for exactly ``{column: str, lo: int, hi: int}`` with ``lo <= hi``."""
    if set(payload) != {"column", "lo", "hi"}:
        return False
    column, lo, hi = payload["column"], payload["lo"], payload["hi"]
    return (
        isinstance(column, str)
        and column != ""
        and all(isinstance(key, int) and not isinstance(key, bool) for key in (lo, hi))
        and lo <= hi
    )
