"""Native type hints carried on Arrow fields.

Arrow's type system is the common language between connectors, but it is coarser than any
database's. A Postgres ``numeric(12,2)``, ``jsonb`` or ``varchar(10)`` all collapse into plain Arrow
types, and ``jsonb`` and ``json`` cannot even be told apart. A source therefore records the exact
native type as Arrow *field metadata*. A destination of the same engine uses the hint to recreate
the column exactly; any other destination ignores it and maps from the Arrow type.
"""

from typing import Any

import pyarrow as pa

ENGINE_KEY = b"bitdrop:engine"
NATIVE_TYPE_KEY = b"bitdrop:native_type"


def with_native_type(field: pa.Field[Any], engine: str, native_type: str) -> pa.Field[Any]:
    """Return ``field`` with a hint that its native type in ``engine`` is ``native_type``."""
    metadata: dict[bytes | str, bytes | str] = {}
    for key, value in (field.metadata or {}).items():
        metadata[key] = value
    metadata[ENGINE_KEY] = engine.encode()
    metadata[NATIVE_TYPE_KEY] = native_type.encode()
    return field.with_metadata(metadata)


def native_type(field: pa.Field[Any], engine: str) -> str | None:
    """Return the native type hint of ``field`` if it was written by ``engine``, else ``None``."""
    metadata = field.metadata or {}
    if metadata.get(ENGINE_KEY) != engine.encode():
        return None
    hint = metadata.get(NATIVE_TYPE_KEY)
    return hint.decode() if hint is not None else None
