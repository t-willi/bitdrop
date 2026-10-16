"""Cutting a key range into chunk-sized pieces."""

import math

MAX_PLANNED_CHUNKS = 1_000_000
"""More chunks than this mean the target size is far too small for the table."""


def split_range(
    lo: int,
    hi: int,
    *,
    rows_per_chunk: int,
    estimated_rows: int,
    max_chunks: int = MAX_PLANNED_CHUNKS,
) -> tuple[list[tuple[int, int]], str | None]:
    """Split the inclusive key range ``lo..hi`` into chunk-sized inclusive ranges.

    Each range holds about ``rows_per_chunk`` rows. Bounds are inclusive so a range may end at
    the largest ``bigint`` without overflowing.
    ``estimated_rows`` is how many rows live in ``lo..hi``; it lets sparse keys (few rows over a
    huge id span) get proportionally wider ranges instead of millions of empty ones. When it is
    unknown (``<= 0``) the keys are assumed dense.

    Returns the ranges and, if the chunk count had to be capped at ``max_chunks``, a note
    saying so.

    Raises:
        ValueError: ``lo`` is greater than ``hi``.
    """
    if lo > hi:
        raise ValueError(f"lo ({lo}) must not exceed hi ({hi})")
    span = hi - lo + 1
    if estimated_rows > 0:
        width = max(1, math.ceil(rows_per_chunk * span / estimated_rows))
    else:
        width = max(1, rows_per_chunk)

    note = None
    count = math.ceil(span / width)
    if count > max_chunks:
        width = math.ceil(span / max_chunks)
        count = math.ceil(span / width)
        note = (
            f"the target chunk size would need more than {max_chunks} chunks; "
            f"using {count} larger ones. Raise target_chunk_bytes."
        )
    return [(lo + i * width, min(lo + (i + 1) * width - 1, hi)) for i in range(count)], note
