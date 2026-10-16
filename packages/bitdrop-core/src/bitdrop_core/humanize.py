"""Numbers for people: one wording for the plain output and the TUI."""

import math

__all__ = ["format_bytes", "format_count", "format_duration", "format_rate"]

_BYTE_UNITS = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
_COUNT_UNITS = ("", "k", "M", "B", "T")


def format_bytes(size: int) -> str:
    """Return ``size`` in binary units with two or three digits: ``1.5 KiB``, ``10 GiB``."""
    if size < 1024:
        return f"{size} B"
    value, unit = _scale(size / 1024, 1024, 1, len(_BYTE_UNITS) - 1)
    return f"{_digits(value)} {_BYTE_UNITS[unit]}"


def format_count(count: int) -> str:
    """Return ``count`` with about three digits: ``999``, ``1.2k``, ``12M``."""
    if count < 1000:
        return str(count)
    value, unit = _scale(count / 1000, 1000, 1, len(_COUNT_UNITS) - 1)
    return f"{_digits(value)}{_COUNT_UNITS[unit]}"


def format_rate(rows_per_second: float) -> str:
    """Return a row rate such as ``480k rows/s``."""
    return f"{format_count(int(rows_per_second))} rows/s"


def format_duration(seconds: float) -> str:
    """Return ``seconds`` as ``m:ss`` (``h:mm:ss`` from an hour on); nonsense reads ``0:00``."""
    if math.isnan(seconds) or seconds < 0:
        return "0:00"
    hours, rest = divmod(int(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def _scale(value: float, base: int, unit: int, last_unit: int) -> tuple[float, int]:
    """Move to the next unit while the rounded value would print as a full ``base``."""
    while round(value, 1) >= base and unit < last_unit:
        value /= base
        unit += 1
    return value, unit


def _digits(value: float) -> str:
    return f"{value:.1f}" if round(value, 1) < 10 else f"{value:.0f}"
