"""What the TUI draws: pure functions from the tracker's state to styled text.

Nothing here knows about Textual. Every function takes the :class:`~bitdrop_core.tracker.JobTracker`
(or plain numbers) and returns a Rich ``Text``, so the exact picture is easy to test and the widgets
that show it stay trivial.
"""

import math
from collections.abc import Sequence

from rich.text import Text

from bitdrop_api.metadata_store import ChunkState, Stage
from bitdrop_core.humanize import format_bytes, format_count, format_duration, format_rate
from bitdrop_core.tracker import JobTracker

__all__ = [
    "STATE_STYLE",
    "activity_text",
    "bar",
    "bucket_state",
    "chunk_map",
    "legend",
    "overall_text",
    "spark",
    "staging_text",
    "throughput_text",
    "workers_text",
]

STATE_STYLE: dict[ChunkState, str] = {
    ChunkState.PENDING: "dim",
    ChunkState.EXTRACTING: "yellow",
    ChunkState.EXTRACTED: "cyan",
    ChunkState.LOADING: "bright_blue",
    ChunkState.LOADED: "green",
    ChunkState.FAILED: "bold red",
}
"""How a chunk in each state is drawn (the same colours in the map and its legend)."""

_BLOCKS = "▁▂▃▄▅▆▇█"
_CELL = "■"


def bar(fraction: float, width: int, *, style: str = "green") -> Text:
    """Draw a bar ``width`` cells wide: never empty once started, never full before the end."""
    if width <= 0:
        return Text()
    fraction = min(max(fraction, 0.0), 1.0)
    filled = round(fraction * width)
    if fraction > 0 and filled == 0:
        filled = 1
    if fraction < 1 and filled == width:
        filled = width - 1
    text = Text()
    if filled:
        text.append("█" * filled, style=style)
    if width - filled:
        text.append("░" * (width - filled), style="dim")
    return text


def spark(values: Sequence[float], width: int) -> Text:
    """Draw the latest ``width`` values as rising blocks, scaled to the largest, right-aligned."""
    recent = list(values)[-width:] if width > 0 else []
    peak = max(recent, default=0.0)
    cells = (
        _BLOCKS[0] if peak <= 0 else _BLOCKS[min(len(_BLOCKS) - 1, int(v * 7 // peak))]
        for v in recent
    )
    return Text(" " * (max(width, 0) - len(recent)) + "".join(cells))


def bucket_state(states: Sequence[ChunkState]) -> ChunkState:
    """Pick the state that a group of chunks sharing one map cell should show.

    Trouble first (any failure), then work in progress, then the common state if there is one; a mix
    of finished and waiting chunks reads as "staged", meaning partly through.
    """
    present = set(states)
    for state in (ChunkState.FAILED, ChunkState.LOADING, ChunkState.EXTRACTING):
        if state in present:
            return state
    if len(present) == 1:
        return next(iter(present))
    return ChunkState.EXTRACTED


def chunk_map(
    states: Sequence[ChunkState], *, width: int, max_rows: int, gap: int = 0
) -> tuple[Text, int]:
    """Draw one cell per chunk in rows at most ``width`` wide; return the picture and chunks/cell.

    ``gap`` blank columns separate neighbouring cells. When the chunks do not fit in ``max_rows``
    rows, neighbours share a cell (so a job of a hundred thousand chunks still fits a screen).
    """
    count = len(states)
    per_row = (width + gap) // (1 + gap)
    if count == 0 or per_row <= 0 or max_rows <= 0:
        return Text(), 1
    per_cell = max(1, math.ceil(count / (per_row * max_rows)))
    cells = math.ceil(count / per_cell)
    text = Text()
    for index in range(cells):
        group = states[index * per_cell : (index + 1) * per_cell]
        text.append(_CELL, style=STATE_STYLE[bucket_state(group)])
        if (index + 1) % per_row == 0:
            if index + 1 < cells:
                text.append("\n")
        elif index + 1 < cells and gap:
            text.append(" " * gap)
    return text, per_cell


def legend() -> Text:
    """Name the colours of the chunk map."""
    text = Text()
    for state in ChunkState:
        text.append(_CELL, style=STATE_STYLE[state])
        text.append(f" {state}  ")
    return text


def overall_text(tracker: JobTracker, *, width: int) -> Text:
    """The overall progress: a bar with percentage and counts, then time, estimate, and rows."""
    done = tracker.fraction_done
    tail = f"  {done * 100:.0f}%  {tracker.chunks_loaded}/{tracker.chunks_total} chunks"
    text = bar(done, width - len(tail))
    text.append(tail)

    eta = tracker.eta_seconds
    second = (
        f"elapsed {format_duration(tracker.elapsed_seconds)}   "
        f"eta {'--' if eta is None else format_duration(eta)}   "
        f"{format_count(tracker.rows_loaded)} rows loaded"
    )
    text.append("\n" + second)
    if tracker.chunks_failed:
        text.append(f"   {tracker.chunks_failed} failed", style=STATE_STYLE[ChunkState.FAILED])
    return text


def throughput_text(tracker: JobTracker, *, width: int) -> Text:
    """Rows per second leaving the source and arriving at the destination, with recent history."""
    label, rate_width = 8, 14
    graph = max(0, width - label - rate_width - 2)
    text = Text()
    for name, rate, history, style in (
        ("extract", tracker.extract_rate, tracker.extract_history, "yellow"),
        ("load", tracker.load_rate, tracker.load_history, "green"),
    ):
        if text.plain:
            text.append("\n")
        text.append(f"{name:<{label}}")
        text.append(f"{format_rate(rate):>{rate_width}}  ")
        line = spark(history, graph)
        line.stylize(style)
        text.append_text(line)
    return text


def staging_text(tracker: JobTracker, *, width: int) -> Text:
    """How full the staging tier is and how much has passed through it."""
    used, capacity = tracker.staging_used, tracker.staging_capacity
    fraction = tracker.staging_fraction
    text = Text()
    if used is None:
        text.append("staging size not reported", style="dim")
    elif capacity and fraction is not None:
        text.append_text(bar(fraction, width - 6, style="cyan"))
        text.append(f"  {fraction * 100:.0f}%")
        text.append(f"\n{format_bytes(used)} / {format_bytes(capacity)}")
    else:
        text.append(f"{format_bytes(used)} held")
    text.append(f"\n{format_bytes(tracker.bytes_staged)} staged so far")
    if tracker.duplicates_skipped:
        text.append(f"\n{tracker.duplicates_skipped} repeated chunks", style="yellow")
    return text


def activity_text(tracker: JobTracker, *, extractors: int, loaders: int) -> Text:
    """How many workers are extracting and loading right now, and how many containers run them.

    ``extractors`` and ``loaders`` are the configured numbers. Every worker runs inside the one
    process that runs the job, so the container count is 1 until the role images exist.
    """
    text = Text()
    for name, stage, configured, style in (
        ("extracting", Stage.EXTRACT, extractors, "yellow"),
        ("loading", Stage.LOAD, loaders, "bright_blue"),
    ):
        text.append(f"{name:<12}")
        text.append(f"{tracker.busy(stage)}", style=style)
        text.append(f" of {configured} busy\n")
    text.append(f"{'containers':<12}1", style="bold")
    text.append("  every worker runs in this process", style="dim")
    return text


def workers_text(tracker: JobTracker) -> Text:
    """One line per worker: what it holds and for how long, or that it is idle."""
    if not tracker.workers:
        return Text("waiting for the first chunk", style="dim")
    lines: list[Text] = []
    for name, activity in tracker.workers.items():
        line = Text(f"{name:<13}")
        if activity is None:
            line.append("idle", style="dim")
        else:
            held = max(0, int(tracker.elapsed_seconds - activity.since_seconds))
            style = "yellow" if activity.stage is Stage.EXTRACT else "bright_blue"
            line.append(f"{activity.stage!s:<9}", style=style)
            line.append(f"{activity.chunk_id:<9}{held}s")
        lines.append(line)
    return Text("\n").join(lines)
