"""The plain display: a progress line every few seconds, then a summary.

Made for logs, CI, containers, and any terminal that is not a TUI. Progress goes to standard
error so that standard output carries only the result; the runner's own log lines (failed
attempts, notes from the source) share standard error and are not repeated here.
"""

import asyncio
import logging
import sys
from typing import TextIO

from bitdrop_core.humanize import format_bytes, format_count, format_duration, format_rate
from bitdrop_core.progress import PlanReady, Snapshot, TransferEvent
from bitdrop_core.summary import summary_lines
from bitdrop_core.tracker import JobTracker
from bitdrop_core.transfer import TransferReport
from bitdrop_core.ui import UiContext

__all__ = ["PlainUi", "create_ui", "progress_line"]

DEFAULT_INTERVAL_SECONDS = 2.0


def progress_line(tracker: JobTracker) -> str:
    """Describe where the job stands in one line."""
    parts = [
        format_duration(tracker.elapsed_seconds).rjust(7),
        f"{tracker.fraction_done * 100:3.0f}%",
        f"{tracker.chunks_loaded}/{tracker.chunks_total} chunks",
        f"{format_count(tracker.rows_loaded)} rows loaded",
        f"in {format_rate(tracker.extract_rate)}, out {format_rate(tracker.load_rate)}",
    ]
    if tracker.staging_used is not None:
        staging = f"staging {format_bytes(tracker.staging_used)}"
        if tracker.staging_capacity:
            staging += f" / {format_bytes(tracker.staging_capacity)}"
        parts.append(staging)
    if not tracker.is_finished and tracker.eta_seconds is not None:
        parts.append(f"eta {format_duration(tracker.eta_seconds)}")
    if tracker.chunks_failed:
        parts.append(f"{tracker.chunks_failed} failed")
    return "  ".join(parts)


class PlainUi:
    """Prints the plan, a throttled progress line, and the summary."""

    def __init__(
        self,
        context: UiContext,
        *,
        out: TextIO,
        err: TextIO,
        interval: float = DEFAULT_INTERVAL_SECONDS,
    ) -> None:
        """Write the summary to ``out`` and progress to ``err``, at most every ``interval`` s."""
        self._context = context
        self._out = out
        self._err = err
        self._interval = interval
        self._tracker = JobTracker()
        self._last_printed: float | None = None

    def __call__(self, event: TransferEvent) -> None:
        """Take one progress event."""
        self._tracker.apply(event)
        if isinstance(event, PlanReady):
            rows = ""
            if event.estimated_rows is not None:
                rows = f", about {format_count(event.estimated_rows)} rows"
            print(
                f"{event.job_id}: {event.source_table} -> {event.destination_table} "
                f"({event.chunks_total} chunks{rows}; "
                f"{event.extractors} extractors, {event.loaders} loaders)",
                file=self._err,
                flush=True,
            )
        elif isinstance(event, Snapshot) and self._due(event.elapsed_seconds):
            self._last_printed = event.elapsed_seconds
            print(progress_line(self._tracker), file=self._err, flush=True)

    def log_handler(self) -> logging.Handler | None:
        """Use the default handler: log lines share standard error with the progress lines."""
        return None

    async def present(self, job: asyncio.Task[TransferReport]) -> None:
        """Wait for the job, then print the summary if it ended with a report."""
        await asyncio.wait({job})
        if job.cancelled() or job.exception() is not None:
            return
        for line in summary_lines(job.result()):
            print(line, file=self._out, flush=True)

    def _due(self, elapsed: float) -> bool:
        if self._tracker.is_finished or self._last_printed is None:
            return True
        return elapsed - self._last_printed >= self._interval


def create_ui(context: UiContext) -> PlainUi:
    """Build the display for ``--ui plain``. This is the plugin entry point."""
    return PlainUi(context, out=sys.stdout, err=sys.stderr)
