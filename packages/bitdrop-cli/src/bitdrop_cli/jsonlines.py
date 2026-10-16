"""The JSON-lines display: one JSON object per line on standard output, for programs to read.

Events: ``plan`` once, ``progress`` about once a second and once more at the end, ``chunk_failed``
for each failed attempt, and finally one of ``report``, ``stopped`` (the job was cancelled), or
``error``. Log lines stay on standard error. Nothing in the stream carries a URL.
"""

import asyncio
import json
import logging
import sys
from typing import Any, TextIO

from bitdrop_core.progress import ChunkFailed, PlanReady, Snapshot, TransferEvent
from bitdrop_core.tracker import JobTracker
from bitdrop_core.transfer import TransferReport
from bitdrop_core.ui import UiContext

__all__ = ["JsonLinesUi", "create_ui"]

DEFAULT_INTERVAL_SECONDS = 1.0


class JsonLinesUi:
    """Writes progress as JSON lines."""

    def __init__(
        self,
        context: UiContext,
        *,
        out: TextIO,
        interval: float = DEFAULT_INTERVAL_SECONDS,
    ) -> None:
        """Write to ``out``, a progress event at most every ``interval`` seconds of job time."""
        self._context = context
        self._out = out
        self._interval = interval
        self._tracker = JobTracker()
        self._last_written: float | None = None

    def __call__(self, event: TransferEvent) -> None:
        """Take one progress event."""
        self._tracker.apply(event)
        if isinstance(event, PlanReady):
            self._write(_plan(event))
        elif isinstance(event, ChunkFailed):
            self._write(
                {
                    "event": "chunk_failed",
                    "chunk_id": event.chunk_id,
                    "stage": str(event.stage),
                    "worker": event.worker,
                    "reason": event.reason,
                    "failures": event.failures,
                    "final": event.final,
                }
            )
        elif isinstance(event, Snapshot) and self._due(event.elapsed_seconds):
            self._last_written = event.elapsed_seconds
            self._write(self._progress(event))

    def log_handler(self) -> logging.Handler | None:
        """Use the default handler: log lines go to standard error, apart from the JSON."""
        return None

    async def present(self, job: asyncio.Task[TransferReport]) -> None:
        """Wait for the job and write how it ended."""
        await asyncio.wait({job})
        job_id = self._context.job_id
        if job.cancelled():
            self._write({"event": "stopped", "job_id": job_id})
        elif (error := job.exception()) is not None:
            self._write({"event": "error", "job_id": job_id, "message": str(error)})
        else:
            self._write(_report(job.result()))

    def _due(self, elapsed: float) -> bool:
        if self._tracker.is_finished or self._last_written is None:
            return True
        return elapsed - self._last_written >= self._interval

    def _progress(self, snapshot: Snapshot) -> dict[str, Any]:
        tracker = self._tracker
        eta = tracker.eta_seconds
        return {
            "event": "progress",
            "elapsed_seconds": snapshot.elapsed_seconds,
            "chunks": {str(state): count for state, count in snapshot.counts.items()},
            "rows_extracted": snapshot.rows_extracted,
            "rows_loaded": snapshot.rows_loaded,
            "bytes_staged": snapshot.bytes_staged,
            "staging_used_bytes": snapshot.staging_used_bytes,
            "staging_max_bytes": snapshot.staging_max_bytes,
            "extract_rows_per_second": round(tracker.extract_rate, 3),
            "load_rows_per_second": round(tracker.load_rate, 3),
            "eta_seconds": None if eta is None else round(eta, 3),
        }

    def _write(self, record: dict[str, Any]) -> None:
        print(json.dumps(record, separators=(",", ":")), file=self._out, flush=True)


def _plan(plan: PlanReady) -> dict[str, Any]:
    return {
        "event": "plan",
        "job_id": plan.job_id,
        "source_table": plan.source_table,
        "destination_table": plan.destination_table,
        "chunks": plan.chunks_total,
        "estimated_rows": plan.estimated_rows,
        "estimated_row_bytes": plan.estimated_row_bytes,
        "columns": [{"name": name, "type": kind} for name, kind in plan.columns],
        "notes": list(plan.notes),
        "extractors": plan.extractors,
        "loaders": plan.loaders,
    }


def _report(report: TransferReport) -> dict[str, Any]:
    return {
        "event": "report",
        "job_id": report.job_id,
        "succeeded": report.succeeded,
        "chunks_total": report.chunks_total,
        "chunks_loaded": report.chunks_loaded,
        "chunks_failed": report.chunks_failed,
        "rows_extracted": report.rows_extracted,
        "rows_loaded": report.rows_loaded,
        "duplicates_skipped": report.duplicates_skipped,
        "bytes_staged": report.bytes_staged,
        "duration_seconds": round(report.duration_seconds, 3),
        "failures": [
            {"chunk_id": r.chunk_id, "failures": r.failures, "last_error": r.last_error}
            for r in report.failures
        ],
    }


def create_ui(context: UiContext) -> JsonLinesUi:
    """Build the display for ``--ui json``. This is the plugin entry point."""
    return JsonLinesUi(context, out=sys.stdout)
