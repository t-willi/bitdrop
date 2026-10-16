"""The end-of-run summary every display prints into the terminal's scrollback."""

from bitdrop_core.humanize import format_bytes, format_duration, format_rate
from bitdrop_core.transfer import TransferReport

__all__ = ["summary_lines"]


def summary_lines(report: TransferReport) -> list[str]:
    """Describe the finished job: how it went, the numbers, and every chunk that gave up."""
    if report.succeeded:
        lines = [f"{report.job_id}: all {report.chunks_total} chunks loaded"]
    else:
        lines = [
            f"{report.job_id}: {report.chunks_loaded} of {report.chunks_total} chunks loaded, "
            f"{report.chunks_failed} FAILED"
        ]
    throughput = report.rows_loaded / report.duration_seconds if report.duration_seconds else 0
    lines += [
        f"  rows loaded   {report.rows_loaded:,}",
        f"  duration      {_duration(report.duration_seconds)}",
        f"  throughput    {format_rate(throughput)}",
        f"  staged        {format_bytes(report.bytes_staged)}",
    ]
    if report.duplicates_skipped:
        lines.append(
            f"  {report.duplicates_skipped} chunks were already loaded "
            "(a repeat after a crash or a re-run)"
        )
    if report.failures:
        lines.append("failed chunks:")
        lines += [
            f"  {record.chunk_id}  {record.failures} failures  {record.last_error}"
            for record in report.failures
        ]
    return lines


def _duration(seconds: float) -> str:
    """Seconds with a decimal below a minute (a quick job should not read ``0:00``), else m:ss."""
    return f"{seconds:.1f} s" if seconds < 60 else format_duration(seconds)
