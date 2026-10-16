"""Doing the work of the commands: open the parts, run a job with a display, stop cleanly.

Nothing here knows a concrete adapter. Components come from the registry by URL scheme and the
display by name, so the same code runs any combination that is installed.
"""

import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from enum import IntEnum
from typing import Protocol

from bitdrop_api.errors import BitdropError
from bitdrop_api.metadata_store import ChunkState
from bitdrop_cli.jobfile import JobFile
from bitdrop_core.registry import (
    UnknownComponent,
    open_chunk_store,
    open_destination,
    open_metadata_store,
    open_source,
    open_ui,
)
from bitdrop_core.transfer import effective_chunk_bytes, run_transfer
from bitdrop_core.ui import UiContext

__all__ = [
    "ExitCode",
    "PlanSummary",
    "StatusSummary",
    "job_status",
    "plan_job",
    "resolve_ui",
    "run_job",
]

AUTO = "auto"
TUI = "tui"
PLAIN = "plain"
SAMPLE_CHUNKS = 3
"""How many chunk descriptions ``plan`` shows."""


class ExitCode(IntEnum):
    """What the process returns, so scripts and schedulers can tell the outcomes apart."""

    OK = 0
    """Every chunk was loaded."""

    CHUNKS_FAILED = 1
    """The job ended, but some chunks gave up."""

    BAD_CONFIGURATION = 2
    """The command line, the job file, or a URL is wrong. Nothing was done."""

    REFUSED = 3
    """A component refused or could not be reached (unknown table, unsupported type, ...)."""

    UNEXPECTED = 70
    """A bug."""

    INTERRUPTED = 130
    """Stopped on purpose (Ctrl-C, SIGTERM, 'q'); unfinished chunks were handed back."""


def resolve_ui(requested: str, *, interactive: bool, available: Sequence[str]) -> str:
    """Turn ``--ui`` into the name of an installed display.

    ``auto`` means the TUI on an interactive terminal if it is installed, plain text otherwise.

    Raises:
        UnknownComponent: ``requested`` is not ``auto`` and not installed.
        ValueError: the TUI was asked for without an interactive terminal.
    """
    if requested == AUTO:
        return TUI if interactive and TUI in available else PLAIN
    if requested not in available:
        raise UnknownComponent("progress UI", requested, sorted(available))
    if requested == TUI and not interactive:
        raise ValueError("the TUI needs an interactive terminal; use --ui plain or --ui json here")
    return requested


async def run_job(
    job: JobFile,
    *,
    ui_name: str,
    job_id: str | None = None,
    extractors: int | None = None,
    loaders: int | None = None,
    verbosity: int = 0,
) -> ExitCode:
    """Run the job with the named display and return the exit code.

    Raises:
        BitdropError: a component refused or failed (the display has already been shut down).
        ValueError: a URL in the job is malformed.
    """
    config = job.transfer_config(job_id=job_id, extractors=extractors, loaders=loaders)
    async with AsyncExitStack() as parts:
        source = _closing(parts, open_source(job.source.url))
        destination = _closing(parts, open_destination(job.destination.url))
        staging = _closing(parts, open_chunk_store(job.staging.url))
        metadata = _closing(parts, open_metadata_store(job.metadata.url))
        ui = open_ui(
            ui_name,
            UiContext(
                job_id=config.job_id,
                source_table=config.source_table,
                destination_table=config.destination_table,
                endpoints=job.endpoints(),
            ),
        )

        with _logging_to(ui.log_handler(), verbosity):
            task = asyncio.create_task(
                run_transfer(source, destination, staging, metadata, config, observer=ui)
            )
            with _stopping_on_signals(task):
                try:
                    await ui.present(task)
                finally:
                    if not task.done():
                        task.cancel()
                    await asyncio.wait({task})

        try:
            report = task.result()
        except asyncio.CancelledError:
            print(
                "stopped: unfinished chunks were handed back. Run the same command again to "
                "resume (a metadata store that survives the process is needed).",
                file=sys.stderr,
            )
            return ExitCode.INTERRUPTED
    return ExitCode.OK if report.succeeded else ExitCode.CHUNKS_FAILED


@dataclass(frozen=True, slots=True)
class PlanSummary:
    """What a run would do, as found by planning the source table."""

    job_id: str
    source_table: str
    destination_table: str
    columns: tuple[tuple[str, str], ...]
    chunks: int
    chunk_bytes: int
    """The chunk size planning aimed for, after the staging store's value limit."""

    estimated_rows: int | None
    estimated_row_bytes: int | None
    notes: tuple[str, ...]
    sample_chunks: tuple[str, ...]
    """Descriptions of the first few chunks, e.g. which key ranges they cover."""


async def plan_job(job: JobFile) -> PlanSummary:
    """Plan the source table and describe the result. The destination is not touched."""
    config = job.transfer_config()
    async with AsyncExitStack() as parts:
        source = _closing(parts, open_source(job.source.url))
        staging = _closing(parts, open_chunk_store(job.staging.url))
        target = effective_chunk_bytes(config, staging)
        plan = await source.plan(
            config.source_table, target_chunk_bytes=target, split_column=config.split_column
        )
    return PlanSummary(
        job_id=config.job_id,
        source_table=config.source_table,
        destination_table=config.destination_table,
        columns=tuple((field.name, str(field.type)) for field in plan.schema),
        chunks=len(plan.chunks),
        chunk_bytes=target,
        estimated_rows=plan.estimated_rows,
        estimated_row_bytes=plan.estimated_row_bytes,
        notes=plan.notes,
        sample_chunks=tuple(chunk.description for chunk in plan.chunks[:SAMPLE_CHUNKS]),
    )


@dataclass(frozen=True, slots=True)
class StatusSummary:
    """Where a job stands according to the metadata store."""

    job_id: str
    counts: Mapping[ChunkState, int]

    @property
    def chunks_total(self) -> int:
        """How many chunks the job has."""
        return sum(self.counts.values())


async def job_status(job: JobFile) -> StatusSummary:
    """Read the job's chunk counts from its metadata store.

    Raises:
        BitdropError: the store knows no chunks of this job.
    """
    job_id = job.job.id
    async with AsyncExitStack() as parts:
        metadata = _closing(parts, open_metadata_store(job.metadata.url))
        counts = dict(await metadata.counts(job_id))
    if not any(counts.values()):
        raise BitdropError(f"no chunks are registered for job {job_id!r} in this metadata store")
    return StatusSummary(job_id=job_id, counts=counts)


# -- helpers --------------------------------------------------------------------------------------


class _Closable(Protocol):
    async def aclose(self) -> None: ...


def _closing[T: _Closable](stack: AsyncExitStack, component: T) -> T:
    """Register ``component.aclose`` to run when the stack unwinds; closing errors are ignored."""

    async def close() -> None:
        with contextlib.suppress(Exception):
            await component.aclose()

    stack.push_async_callback(close)
    return component


@contextlib.contextmanager
def _logging_to(handler: logging.Handler | None, verbosity: int) -> Iterator[None]:
    """Send the ``bitdrop`` log lines to the display's handler or, by default, to standard error."""
    logger = logging.getLogger("bitdrop")
    chosen = handler or logging.StreamHandler(sys.stderr)
    chosen.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    level = logger.level
    logger.setLevel((logging.WARNING, logging.INFO, logging.DEBUG)[min(verbosity, 2)])
    logger.addHandler(chosen)
    try:
        yield
    finally:
        logger.removeHandler(chosen)
        logger.setLevel(level)


@contextlib.contextmanager
def _stopping_on_signals(task: asyncio.Task[object]) -> Iterator[None]:
    """Cancel ``task`` on SIGINT or SIGTERM. A second Ctrl-C interrupts at once."""
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    def stop(received: signal.Signals) -> None:
        task.cancel()
        if received is signal.SIGINT:
            loop.remove_signal_handler(signal.SIGINT)  # back to the default: Ctrl-C again quits

    for received in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(received, stop, received)
        except NotImplementedError, RuntimeError, ValueError:
            continue  # not the main thread, or a platform without signal handlers
        installed.append(received)
    try:
        yield
    finally:
        for received in installed:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(received)
