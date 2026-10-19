"""Doing the work of the commands: open the parts, run a job with a display, stop cleanly.

Nothing here knows a concrete adapter. Components come from the registry by URL scheme and the
display by name, so the same code runs any combination that is installed.
"""

import asyncio
import contextlib
import logging
import os
import re
import signal
import socket
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from enum import IntEnum
from typing import Protocol

from bitdrop_api.errors import BitdropError
from bitdrop_api.metadata_store import ChunkState
from bitdrop_cli.jobfile import JobFile
from bitdrop_core.queueflow import (
    ConsumerStats,
    DestinationWorkerConfig,
    PlannerConfig,
    PlannerReport,
    QueueflowStatus,
    SourceWorkerConfig,
    queueflow_status,
    run_destination_worker,
    run_planner,
    run_source_worker,
    wait_until_finished,
    work_is_drained,
)
from bitdrop_core.registry import (
    UnknownComponent,
    open_chunk_queue,
    open_chunk_store,
    open_destination,
    open_marker_store,
    open_metadata_store,
    open_source,
    open_ui,
)
from bitdrop_core.transfer import TransferReport, effective_chunk_bytes, run_transfer
from bitdrop_core.ui import UiContext

__all__ = [
    "ExitCode",
    "PlanSummary",
    "QueueWorkResult",
    "StatusSummary",
    "default_worker_name",
    "job_status",
    "plan_job",
    "queue_plan",
    "queue_state",
    "queue_work",
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
    wait: bool = False,
    verbosity: int = 0,
) -> ExitCode:
    """Run the job with the named display and return the exit code.

    With ``wait`` the display opens first and the job does nothing until the display calls
    ``UiContext.begin``. Only a display that offers that (the TUI) may be used with it, or the job
    would wait forever.

    Raises:
        BitdropError: a component refused or failed (the display has already been shut down).
        ValueError: a URL in the job is malformed.
    """
    config = job.transfer_config(job_id=job_id, extractors=extractors, loaders=loaders)
    go = asyncio.Event()
    if not wait:
        go.set()
    async with AsyncExitStack() as parts:
        source = _closing(parts, open_source(job.source.url))
        destination = _closing(parts, open_destination(job.destination.url))
        staging = _closing(parts, open_chunk_store(job.staging.url))
        metadata = _closing(parts, open_metadata_store(job.require_metadata().url))
        ui = open_ui(
            ui_name,
            UiContext(
                job_id=config.job_id,
                source_table=config.source_table,
                destination_table=config.destination_table,
                endpoints=job.endpoints(),
                begin=None if go.is_set() else go.set,
                extractors=config.extractors,
                loaders=config.loaders,
            ),
        )

        async def started() -> TransferReport:
            await go.wait()
            return await run_transfer(source, destination, staging, metadata, config, observer=ui)

        with _logging_to(ui.log_handler(), verbosity):
            task = asyncio.create_task(started())
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
            if not go.is_set():
                print("not started: nothing was moved.", file=sys.stderr)
                return ExitCode.INTERRUPTED
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
        metadata = _closing(parts, open_metadata_store(job.require_metadata().url))
        counts = dict(await metadata.counts(job_id))
    if not any(counts.values()):
        raise BitdropError(f"no chunks are registered for job {job_id!r} in this metadata store")
    return StatusSummary(job_id=job_id, counts=counts)


# -- the queue workflow ---------------------------------------------------------------------------


async def queue_plan(job: JobFile, *, republish: bool = False) -> PlannerReport:
    """Plan the job's table and publish its chunk ids to the work queue (the planner container).

    Raises:
        BitdropError: a component refused (unknown table, ...) or the job was chunked differently.
    """
    config = job.transfer_config()
    async with AsyncExitStack() as parts:
        source = _closing(parts, open_source(job.source.url))
        destination = _closing(parts, open_destination(job.destination.url))
        staging = _closing(parts, open_chunk_store(job.staging.url))
        work = _closing(parts, open_chunk_queue(job.require_queues().work))
        markers = _closing(parts, open_marker_store(job.require_markers().url))
        return await run_planner(
            source,
            destination,
            work,
            markers,
            PlannerConfig(
                job_id=config.job_id,
                source_table=config.source_table,
                destination_table=config.destination_table,
                target_chunk_bytes=effective_chunk_bytes(config, staging),
                split_column=config.split_column,
            ),
            republish=republish,
        )


@dataclass(frozen=True, slots=True)
class QueueWorkResult:
    """How a container's run ended."""

    stats: ConsumerStats
    gave_up: QueueflowStatus | None
    """Set when ``give_up_after`` ran out before the container's end condition was met."""


def default_worker_name(role: str) -> str:
    """A name for this container that no other container shares: role, host, and process id."""
    host = re.sub(r"[^A-Za-z0-9.-]", "-", socket.gethostname()) or "host"
    return f"{role}-{host}-{os.getpid()}"[:100]


async def queue_work(
    job: JobFile,
    role: str,
    *,
    name: str | None = None,
    stop_when_finished: bool = False,
    give_up_after: float | None = None,
) -> QueueWorkResult:
    """Run one container of the queue workflow until it is stopped, and return what it did.

    ``role`` is ``source`` or ``destination``. SIGINT and SIGTERM ask it to finish the message in
    hand and leave; a second SIGINT cancels the message, which is handed back to the queue.
    With ``stop_when_finished`` a source leaves once the plan is complete and no chunk is left to
    extract, a destination once the whole job is over; ``give_up_after`` bounds the wait for that.

    Raises:
        BitdropError: a component refused, or the job file lacks a section the role needs.
        ValueError: ``role`` is neither ``source`` nor ``destination``.
    """
    if role not in ("source", "destination"):
        raise ValueError(f"role must be source or destination, got {role!r}")
    name = name or default_worker_name(role)
    consumer = job.consumer_config()
    queues = job.require_queues()
    job_id = job.job.id
    stop = asyncio.Event()
    async with AsyncExitStack() as parts:
        staging = _closing(parts, open_chunk_store(job.staging.url))
        markers = _closing(parts, open_marker_store(job.require_markers().url))
        work = _closing(parts, open_chunk_queue(queues.work))
        files = _closing(parts, open_chunk_queue(queues.files))
        if role == "source":
            source = _closing(parts, open_source(job.source.url))
            worker = asyncio.create_task(
                run_source_worker(
                    source,
                    staging,
                    work,
                    files,
                    markers,
                    SourceWorkerConfig(job_id, consumer),
                    stop,
                    name=name,
                )
            )
        else:
            destination = _closing(parts, open_destination(job.destination.url))
            worker = asyncio.create_task(
                run_destination_worker(
                    destination,
                    staging,
                    files,
                    DestinationWorkerConfig(job_id, job.destination.table, consumer),
                    stop,
                    name=name,
                )
            )

        async def is_over() -> bool:
            if role == "source":
                return await work_is_drained(markers, work, job_id)
            return (await queueflow_status(markers, work, files, job_id)).state.is_over

        gave_up: list[QueueflowStatus] = []

        async def watch() -> None:
            deadline = (
                None if give_up_after is None else asyncio.get_running_loop().time() + give_up_after
            )
            while not stop.is_set():
                if await is_over():
                    stop.set()
                    return
                if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                    gave_up.append(await queueflow_status(markers, work, files, job_id))
                    stop.set()
                    return
                await asyncio.sleep(consumer.poll_interval)

        watcher = asyncio.create_task(watch()) if stop_when_finished else None
        try:
            with _stopping_gracefully(stop, worker):
                stats = await worker
        finally:
            if watcher is not None:
                watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watcher
    return QueueWorkResult(stats, gave_up[0] if gave_up else None)


async def queue_state(
    job: JobFile, *, wait: bool = False, give_up_after: float | None = None
) -> QueueflowStatus:
    """Read the job's state from the markers and queues; with ``wait``, until it is over.

    Raises:
        BitdropError: the job file lacks a section, or a component refused.
        TimeoutError: ``wait`` was set and the job was not over after ``give_up_after`` seconds.
    """
    async with AsyncExitStack() as parts:
        queues = job.require_queues()
        markers = _closing(parts, open_marker_store(job.require_markers().url))
        work = _closing(parts, open_chunk_queue(queues.work))
        files = _closing(parts, open_chunk_queue(queues.files))
        if wait:
            return await wait_until_finished(
                markers, work, files, job.job.id, give_up_after=give_up_after
            )
        return await queueflow_status(markers, work, files, job.job.id)


@contextlib.contextmanager
def _stopping_gracefully(stop: asyncio.Event, task: asyncio.Task[object]) -> Iterator[None]:
    """On SIGINT or SIGTERM ask for a stop after the message in hand; a second SIGINT cancels it."""
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    signals_seen = 0

    def ask(received: signal.Signals) -> None:
        nonlocal signals_seen
        signals_seen += 1
        if signals_seen == 1:
            stop.set()
        else:
            task.cancel()  # hands the message in hand back to the queue

    for received in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(received, ask, received)
        except NotImplementedError, RuntimeError, ValueError:
            continue
        installed.append(received)
    try:
        yield
    finally:
        for received in installed:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(received)


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
