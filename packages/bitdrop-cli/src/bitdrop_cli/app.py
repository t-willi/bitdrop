"""The ``bitdrop`` commands: ``run``, ``plan``, ``status``, and ``components``."""

import asyncio
import json
import sys
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated, Any, NoReturn

import typer

from bitdrop_api.errors import BitdropError
from bitdrop_api.metadata_store import ChunkState
from bitdrop_cli.execute import (
    AUTO,
    TUI,
    ExitCode,
    PlanSummary,
    StatusSummary,
    default_worker_name,
    job_status,
    plan_job,
    queue_plan,
    queue_state,
    queue_work,
    resolve_ui,
    run_job,
)
from bitdrop_cli.jobfile import JobFile, JobFileError, load_job
from bitdrop_core.humanize import format_bytes, format_count
from bitdrop_core.queueflow import ConsumerStats, JobState, PlannerReport, QueueflowStatus
from bitdrop_core.registry import (
    AmbiguousComponent,
    InvalidComponent,
    UnknownComponent,
    available_chunk_queues,
    available_chunk_stores,
    available_destinations,
    available_marker_stores,
    available_metadata_stores,
    available_sources,
    available_uis,
)

__all__ = ["app", "plan_lines", "status_lines"]

app = typer.Typer(
    name="bitdrop",
    help="Move database tables in parallel through an elastic in-RAM staging tier.",
    no_args_is_help=True,
    add_completion=False,
    # Rich tracebacks print local variables, and a local may hold a connection URL with a password.
    pretty_exceptions_enable=False,
)

JobFileArgument = Annotated[
    Path,
    typer.Argument(
        metavar="JOB_FILE",
        help="The TOML job file.",
        exists=True,
        dir_okay=False,
        readable=True,
        show_default=False,
    ),
]


def _show_version(show: bool) -> None:
    if show:
        try:
            current = version("bitdrop-cli")
        except PackageNotFoundError:  # pragma: no cover  (running from a source tree)
            current = "unknown"
        typer.echo(f"bitdrop {current}")
        raise typer.Exit


@app.callback()
def _root(
    show_version: Annotated[
        bool,
        typer.Option(
            "--version", callback=_show_version, is_eager=True, help="Show the version and exit."
        ),
    ] = False,
) -> None:
    """Move database tables in parallel through an elastic in-RAM staging tier."""


@app.command()
def run(
    job_file: JobFileArgument,
    ui: Annotated[
        str,
        typer.Option(help="How to show progress: auto, plain, json, or tui if installed."),
    ] = AUTO,
    job_id: Annotated[
        str | None, typer.Option(help="Use this job id instead of the file's. Reusing one resumes.")
    ] = None,
    extractors: Annotated[
        int | None, typer.Option(min=1, help="Concurrent reads from the source.")
    ] = None,
    loaders: Annotated[
        int | None, typer.Option(min=1, help="Concurrent writes to the destination.")
    ] = None,
    wait: Annotated[
        bool,
        typer.Option("--wait", help="Open the tui first and start the load from it by pressing s."),
    ] = False,
    verbose: Annotated[
        int, typer.Option("--verbose", "-v", count=True, help="Log more (-v info, -vv debug).")
    ] = 0,
) -> None:
    """Run a job. Running the same job id again resumes it; stop it any time with Ctrl-C.

    Exit codes: 0 done, 1 some chunks failed, 2 bad configuration, 3 refused by a component,
    130 stopped on purpose.
    """
    with _exiting_on_errors(verbose):
        job = _load(job_file)
        name = resolve_ui(
            ui, interactive=sys.stdout.isatty() and sys.stdin.isatty(), available=available_uis()
        )
        if wait and name != TUI:
            raise ValueError("--wait needs the tui display, the only one that can start a job")
        code = asyncio.run(
            run_job(
                job,
                ui_name=name,
                job_id=job_id,
                extractors=extractors,
                loaders=loaders,
                wait=wait,
                verbosity=verbose,
            )
        )
    raise typer.Exit(code)


@app.command()
def plan(
    job_file: JobFileArgument,
    as_json: Annotated[bool, typer.Option("--json", help="Print one JSON document.")] = False,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    """Show how the source table would be cut into chunks. Moves no data."""
    with _exiting_on_errors(verbose):
        summary = asyncio.run(plan_job(_load(job_file)))
    if as_json:
        typer.echo(json.dumps(_plan_document(summary)))
    else:
        typer.echo("\n".join(plan_lines(summary)))


@app.command()
def status(
    job_file: JobFileArgument,
    as_json: Annotated[bool, typer.Option("--json", help="Print one JSON document.")] = False,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    """Show how far a job is, as recorded in its metadata store (whoever is running it)."""
    with _exiting_on_errors(verbose):
        summary = asyncio.run(job_status(_load(job_file)))
    if as_json:
        typer.echo(json.dumps(_status_document(summary)))
    else:
        typer.echo("\n".join(status_lines(summary)))


queue_app = typer.Typer(
    help="The queue workflow: a planner, source containers, and destination containers.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)
app.add_typer(queue_app, name="queue")

GiveUpAfter = Annotated[
    float | None,
    typer.Option(
        min=0.0, help="Give up after this many seconds (default: wait as long as needed)."
    ),
]
WorkerName = Annotated[
    str | None,
    typer.Option(help="Name of this container for the queue. The default is unique per process."),
]
StopWhenFinished = Annotated[
    bool,
    typer.Option(
        "--stop-when-finished",
        help="Leave when there is nothing left to do (source: all chunks extracted; "
        "destination: the whole job is over). Without it, run until stopped.",
    ),
]


@queue_app.command("plan")
def queue_plan_command(
    job_file: JobFileArgument,
    republish: Annotated[
        bool,
        typer.Option("--republish", help="Publish every chunk id again, for a lost work queue."),
    ] = False,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    """Plan the table and publish one chunk id per chunk. Run it once, before the containers."""
    with _exiting_on_errors(verbose):
        report = asyncio.run(queue_plan(_load(job_file), republish=republish))
    typer.echo(planner_line(report))


@queue_app.command("source")
def queue_source_command(
    job_file: JobFileArgument,
    name: WorkerName = None,
    stop_when_finished: StopWhenFinished = False,
    give_up_after: GiveUpAfter = None,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    """Run a source container: read chunks named in the work queue and stage them.

    Stop it with Ctrl-C or SIGTERM; it finishes the chunk in hand first.
    """
    _run_container(job_file, "source", name, stop_when_finished, give_up_after, verbose)


@queue_app.command("destination")
def queue_destination_command(
    job_file: JobFileArgument,
    name: WorkerName = None,
    stop_when_finished: StopWhenFinished = False,
    give_up_after: GiveUpAfter = None,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    """Run a destination container: load the staged chunks named in the file queue.

    Stop it with Ctrl-C or SIGTERM; it finishes the chunk in hand first.
    """
    _run_container(job_file, "destination", name, stop_when_finished, give_up_after, verbose)


@queue_app.command("status")
def queue_status_command(
    job_file: JobFileArgument,
    wait: Annotated[
        bool, typer.Option("--wait", help="Wait until the job is over (exit 1 if chunks failed).")
    ] = False,
    give_up_after: GiveUpAfter = None,
    as_json: Annotated[bool, typer.Option("--json", help="Print one JSON document.")] = False,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    """Show how far the job is, from the planner's markers and the two queues."""
    try:
        with _exiting_on_errors(verbose):
            status = asyncio.run(
                queue_state(_load(job_file), wait=wait, give_up_after=give_up_after)
            )
    except TimeoutError as error:
        _fail(str(error), ExitCode.CHUNKS_FAILED)
    typer.echo(
        json.dumps(_queue_status_document(status))
        if as_json
        else "\n".join(queue_status_lines(status))
    )
    if status.state is JobState.FINISHED_WITH_FAILURES:
        raise typer.Exit(ExitCode.CHUNKS_FAILED)


def _run_container(
    job_file: Path,
    role: str,
    name: str | None,
    stop_when_finished: bool,
    give_up_after: float | None,
    verbose: int,
) -> None:
    name = name or default_worker_name(role)
    with _exiting_on_errors(verbose):
        result = asyncio.run(
            queue_work(
                _load(job_file),
                role,
                name=name,
                stop_when_finished=stop_when_finished,
                give_up_after=give_up_after,
            )
        )
    typer.echo(worker_line(name, result.stats))
    if result.gave_up is not None:
        _fail(f"gave up waiting: the job was still {result.gave_up.state}", ExitCode.CHUNKS_FAILED)
    if result.stats.dead:
        raise typer.Exit(ExitCode.CHUNKS_FAILED)


@app.command()
def components() -> None:
    """List the connectors, stores, and displays that are installed."""
    rows = {
        "sources": available_sources(),
        "destinations": available_destinations(),
        "staging": available_chunk_stores(),
        "metadata": available_metadata_stores(),
        "queues": available_chunk_queues(),
        "markers": available_marker_stores(),
        "displays": available_uis(),
    }
    for kind, names in rows.items():
        typer.echo(f"{kind:<13} {', '.join(names) if names else '(none installed)'}")


# -- output ------------------------------------------------------------------------------------


def plan_lines(summary: PlanSummary) -> list[str]:
    """Describe a plan for a person."""
    lines = [f"{summary.job_id}: {summary.source_table} -> {summary.destination_table}"]
    width = max((len(name) for name, _ in summary.columns), default=0)
    lines.append(f"  columns   {len(summary.columns)}")
    lines += [f"    {name:<{width}}  {kind}" for name, kind in summary.columns]
    size = f"about {format_bytes(summary.chunk_bytes)} each"
    lines.append(f"  chunks    {summary.chunks} ({size})")
    if summary.estimated_rows is not None:
        rows = f"{summary.estimated_rows:,} rows"
        if summary.estimated_row_bytes:
            total = format_bytes(summary.estimated_rows * summary.estimated_row_bytes)
            rows += f", about {summary.estimated_row_bytes} B each, {total} in all"
        lines.append(f"  estimate  {rows}")
    for description in summary.sample_chunks:
        lines.append(f"  chunk     {description}")
    if summary.chunks > len(summary.sample_chunks):
        lines.append(f"            ... and {summary.chunks - len(summary.sample_chunks)} more")
    lines += [f"  note      {note}" for note in summary.notes]
    return lines


def status_lines(summary: StatusSummary) -> list[str]:
    """Describe a job's progress for a person."""
    loaded = summary.counts[ChunkState.LOADED]
    percent = loaded * 100 // summary.chunks_total
    states = ", ".join(f"{state} {summary.counts[state]}" for state in ChunkState)
    return [
        f"{summary.job_id}: {loaded} of {summary.chunks_total} chunks loaded ({percent}%)",
        f"  {states}",
    ]


def planner_line(report: PlannerReport) -> str:
    """Describe what the planner did."""
    rows = (
        ""
        if report.estimated_rows is None
        else f" (about {format_count(report.estimated_rows)} rows)"
    )
    if report.already_planned:
        return (
            f"{report.job_id}: already planned: {report.chunks_total} chunks{rows}; nothing was "
            "published (--republish publishes them again)"
        )
    return f"{report.job_id}: planned {report.chunks_total} chunks{rows} and published them"


def worker_line(name: str, stats: ConsumerStats) -> str:
    """Describe what a container did."""
    return (
        f"{name}: {stats.handled} handled, {stats.retried} retried, {stats.dead} set aside, "
        f"{stats.taken_over} taken over, {stats.deferred} deferred"
    )


def queue_status_lines(status: QueueflowStatus) -> list[str]:
    """Describe a queue job's progress for a person."""
    lines = [f"{status.job_id}: {status.state}"]
    if status.chunks_total is not None:
        total = status.chunks_total
        lines.append(
            f"  chunks      {total}: extracted {status.extracted} of {total}, "
            f"loaded {status.loaded} of {total}"
            + (f", {status.failed} set aside" if status.failed else "")
        )
    for label, stats in (("work queue", status.work), ("file queue", status.files)):
        lines.append(
            f"  {label:<11} waiting {stats.waiting}, in flight {stats.in_flight}, dead {stats.dead}"
        )
    return lines


def _queue_status_document(status: QueueflowStatus) -> dict[str, object]:
    def counts(stats: Any) -> dict[str, int]:
        return {"waiting": stats.waiting, "in_flight": stats.in_flight, "dead": stats.dead}

    return {
        "job_id": status.job_id,
        "state": str(status.state),
        "chunks_total": status.chunks_total,
        "extracted": status.extracted,
        "loaded": status.loaded,
        "failed": status.failed,
        "work": counts(status.work),
        "files": counts(status.files),
    }


def _plan_document(summary: PlanSummary) -> dict[str, object]:
    return {
        "job_id": summary.job_id,
        "source_table": summary.source_table,
        "destination_table": summary.destination_table,
        "columns": [{"name": name, "type": kind} for name, kind in summary.columns],
        "chunks": summary.chunks,
        "chunk_bytes": summary.chunk_bytes,
        "estimated_rows": summary.estimated_rows,
        "estimated_row_bytes": summary.estimated_row_bytes,
        "notes": list(summary.notes),
        "sample_chunks": list(summary.sample_chunks),
    }


def _status_document(summary: StatusSummary) -> dict[str, object]:
    return {
        "job_id": summary.job_id,
        "chunks_total": summary.chunks_total,
        "chunks": {str(state): count for state, count in summary.counts.items()},
    }


# -- errors become exit codes ------------------------------------------------------------------


def _load(path: Path) -> JobFile:
    return load_job(path)


def _fail(message: str, code: ExitCode) -> NoReturn:
    typer.echo(f"error: {message}", err=True)
    raise typer.Exit(code)


@contextmanager
def _exiting_on_errors(verbosity: int) -> Iterator[None]:
    """Print a failure as one line and exit with the code that says what kind it was."""
    try:
        yield
    except (
        JobFileError,
        ValueError,
        UnknownComponent,
        AmbiguousComponent,
        InvalidComponent,
    ) as error:
        _fail(str(error), ExitCode.BAD_CONFIGURATION)
    except BitdropError as error:
        _fail(str(error), ExitCode.REFUSED)
    except typer.Exit, KeyboardInterrupt, SystemExit:
        raise
    except Exception as error:
        if verbosity:
            traceback.print_exc()
        _fail(f"unexpected {type(error).__name__}: {error}", ExitCode.UNEXPECTED)
