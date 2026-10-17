"""The ``bitdrop`` commands: ``run``, ``plan``, ``status``, and ``components``."""

import asyncio
import json
import sys
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from bitdrop_api.errors import BitdropError
from bitdrop_api.metadata_store import ChunkState
from bitdrop_cli.execute import (
    AUTO,
    TUI,
    ExitCode,
    PlanSummary,
    StatusSummary,
    job_status,
    plan_job,
    resolve_ui,
    run_job,
)
from bitdrop_cli.jobfile import JobFile, JobFileError, load_job
from bitdrop_core.humanize import format_bytes
from bitdrop_core.registry import (
    AmbiguousComponent,
    InvalidComponent,
    UnknownComponent,
    available_chunk_stores,
    available_destinations,
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


@app.command()
def components() -> None:
    """List the connectors, stores, and displays that are installed."""
    rows = {
        "sources": available_sources(),
        "destinations": available_destinations(),
        "staging": available_chunk_stores(),
        "metadata": available_metadata_stores(),
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
