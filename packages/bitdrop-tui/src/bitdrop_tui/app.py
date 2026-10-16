"""The Textual screen for a running job, and the display (``--ui tui``) that owns it.

``TuiUi`` is the :class:`~bitdrop_core.ui.ProgressUi`: the runner calls it with every event, it
feeds a :class:`~bitdrop_core.tracker.JobTracker`, and ``present`` shows a :class:`BitdropApp`
until the job is over. The screen redraws a few times a second from the tracker, so a flood of
events costs nothing extra. Pressing ``q`` cancels the job, which makes the runner hand its chunks
back (a rerun resumes at once); a second press leaves immediately.
"""

import asyncio
import logging
import sys
from collections import deque
from collections.abc import Callable, Coroutine
from typing import Any, TextIO

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from textual.pilot import Pilot
from textual.widgets import Footer, RichLog, Static

from bitdrop_core.humanize import format_duration
from bitdrop_core.progress import ChunkFailed, TransferEvent
from bitdrop_core.summary import summary_lines
from bitdrop_core.tracker import JobTracker
from bitdrop_core.transfer import TransferReport
from bitdrop_core.ui import UiContext
from bitdrop_tui.render import (
    chunk_map,
    legend,
    overall_text,
    staging_text,
    throughput_text,
    workers_text,
)

__all__ = ["BitdropApp", "TuiUi"]

REFRESH_SECONDS = 0.25
MAP_ROWS = 6
"""The most rows the chunk map may take; bigger jobs share cells."""

DEFAULT_WIDTH = 60
"""Width assumed for a panel that has not been laid out yet."""

AutoPilot = Callable[[Pilot[object]], Coroutine[Any, Any, None]]


class TuiUi:
    """Shows a running job as a full-screen terminal UI."""

    def __init__(
        self,
        context: UiContext,
        *,
        out: TextIO | None = None,
        headless: bool = False,
        size: tuple[int, int] | None = None,
        auto_pilot: AutoPilot | None = None,
    ) -> None:
        """Show the job described by ``context``; the summary goes to ``out`` (default stdout).

        ``headless``, ``size``, and ``auto_pilot`` run the screen without a terminal and let a test
        press keys. They are for tests.
        """
        self.context = context
        self.tracker = JobTracker()
        self._out = out
        self._headless = headless
        self._size = size
        self._auto_pilot = auto_pilot
        self._lines: deque[Text] = deque(maxlen=1000)

    def __call__(self, event: TransferEvent) -> None:
        """Take one progress event."""
        self.tracker.apply(event)
        if isinstance(event, ChunkFailed):
            self._lines.append(self._describe_failure(event))

    def log_handler(self) -> logging.Handler:
        """Collect log records for the events pane, so they never corrupt the screen."""
        return _EventsHandler(self._lines)

    def take_lines(self) -> list[Text]:
        """Return and forget the event lines that arrived since the last call."""
        lines = list(self._lines)
        self._lines.clear()
        return lines

    async def present(self, job: asyncio.Task[TransferReport]) -> None:
        """Show the screen until the job is over and the person has left it."""
        app = BitdropApp(self, job)
        await app.run_async(headless=self._headless, size=self._size, auto_pilot=self._auto_pilot)
        if job.done() and not job.cancelled() and job.exception() is None:
            for line in summary_lines(job.result()):
                print(line, file=self._out or sys.stdout)

    def _describe_failure(self, event: ChunkFailed) -> Text:
        plan = self.tracker.plan
        limit = plan.max_attempts if plan else event.failures
        line = Text(f"{event.chunk_id} {event.stage} attempt {event.failures}/{limit}: ")
        line.append(event.reason, style="red")
        if event.final:
            line.append("  gave up", style="bold red")
        return line


class _EventsHandler(logging.Handler):
    """Puts log records into the same queue as the failures."""

    def __init__(self, lines: deque[Text]) -> None:
        super().__init__()
        self._lines = lines

    def emit(self, record: logging.LogRecord) -> None:
        style = "red" if record.levelno >= logging.ERROR else "yellow"
        self._lines.append(Text(f"{record.levelname.lower()}: {record.getMessage()}", style=style))


class BitdropApp(App[None]):
    """One screen: progress, throughput, staging, the chunk map, workers, and events."""

    TITLE = "bitdrop"
    ENABLE_COMMAND_PALETTE = False  # it adds nothing here but a stray hint in the footer
    CSS = """
    Screen { layout: vertical; }
    #title { height: 1; background: $primary; color: $text; padding: 0 1; text-style: bold; }
    .panel { border: round $primary-darken-1; padding: 0 1; }
    #overall { height: 4; }
    #mid { height: 6; }
    #throughput { width: 3fr; }
    #staging { width: 2fr; }
    #chunks { height: auto; min-height: 4; }
    #low { height: 1fr; min-height: 5; }
    #workers-box { width: 2fr; }
    #events { width: 3fr; }
    #banner { height: auto; padding: 0 1; }
    """
    BINDINGS = [  # noqa: RUF012  (Textual reads this class attribute)
        Binding("q", "stop", "Stop safely"),
        Binding("ctrl+c", "stop", "Stop", show=False, priority=True),
        Binding("l", "toggle_events", "Events"),
    ]

    def __init__(self, ui: TuiUi, job: asyncio.Task[TransferReport]) -> None:
        """Show ``ui``'s tracker while ``job`` runs; ``q`` cancels the job."""
        super().__init__()
        self._ui = ui
        self._job = job
        self._stopping = False

    def compose(self) -> ComposeResult:
        """Lay out the panels."""
        yield Static(id="title")
        yield Static(id="overall", classes="panel")
        with Horizontal(id="mid"):
            yield Static(id="throughput", classes="panel")
            yield Static(id="staging", classes="panel")
        yield Static(id="chunks", classes="panel")
        with Horizontal(id="low"):
            with VerticalScroll(id="workers-box", classes="panel"):
                yield Static(id="workers")
            yield RichLog(id="events", classes="panel", wrap=True, markup=False)
        yield Static(id="banner")
        yield Footer()

    def on_mount(self) -> None:
        """Name the panels, start redrawing, and watch the job."""
        for selector, title in (
            ("#overall", "Progress"),
            ("#throughput", "Rows per second"),
            ("#staging", "Staging tier"),
            ("#chunks", "Chunks"),
            ("#workers-box", "Workers"),
            ("#events", "Events"),
        ):
            self.query_one(selector).border_title = title
        self.query_one("#banner").display = False
        self.refresh_view()
        self.set_interval(REFRESH_SECONDS, self.refresh_view)
        self.run_worker(self._watch_job(), exit_on_error=False)

    def refresh_view(self) -> None:
        """Redraw every panel from the tracker and move new event lines into the log."""
        tracker = self._ui.tracker
        context = self._ui.context
        title = Text(f" bitdrop   {context.job_id}   {context.source_table} → ")
        title.append(context.destination_table)
        self.query_one("#title", Static).update(title)

        self.query_one("#overall", Static).update(
            overall_text(tracker, width=self._width("#overall"))
        )
        self.query_one("#throughput", Static).update(
            throughput_text(tracker, width=self._width("#throughput"))
        )
        self.query_one("#staging", Static).update(
            staging_text(tracker, width=self._width("#staging"))
        )

        cells, per_cell = chunk_map(
            tracker.chunk_states, width=self._width("#chunks"), max_rows=MAP_ROWS, gap=1
        )
        picture = Text()
        if cells.plain:
            picture.append_text(cells)
            picture.append("\n")
        picture.append_text(legend())
        if per_cell > 1:
            picture.append(f"  each ■ = {per_cell} chunks", style="dim")
        self.query_one("#chunks", Static).update(picture)

        self.query_one("#workers", Static).update(workers_text(tracker))

        log = self.query_one("#events", RichLog)
        for line in self._ui.take_lines():
            log.write(line)

    def action_stop(self) -> None:
        """Ask the job to stop; leave at once if it already has, or if asked a second time."""
        if self._job.done() or self._stopping:
            self.exit()
            return
        self._stopping = True
        self._job.cancel()
        self._banner("Stopping: handing unfinished chunks back…", "yellow")

    def action_toggle_events(self) -> None:
        """Show or hide the events pane to give the workers more room."""
        events = self.query_one("#events")
        events.display = not events.display

    # -- the job's end ----------------------------------------------------------------------------

    async def _watch_job(self) -> None:
        await asyncio.wait({self._job})
        self.refresh_view()
        job = self._job
        if job.cancelled():
            self._banner(
                "Stopped. Unfinished chunks were handed back; run the same command to resume.",
                "yellow",
            )
            self.exit()  # the command repeats this after the screen closes; nobody needs to read it
            return
        error = job.exception()
        if error is not None:
            self._banner(f"Failed: {error}   (press q to leave)", "bold red")
        else:
            report = job.result()
            self._banner(_outcome(report), "green" if report.succeeded else "bold red")
        if not self._ui.context.keep_open:
            self.exit()

    def _banner(self, message: str, style: str) -> None:
        banner = self.query_one("#banner", Static)
        banner.update(Text(message, style=style))
        banner.display = True

    def _width(self, selector: str) -> int:
        width = self.query_one(selector).content_size.width
        return width if width > 0 else DEFAULT_WIDTH


def _outcome(report: TransferReport) -> str:
    if report.succeeded:
        return (
            f"Done: all {report.chunks_total} chunks loaded, {report.rows_loaded:,} rows in "
            f"{format_duration(report.duration_seconds)}.   (press q to leave)"
        )
    return (
        f"Finished: {report.chunks_failed} chunks failed (see Events); "
        f"{report.chunks_loaded} of {report.chunks_total} loaded.   (press q to leave)"
    )
