"""The Textual screen, driven by Textual's own test pilot (no terminal needed)."""

import asyncio
import io
import logging
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import pytest
from textual.pilot import Pilot
from textual.widgets import RichLog, Static

from bitdrop_api.metadata_store import ChunkRecord, ChunkState, Stage
from bitdrop_core.progress import ChunkFailed, ChunkStarted, PlanReady, Snapshot
from bitdrop_core.transfer import TransferReport
from bitdrop_core.ui import ProgressUi, UiContext
from bitdrop_tui import create_ui
from bitdrop_tui.app import BitdropApp, TuiUi

CONTEXT = UiContext(
    job_id="job-1",
    source_table="src",
    destination_table="dst",
    endpoints={"source": "a://", "destination": "b://", "staging": "c://", "metadata": "d://"},
)


def plan(chunks: int = 10) -> PlanReady:
    return PlanReady(
        job_id="job-1",
        source_table="src",
        destination_table="dst",
        chunk_ids=tuple(f"c{i:03d}" for i in range(chunks)),
        columns=(("id", "int64"),),
        estimated_rows=1000,
        estimated_row_bytes=10,
        notes=(),
        extractors=2,
        loaders=2,
        max_attempts=3,
    )


def snapshot(elapsed: float, *, loaded: int = 0, total: int = 10, rows: int = 0) -> Snapshot:
    counts = dict.fromkeys(ChunkState, 0)
    counts[ChunkState.LOADED] = loaded
    counts[ChunkState.PENDING] = total - loaded
    return Snapshot("job-1", elapsed, counts, rows, rows, 0, 0, 1 << 20, 4 << 20)


def report(*, failed: int = 0) -> TransferReport:
    failures = (
        ChunkRecord("job-1", "c003", ChunkState.FAILED, 3, None, "ConnectionError: gone"),
    ) * failed
    return TransferReport("job-1", 10, 10 - failed, failed, 1000, 1000, 0, 4096, 2.0, failures)


def shown(app: BitdropApp, selector: str) -> str:
    widget = app.query_one(selector, Static)
    content = widget.content
    return content.plain if hasattr(content, "plain") else str(content)


def events(app: BitdropApp) -> str:
    return "\n".join(strip.text for strip in app.query_one("#events", RichLog).lines)


async def never_ends() -> TransferReport:
    await asyncio.sleep(3600)
    raise AssertionError("unreachable")


@pytest.fixture
async def running_job() -> AsyncIterator[asyncio.Task[TransferReport]]:
    task = asyncio.create_task(never_ends())
    yield task
    task.cancel()


class TestTheScreen:
    async def test_it_shows_the_job_and_how_far_it_is(
        self, running_job: asyncio.Task[TransferReport]
    ) -> None:
        ui = TuiUi(CONTEXT)
        app = BitdropApp(ui, running_job)
        async with app.run_test(size=(120, 40)) as pilot:
            ui(plan())
            ui(snapshot(0.0))
            ui(snapshot(4.0, loaded=6, rows=600))
            app.refresh_view()
            await pilot.pause()

            assert "job-1" in shown(app, "#title")
            assert "src" in shown(app, "#title")
            assert "dst" in shown(app, "#title")
            assert "60%" in shown(app, "#overall")
            assert "6/10 chunks" in shown(app, "#overall")
            assert "extract" in shown(app, "#throughput")
            assert "25%" in shown(app, "#staging")
            assert "■" in shown(app, "#chunks")

    async def test_workers_are_listed_with_what_they_hold(
        self, running_job: asyncio.Task[TransferReport]
    ) -> None:
        ui = TuiUi(CONTEXT)
        app = BitdropApp(ui, running_job)
        async with app.run_test(size=(120, 40)) as pilot:
            ui(plan())
            ui(ChunkStarted("c004", Stage.EXTRACT, "extractor-0"))
            app.refresh_view()
            await pilot.pause()

            assert "extractor-0" in shown(app, "#workers")
            assert "c004" in shown(app, "#workers")

    async def test_a_failed_attempt_appears_in_the_events(
        self, running_job: asyncio.Task[TransferReport]
    ) -> None:
        ui = TuiUi(CONTEXT)
        app = BitdropApp(ui, running_job)
        async with app.run_test(size=(120, 40)) as pilot:
            ui(plan())
            ui(ChunkFailed("c003", Stage.LOAD, "loader-1", "boom", failures=2, final=False))
            ui(ChunkFailed("c004", Stage.LOAD, "loader-0", "gone", failures=3, final=True))
            app.refresh_view()
            await pilot.pause()

            text = events(app)
            assert "c003" in text
            assert "attempt 2/3" in text
            assert "boom" in text
            assert "c004" in text
            assert "gave up" in text

    async def test_log_lines_go_to_the_events_instead_of_the_terminal(
        self, running_job: asyncio.Task[TransferReport]
    ) -> None:
        ui = TuiUi(CONTEXT)
        handler = ui.log_handler()
        assert isinstance(handler, logging.Handler)
        logger = logging.getLogger("bitdrop.test-tui")
        logger.propagate = False
        logger.addHandler(handler)
        app = BitdropApp(ui, running_job)
        async with app.run_test(size=(120, 40)) as pilot:
            logger.warning("source %s is slow", "shop")
            app.refresh_view()
            await pilot.pause()

            assert "source shop is slow" in events(app)
        logger.removeHandler(handler)

    async def test_the_events_can_be_hidden_and_shown(
        self, running_job: asyncio.Task[TransferReport]
    ) -> None:
        ui = TuiUi(CONTEXT)
        app = BitdropApp(ui, running_job)
        async with app.run_test(size=(120, 40)) as pilot:
            assert app.query_one("#events").display

            await pilot.press("l")
            assert not app.query_one("#events").display

            await pilot.press("l")
            assert app.query_one("#events").display

    async def test_a_huge_job_still_draws_quickly(
        self, running_job: asyncio.Task[TransferReport]
    ) -> None:
        ui = TuiUi(CONTEXT)
        app = BitdropApp(ui, running_job)
        async with app.run_test(size=(120, 40)) as pilot:
            ui(plan(100_000))
            ui(snapshot(1.0, total=100_000))

            started = time.monotonic()
            app.refresh_view()
            await pilot.pause()

            assert time.monotonic() - started < 2.0
            assert "each ■ =" in shown(app, "#chunks")


Auto = Callable[[Pilot[object]], Coroutine[Any, Any, None]]


def tui(*, keep_open: bool = True, auto: Auto | None = None) -> tuple[TuiUi, io.StringIO]:
    out = io.StringIO()
    context = UiContext(
        job_id="job-1",
        source_table="src",
        destination_table="dst",
        endpoints={},
        keep_open=keep_open,
    )
    return TuiUi(context, out=out, headless=True, size=(110, 36), auto_pilot=auto), out


async def present_promptly(
    ui: TuiUi, job: asyncio.Task[TransferReport], seconds: float = 6
) -> None:
    """Present the job and insist that the screen closes within ``seconds``.

    A screen that never closes must fail the test, not hang it, and Textual swallows a
    cancellation, so the clock decides; the stuck screen is left to the event loop's teardown.
    """
    presenting = asyncio.create_task(ui.present(job))
    done, _ = await asyncio.wait({presenting}, timeout=seconds)
    if not done:
        presenting.cancel()
        pytest.fail(f"the screen did not close within {seconds} s")
    presenting.result()


class TestPresenting:
    """``present`` keeps the screen up for the job and hands control back when it is over."""

    async def test_it_is_a_progress_ui(self) -> None:
        ui, _ = tui()

        assert isinstance(ui, ProgressUi)
        assert isinstance(create_ui(CONTEXT), ProgressUi)

    async def test_pressing_q_stops_a_running_job_and_the_screen_closes_by_itself(self) -> None:
        async def press_q(pilot: Pilot[object]) -> None:
            await pilot.pause(0.2)
            await pilot.press("q")

        ui, out = tui(auto=press_q)
        job = asyncio.create_task(never_ends())

        await present_promptly(ui, job)

        assert job.cancelled()
        assert out.getvalue() == "", "a stopped job has no summary"

    async def test_a_second_q_leaves_even_if_the_job_will_not_stop(self) -> None:
        """If handing chunks back hangs, the person is not trapped on the screen."""
        release = asyncio.Event()

        async def stubborn() -> TransferReport:
            while not release.is_set():
                try:
                    await asyncio.sleep(0.05)
                except asyncio.CancelledError:
                    continue  # refuses to be cancelled
            return report()

        async def press_q_twice(pilot: Pilot[object]) -> None:
            await pilot.pause(0.2)
            await pilot.press("q")
            await pilot.pause(0.2)
            await pilot.press("q")

        ui, _ = tui(auto=press_q_twice)
        job = asyncio.create_task(stubborn())

        await present_promptly(ui, job)

        assert not job.done(), "the screen left while the job was still being stopped"
        release.set()
        await job

    async def test_a_finished_job_stays_on_screen_until_q_and_then_prints_its_summary(
        self,
    ) -> None:
        seen: list[str] = []

        async def look_then_leave(pilot: Pilot[object]) -> None:
            for _ in range(50):
                await pilot.pause(0.1)
                banner = pilot.app.query_one("#banner", Static).content
                if "Done" in getattr(banner, "plain", str(banner)):
                    seen.append(getattr(banner, "plain", str(banner)))
                    break
            await pilot.press("q")

        ui, out = tui(auto=look_then_leave)

        async def finishes() -> TransferReport:
            ui(plan())
            await asyncio.sleep(0.3)
            return report()

        await present_promptly(ui, asyncio.create_task(finishes()))

        assert seen
        assert "all 10 chunks loaded" in seen[0]
        assert "job-1: all 10 chunks loaded" in out.getvalue()

    async def test_without_keep_open_the_screen_closes_when_the_job_ends(self) -> None:
        ui, out = tui(keep_open=False)

        async def finishes() -> TransferReport:
            await asyncio.sleep(0.3)
            return report()

        await present_promptly(ui, asyncio.create_task(finishes()))

        assert "all 10 chunks loaded" in out.getvalue()

    async def test_chunks_that_gave_up_are_shown_as_a_failure(self) -> None:
        seen: list[str] = []

        async def look_then_leave(pilot: Pilot[object]) -> None:
            for _ in range(50):
                await pilot.pause(0.1)
                banner = pilot.app.query_one("#banner", Static).content
                text = getattr(banner, "plain", str(banner))
                if "failed" in text:
                    seen.append(text)
                    break
            await pilot.press("q")

        ui, out = tui(auto=look_then_leave)

        async def finishes() -> TransferReport:
            await asyncio.sleep(0.2)
            return report(failed=2)

        await present_promptly(ui, asyncio.create_task(finishes()))

        assert seen
        assert "2 chunks failed" in seen[0]
        assert "FAILED" in out.getvalue()

    async def test_a_job_that_raises_leaves_the_error_on_screen_until_q(self) -> None:
        seen: list[str] = []

        async def look_then_leave(pilot: Pilot[object]) -> None:
            for _ in range(50):
                await pilot.pause(0.1)
                banner = pilot.app.query_one("#banner", Static).content
                text = getattr(banner, "plain", str(banner))
                if "planning failed" in text:
                    seen.append(text)
                    break
            await pilot.press("q")

        ui, out = tui(auto=look_then_leave)

        async def breaks() -> TransferReport:
            await asyncio.sleep(0.2)
            raise RuntimeError("planning failed")

        job = asyncio.create_task(breaks())
        await present_promptly(ui, job)

        assert seen
        assert out.getvalue() == ""
        assert isinstance(job.exception(), RuntimeError)

    async def test_a_job_cancelled_from_outside_closes_the_screen_too(self) -> None:
        """SIGTERM cancels the job; nobody may be there to press q."""
        ui, _ = tui()
        job = asyncio.create_task(never_ends())

        async def cancel_later() -> None:
            await asyncio.sleep(0.5)
            job.cancel()

        canceller = asyncio.create_task(cancel_later())
        await present_promptly(ui, job)
        await canceller

        assert job.cancelled()
