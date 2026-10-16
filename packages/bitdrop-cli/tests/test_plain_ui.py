"""The plain display: one line of progress now and then, and a summary, for logs and terminals."""

import asyncio
import io

from bitdrop_api.metadata_store import ChunkRecord, ChunkState, Stage
from bitdrop_cli.plain import PlainUi, progress_line, summary_lines
from bitdrop_core.progress import ChunkFailed, PlanReady, Snapshot
from bitdrop_core.tracker import JobTracker
from bitdrop_core.transfer import TransferReport
from bitdrop_core.ui import ProgressUi, UiContext

CONTEXT = UiContext(
    job_id="job-1",
    source_table="src",
    destination_table="dst",
    endpoints={"source": "a://", "destination": "b://", "staging": "c://", "metadata": "d://"},
)


def plan(chunks: int = 200) -> PlanReady:
    return PlanReady(
        job_id="job-1",
        source_table="src",
        destination_table="dst",
        chunk_ids=tuple(f"c{i:04d}" for i in range(chunks)),
        columns=(("id", "int64"),),
        estimated_rows=2_000_000,
        estimated_row_bytes=100,
        notes=(),
        extractors=4,
        loaders=3,
        max_attempts=3,
    )


def snapshot(
    elapsed: float,
    *,
    loaded: int = 0,
    failed: int = 0,
    total: int = 200,
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    used: int | None = None,
    capacity: int | None = None,
) -> Snapshot:
    counts = dict.fromkeys(ChunkState, 0)
    counts[ChunkState.LOADED] = loaded
    counts[ChunkState.FAILED] = failed
    counts[ChunkState.PENDING] = total - loaded - failed
    return Snapshot("job-1", elapsed, counts, rows_extracted, rows_loaded, 0, 0, used, capacity)


def report(
    *,
    loaded: int = 200,
    failed: int = 0,
    failures: tuple[ChunkRecord, ...] = (),
    duplicates: int = 0,
) -> TransferReport:
    return TransferReport(
        job_id="job-1",
        chunks_total=loaded + failed,
        chunks_loaded=loaded,
        chunks_failed=failed,
        rows_extracted=2_000_000,
        rows_loaded=2_000_000,
        duplicates_skipped=duplicates,
        bytes_staged=1_288_490_189,
        duration_seconds=4.0,
        failures=failures,
    )


class TestProgressLine:
    def test_it_shows_how_far_the_job_is_and_how_fast(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(12.0, loaded=126, rows_extracted=2_600_000, rows_loaded=2_400_000))

        line = progress_line(tracker)

        assert "0:12" in line
        assert "63%" in line
        assert "126/200 chunks" in line
        assert "2.4M rows" in line
        assert "in 217k rows/s" in line
        assert "out 200k rows/s" in line

    def test_staging_is_shown_when_the_store_knows_it(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(snapshot(1.0, used=1_288_490_189, capacity=4 * 1024**3))

        assert "staging 1.2 GiB / 4.0 GiB" in progress_line(tracker)

    def test_staging_without_a_capacity_shows_only_what_it_holds(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(snapshot(1.0, used=2048, capacity=None))

        assert "staging 2.0 KiB" in progress_line(tracker)
        assert "/" not in progress_line(tracker).split("staging")[1]

    def test_no_staging_figure_is_invented(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(snapshot(1.0))

        assert "staging" not in progress_line(tracker)

    def test_the_estimate_appears_once_it_can_be_made(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan(10))
        tracker.apply(snapshot(0.0, total=10))
        assert "eta" not in progress_line(tracker)

        tracker.apply(snapshot(4.0, loaded=2, total=10))

        assert "eta 0:16" in progress_line(tracker)

    def test_failed_chunks_are_counted(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(snapshot(1.0, loaded=10, failed=2))

        assert "2 failed" in progress_line(tracker)

    def test_before_the_first_snapshot_it_still_says_something_sensible(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())

        assert "0/200 chunks" in progress_line(tracker)


class TestSummary:
    def test_a_complete_job_says_so_and_gives_the_numbers(self) -> None:
        text = "\n".join(summary_lines(report()))

        assert "job-1: all 200 chunks loaded" in text
        assert "2,000,000" in text
        assert "1.2 GiB" in text
        assert "4.0 s" in text
        assert "500k rows/s" in text

    def test_failures_are_listed_with_their_reasons(self) -> None:
        failure = ChunkRecord(
            "job-1", "c0042", ChunkState.FAILED, 3, None, "ConnectionError: source went away"
        )

        text = "\n".join(summary_lines(report(loaded=198, failed=2, failures=(failure,))))

        assert "198 of 200 chunks loaded, 2 FAILED" in text
        assert "c0042" in text
        assert "ConnectionError: source went away" in text

    def test_chunks_the_destination_already_had_are_mentioned(self) -> None:
        assert "3 chunks were already loaded" in "\n".join(summary_lines(report(duplicates=3)))

    def test_nothing_is_said_about_repeats_when_there_were_none(self) -> None:
        assert "already" not in "\n".join(summary_lines(report()))


class TestThePlainDisplay:
    def make(self, interval: float = 2.0) -> tuple[PlainUi, io.StringIO, io.StringIO]:
        out, err = io.StringIO(), io.StringIO()
        return PlainUi(CONTEXT, out=out, err=err, interval=interval), out, err

    def test_it_is_a_progress_ui(self) -> None:
        ui, _, _ = self.make()

        assert isinstance(ui, ProgressUi)
        assert ui.log_handler() is None

    def test_the_plan_is_announced_on_the_diagnostics_stream(self) -> None:
        ui, out, err = self.make()

        ui(plan())

        assert "job-1" in err.getvalue()
        assert "src -> dst" in err.getvalue()
        assert "200 chunks" in err.getvalue()
        assert out.getvalue() == "", "stdout is for the result only"

    def test_progress_lines_are_throttled(self) -> None:
        ui, _, err = self.make(interval=2.0)
        ui(plan())
        before = err.getvalue().count("\n")

        for elapsed in (0.0, 0.5, 1.0, 1.9, 2.0, 2.5, 3.9, 4.1):
            ui(snapshot(elapsed, loaded=1))

        assert err.getvalue().count("\n") - before == 3, "at 0.0, 2.0 and 4.1"

    def test_the_last_snapshot_is_always_shown(self) -> None:
        ui, _, err = self.make(interval=60.0)
        ui(plan(10))
        ui(snapshot(0.0, total=10))

        ui(snapshot(1.0, loaded=10, total=10))

        assert "10/10 chunks" in err.getvalue().splitlines()[-1]

    def test_failures_are_left_to_the_log(self) -> None:
        """The runner logs every failed attempt; saying it again here would print it twice."""
        ui, _, err = self.make()
        ui(plan())
        before = err.getvalue()

        ui(ChunkFailed("c0001", Stage.EXTRACT, "extractor-0", "boom", failures=1, final=False))

        assert err.getvalue() == before

    async def test_presenting_prints_the_summary_when_the_job_ends(self) -> None:
        ui, out, _ = self.make()

        async def job() -> TransferReport:
            return report()

        await ui.present(asyncio.create_task(job()))

        assert "all 200 chunks loaded" in out.getvalue()

    async def test_a_job_that_raised_is_left_for_the_caller_to_report(self) -> None:
        ui, out, _ = self.make()

        async def job() -> TransferReport:
            raise RuntimeError("planning failed")

        task = asyncio.create_task(job())
        await ui.present(task)

        assert out.getvalue() == ""
        assert task.done()

    async def test_a_cancelled_job_prints_no_summary(self) -> None:
        ui, out, _ = self.make()

        async def job() -> TransferReport:
            await asyncio.sleep(60)
            return report()

        task = asyncio.create_task(job())
        await asyncio.sleep(0)
        task.cancel()
        await ui.present(task)

        assert out.getvalue() == ""
