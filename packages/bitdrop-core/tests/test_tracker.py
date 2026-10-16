"""The tracker turns the stream of progress events into what a display shows.

It is pure: time comes from the snapshots' ``elapsed_seconds``, never from a clock, so every rate
and estimate below is exact.
"""

from bitdrop_api.metadata_store import ChunkState, Stage
from bitdrop_core.progress import (
    ChunkFailed,
    ChunkFinished,
    ChunkStarted,
    PlanReady,
    Snapshot,
)
from bitdrop_core.tracker import JobTracker


def plan(chunks: int = 10, *, extractors: int = 2, loaders: int = 2) -> PlanReady:
    return PlanReady(
        job_id="job-1",
        source_table="src",
        destination_table="dst",
        chunk_ids=tuple(f"c{i:03d}" for i in range(chunks)),
        columns=(("id", "int64"),),
        estimated_rows=chunks * 100,
        estimated_row_bytes=10,
        notes=("a note",),
        extractors=extractors,
        loaders=loaders,
        max_attempts=3,
    )


def snapshot(
    elapsed: float,
    *,
    loaded: int = 0,
    failed: int = 0,
    total: int = 10,
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    used: int | None = None,
    capacity: int | None = None,
) -> Snapshot:
    counts = dict.fromkeys(ChunkState, 0)
    counts[ChunkState.LOADED] = loaded
    counts[ChunkState.FAILED] = failed
    counts[ChunkState.PENDING] = total - loaded - failed
    return Snapshot(
        job_id="job-1",
        elapsed_seconds=elapsed,
        counts=counts,
        rows_extracted=rows_extracted,
        rows_loaded=rows_loaded,
        duplicates_skipped=0,
        bytes_staged=0,
        staging_used_bytes=used,
        staging_max_bytes=capacity,
    )


def started(chunk: str, stage: Stage, worker: str) -> ChunkStarted:
    return ChunkStarted(chunk, stage, worker)


def finished(chunk: str, stage: Stage, worker: str, rows: int = 100) -> ChunkFinished:
    return ChunkFinished(chunk, stage, worker, rows=rows, bytes=1000, seconds=0.1)


def failed(chunk: str, stage: Stage, worker: str, *, final: bool, failures: int = 1) -> ChunkFailed:
    return ChunkFailed(chunk, stage, worker, "boom", failures=failures, final=final)


class TestBeforeAnythingHappens:
    def test_there_is_nothing_to_show_and_nothing_breaks(self) -> None:
        tracker = JobTracker()

        assert tracker.plan is None
        assert tracker.chunk_states == []
        assert (tracker.chunks_total, tracker.chunks_loaded) == (0, 0)
        assert tracker.fraction_done == 0.0
        assert tracker.eta_seconds is None
        assert tracker.extract_rate == tracker.load_rate == 0.0


class TestThePlan:
    def test_every_chunk_starts_pending(self) -> None:
        tracker = JobTracker()

        tracker.apply(plan(5))

        assert tracker.chunks_total == 5
        assert tracker.chunk_states == [ChunkState.PENDING] * 5
        assert tracker.plan is not None
        assert tracker.plan.notes == ("a note",)


class TestChunkTransitions:
    def test_a_chunk_walks_through_the_pipeline(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())

        tracker.apply(started("c002", Stage.EXTRACT, "extractor-0"))
        assert tracker.state_of("c002") is ChunkState.EXTRACTING
        tracker.apply(finished("c002", Stage.EXTRACT, "extractor-0"))
        assert tracker.state_of("c002") is ChunkState.EXTRACTED
        tracker.apply(started("c002", Stage.LOAD, "loader-1"))
        assert tracker.state_of("c002") is ChunkState.LOADING
        tracker.apply(finished("c002", Stage.LOAD, "loader-1"))
        assert tracker.state_of("c002") is ChunkState.LOADED
        assert tracker.state_of("c003") is ChunkState.PENDING

    def test_chunk_states_follow_plan_order(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan(4))
        tracker.apply(started("c001", Stage.EXTRACT, "extractor-0"))
        tracker.apply(started("c003", Stage.EXTRACT, "extractor-1"))

        assert tracker.chunk_states == [
            ChunkState.PENDING,
            ChunkState.EXTRACTING,
            ChunkState.PENDING,
            ChunkState.EXTRACTING,
        ]

    def test_a_failed_attempt_sends_the_chunk_back_to_where_it_waits(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(started("c000", Stage.EXTRACT, "extractor-0"))
        tracker.apply(failed("c000", Stage.EXTRACT, "extractor-0", final=False))
        assert tracker.state_of("c000") is ChunkState.PENDING

        tracker.apply(started("c001", Stage.LOAD, "loader-0"))
        tracker.apply(failed("c001", Stage.LOAD, "loader-0", final=False))
        assert tracker.state_of("c001") is ChunkState.EXTRACTED

    def test_a_final_failure_marks_the_chunk_failed(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(started("c000", Stage.EXTRACT, "extractor-0"))

        tracker.apply(failed("c000", Stage.EXTRACT, "extractor-0", final=True, failures=3))

        assert tracker.state_of("c000") is ChunkState.FAILED

    def test_events_for_chunks_outside_the_plan_are_ignored(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan(2))

        tracker.apply(started("nope", Stage.EXTRACT, "extractor-0"))
        tracker.apply(finished("nope", Stage.EXTRACT, "extractor-0"))

        assert tracker.chunk_states == [ChunkState.PENDING] * 2

    def test_events_before_the_plan_do_not_crash(self) -> None:
        tracker = JobTracker()

        tracker.apply(started("c000", Stage.EXTRACT, "extractor-0"))

        assert tracker.chunk_states == []


class TestWorkers:
    def test_a_worker_is_busy_with_its_chunk_until_it_finishes(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(snapshot(2.0))

        tracker.apply(started("c004", Stage.EXTRACT, "extractor-0"))
        busy = tracker.workers["extractor-0"]
        assert busy is not None
        assert (busy.chunk_id, busy.stage, busy.since_seconds) == ("c004", Stage.EXTRACT, 2.0)

        tracker.apply(finished("c004", Stage.EXTRACT, "extractor-0"))
        assert tracker.workers["extractor-0"] is None

    def test_a_failed_attempt_frees_the_worker_too(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        tracker.apply(started("c004", Stage.LOAD, "loader-1"))

        tracker.apply(failed("c004", Stage.LOAD, "loader-1", final=False))

        assert tracker.workers["loader-1"] is None

    def test_workers_are_listed_in_name_order(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())
        for worker in ("loader-1", "extractor-1", "extractor-0"):
            tracker.apply(started("c000", Stage.EXTRACT, worker))

        assert list(tracker.workers) == ["extractor-0", "extractor-1", "loader-1"]


class TestFailures:
    def test_failures_are_kept_oldest_first_and_capped(self) -> None:
        tracker = JobTracker(max_failures=3)
        tracker.apply(plan())

        for index in range(5):
            tracker.apply(failed(f"c{index:03d}", Stage.EXTRACT, "extractor-0", final=False))

        assert [f.chunk_id for f in tracker.failures] == ["c002", "c003", "c004"]


class TestSnapshots:
    def test_counts_and_totals_come_from_the_latest_snapshot(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())

        tracker.apply(snapshot(3.0, loaded=4, failed=1, rows_extracted=900, rows_loaded=400))

        assert (tracker.chunks_loaded, tracker.chunks_failed) == (4, 1)
        assert tracker.fraction_done == 0.5
        assert (tracker.rows_extracted, tracker.rows_loaded) == (900, 400)
        assert tracker.elapsed_seconds == 3.0

    def test_staging_fill_is_reported_when_the_store_knows_it(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan())

        tracker.apply(snapshot(1.0, used=250, capacity=1000))

        assert (tracker.staging_used, tracker.staging_capacity) == (250, 1000)
        assert tracker.staging_fraction == 0.25

    def test_staging_fraction_is_unknown_without_a_capacity(self) -> None:
        tracker = JobTracker()
        tracker.apply(snapshot(1.0, used=250, capacity=None))

        assert tracker.staging_fraction is None


class TestRates:
    def test_rows_per_second_is_measured_over_the_recent_window(self) -> None:
        tracker = JobTracker(rate_window=5.0)
        tracker.apply(plan())

        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(1.0, rows_extracted=1000, rows_loaded=500))
        tracker.apply(snapshot(2.0, rows_extracted=3000, rows_loaded=1500))

        assert tracker.extract_rate == 1500.0
        assert tracker.load_rate == 750.0

    def test_old_samples_fall_out_of_the_window(self) -> None:
        tracker = JobTracker(rate_window=2.0)
        tracker.apply(plan())

        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(1.0, rows_extracted=10_000))
        tracker.apply(snapshot(2.0, rows_extracted=10_300))
        tracker.apply(snapshot(3.0, rows_extracted=10_400))

        # The window reaches back exactly 2 s, to the sample at t=1 (the burst before it is out).
        assert tracker.extract_rate == 200.0

    def test_one_snapshot_is_not_enough_to_know_a_rate(self) -> None:
        tracker = JobTracker()
        tracker.apply(snapshot(1.0, rows_extracted=1000))

        assert tracker.extract_rate == 0.0

    def test_history_keeps_one_value_per_snapshot_up_to_its_limit(self) -> None:
        tracker = JobTracker(history=3)
        tracker.apply(plan())

        for second in range(6):
            tracker.apply(snapshot(float(second), rows_extracted=second * 100))

        assert len(tracker.extract_history) == 3
        assert tracker.extract_history[-1] == tracker.extract_rate == 100.0
        assert len(tracker.load_history) == 3


class TestEta:
    def test_the_estimate_extrapolates_the_recent_chunk_rate(self) -> None:
        tracker = JobTracker(rate_window=60.0)
        tracker.apply(plan(10))

        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(4.0, loaded=2))

        assert tracker.eta_seconds == 16.0, "8 chunks left at 0.5 chunks per second"

    def test_no_estimate_until_something_has_finished(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan(10))
        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(4.0, loaded=0))

        assert tracker.eta_seconds is None

    def test_a_finished_job_has_nothing_left_to_wait_for(self) -> None:
        tracker = JobTracker()
        tracker.apply(plan(10))
        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(4.0, loaded=10))

        assert tracker.eta_seconds == 0.0
        assert tracker.fraction_done == 1.0
        assert tracker.is_finished
