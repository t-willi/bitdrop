"""What the TUI draws, as pure functions from the tracker's state to styled text."""

import pytest
from rich.text import Text

from bitdrop_api.metadata_store import ChunkState, Stage
from bitdrop_core.progress import ChunkFinished, ChunkStarted, PlanReady, Snapshot
from bitdrop_core.tracker import JobTracker
from bitdrop_tui.render import (
    STATE_STYLE,
    activity_text,
    bar,
    bucket_state,
    chunk_map,
    overall_text,
    spark,
    staging_text,
    throughput_text,
    workers_text,
)

S = ChunkState


class TestBar:
    @pytest.mark.parametrize(
        ("fraction", "width", "drawn"),
        [
            (0.0, 4, "░░░░"),
            (1.0, 4, "████"),
            (0.5, 10, "█████░░░░░"),
            (0.99, 10, "█████████░"),
            (0.01, 10, "█░░░░░░░░░"),
            (-1.0, 3, "░░░"),
            (7.0, 3, "███"),
            (0.5, 0, ""),
        ],
    )
    def test_it_fills_in_proportion_and_never_lies_at_the_ends(
        self, fraction: float, width: int, drawn: str
    ) -> None:
        assert bar(fraction, width).plain == drawn

    def test_the_filled_part_is_coloured_and_the_rest_is_dim(self) -> None:
        text = bar(0.5, 4, style="green")

        assert [(span.start, span.end, str(span.style)) for span in text.spans] == [
            (0, 2, "green"),
            (2, 4, "dim"),
        ]


class TestSpark:
    def test_values_become_rising_blocks(self) -> None:
        assert spark([0, 1, 2, 3, 4, 5, 6, 7], 8).plain == "▁▂▃▄▅▆▇█"

    def test_it_scales_to_the_largest_value(self) -> None:
        assert spark([5, 10], 2).plain == "▄█"

    def test_nothing_happening_is_a_flat_line(self) -> None:
        assert spark([0, 0, 0], 3).plain == "▁▁▁"

    def test_only_the_most_recent_values_are_shown(self) -> None:
        assert spark([7, 7, 0, 7], 2).plain == "▁█"

    def test_a_short_history_is_right_aligned(self) -> None:
        assert spark([4, 4], 4).plain == "  ██"

    def test_no_history_is_blank(self) -> None:
        assert spark([], 3).plain == "   "


class TestBucketState:
    @pytest.mark.parametrize(
        ("states", "shown"),
        [
            ([S.PENDING, S.PENDING], S.PENDING),
            ([S.LOADED, S.LOADED], S.LOADED),
            ([S.EXTRACTED, S.EXTRACTED], S.EXTRACTED),
            ([S.LOADED, S.PENDING], S.EXTRACTED),
            ([S.LOADED, S.FAILED, S.PENDING], S.FAILED),
            ([S.LOADED, S.EXTRACTING], S.EXTRACTING),
            ([S.PENDING, S.LOADING, S.EXTRACTING], S.LOADING),
            ([S.FAILED, S.LOADING], S.FAILED),
        ],
    )
    def test_a_group_of_chunks_shows_its_most_telling_state(
        self, states: list[ChunkState], shown: ChunkState
    ) -> None:
        assert bucket_state(states) is shown


class TestChunkMap:
    def test_few_chunks_get_one_cell_each(self) -> None:
        text, per_cell = chunk_map([S.LOADED, S.PENDING, S.FAILED], width=10, max_rows=4)

        assert (text.plain, per_cell) == ("■■■", 1)
        assert [str(span.style) for span in text.spans] == [
            STATE_STYLE[S.LOADED],
            STATE_STYLE[S.PENDING],
            STATE_STYLE[S.FAILED],
        ]

    def test_rows_wrap_at_the_width(self) -> None:
        text, per_cell = chunk_map([S.PENDING] * 25, width=10, max_rows=4)

        assert text.plain.split("\n") == ["■" * 10, "■" * 10, "■" * 5]
        assert per_cell == 1

    def test_many_chunks_share_cells_so_the_map_never_outgrows_its_space(self) -> None:
        text, per_cell = chunk_map([S.PENDING] * 1000, width=10, max_rows=4)

        rows = text.plain.split("\n")
        assert len(rows) == 4
        assert per_cell == 25
        assert sum(len(row) for row in rows) == 40

    def test_a_shared_cell_shows_trouble_even_if_most_of_its_chunks_are_fine(self) -> None:
        states = [S.LOADED] * 99 + [S.FAILED] + [S.LOADED] * 100
        text, per_cell = chunk_map(states, width=2, max_rows=1)

        assert per_cell == 100
        assert [str(span.style) for span in text.spans] == [
            STATE_STYLE[S.FAILED],
            STATE_STYLE[S.LOADED],
        ]

    def test_cells_can_be_spaced_apart_so_they_stay_distinct(self) -> None:
        text, per_cell = chunk_map([S.PENDING] * 7, width=10, max_rows=4, gap=1)

        assert text.plain.split("\n") == ["■ ■ ■ ■ ■", "■ ■"]
        assert per_cell == 1

    def test_spacing_counts_against_the_space_the_map_may_use(self) -> None:
        text, per_cell = chunk_map([S.PENDING] * 100, width=10, max_rows=2, gap=1)

        assert per_cell == 10, "5 cells per row, 2 rows: 10 cells for 100 chunks"
        assert len(text.plain.split("\n")) == 2

    def test_no_chunks_draws_nothing(self) -> None:
        text, per_cell = chunk_map([], width=10, max_rows=4)

        assert (text.plain, per_cell) == ("", 1)

    def test_every_state_has_a_style(self) -> None:
        assert set(STATE_STYLE) == set(ChunkState)


def tracked(chunks: int = 10) -> JobTracker:
    tracker = JobTracker()
    tracker.apply(
        PlanReady(
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
    )
    return tracker


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
    staged: int = 0,
    duplicates: int = 0,
) -> Snapshot:
    counts = dict.fromkeys(ChunkState, 0)
    counts[ChunkState.LOADED] = loaded
    counts[ChunkState.FAILED] = failed
    counts[ChunkState.PENDING] = total - loaded - failed
    return Snapshot(
        "job-1", elapsed, counts, rows_extracted, rows_loaded, duplicates, staged, used, capacity
    )


class TestOverall:
    def test_it_shows_the_bar_the_percentage_and_the_counts(self) -> None:
        tracker = tracked(10)
        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(4.0, loaded=5, rows_loaded=500))

        text = overall_text(tracker, width=60).plain

        assert "50%" in text
        assert "5/10 chunks" in text
        assert "█" in text
        assert "░" in text
        assert "elapsed 0:04" in text
        assert "eta 0:04" in text
        assert "500 rows loaded" in text

    def test_failures_are_called_out(self) -> None:
        tracker = tracked(10)
        tracker.apply(snapshot(1.0, loaded=3, failed=2))

        assert "2 failed" in overall_text(tracker, width=60).plain

    def test_no_failures_means_no_failure_wording(self) -> None:
        tracker = tracked(10)
        tracker.apply(snapshot(1.0, loaded=3))

        assert "failed" not in overall_text(tracker, width=60).plain

    def test_before_the_first_snapshot_it_is_a_zero_bar(self) -> None:
        text = overall_text(tracked(10), width=60).plain

        assert "0%" in text
        assert "0/10 chunks" in text
        assert "eta --" in text


class TestThroughput:
    def test_each_stage_shows_its_rate_and_a_sparkline(self) -> None:
        tracker = tracked()
        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(1.0, rows_extracted=480_000, rows_loaded=455_000))

        lines = throughput_text(tracker, width=60).plain.split("\n")

        assert lines[0].startswith("extract")
        assert "480k rows/s" in lines[0]
        assert lines[1].startswith("load")
        assert "455k rows/s" in lines[1]
        assert any(block in lines[0] for block in "▁▂▃▄▅▆▇█")

    def test_the_sparklines_are_coloured_by_stage(self) -> None:
        tracker = tracked()
        tracker.apply(snapshot(0.0))
        tracker.apply(snapshot(1.0, rows_extracted=1000, rows_loaded=1000))

        styles = {str(span.style) for span in throughput_text(tracker, width=60).spans}

        assert {"yellow", "green"} <= styles


class TestStaging:
    def test_a_bounded_store_shows_how_full_it_is(self) -> None:
        tracker = tracked()
        tracker.apply(snapshot(1.0, used=1 << 30, capacity=4 << 30, staged=3 << 30))

        text = staging_text(tracker, width=50).plain

        assert "25%" in text
        assert "1.0 GiB / 4.0 GiB" in text
        assert "3.0 GiB staged so far" in text

    def test_an_unbounded_store_shows_what_it_holds(self) -> None:
        tracker = tracked()
        tracker.apply(snapshot(1.0, used=2048, capacity=None))

        text = staging_text(tracker, width=50).plain

        assert "2.0 KiB held" in text
        assert "/" not in text.split("staged")[0]

    def test_a_store_that_cannot_tell_says_so(self) -> None:
        tracker = tracked()
        tracker.apply(snapshot(1.0))

        assert "not reported" in staging_text(tracker, width=50).plain

    def test_repeats_are_mentioned_only_when_there_were_some(self) -> None:
        tracker = tracked()
        tracker.apply(snapshot(1.0, duplicates=2))
        assert "2 repeated chunks" in staging_text(tracker, width=50).plain

        quiet = tracked()
        quiet.apply(snapshot(1.0))
        assert "repeated" not in staging_text(quiet, width=50).plain


class TestWorkers:
    def test_busy_workers_show_their_chunk_and_idle_ones_say_so(self) -> None:
        tracker = tracked()
        tracker.apply(snapshot(10.0))
        tracker.apply(ChunkStarted("c004", Stage.EXTRACT, "extractor-0"))
        tracker.apply(snapshot(13.0))
        tracker.apply(ChunkStarted("c001", Stage.LOAD, "loader-0"))
        tracker.apply(ChunkStarted("c002", Stage.LOAD, "loader-1"))
        tracker.apply(ChunkFinished("c002", Stage.LOAD, "loader-1", rows=1, bytes=1, seconds=0.1))

        lines = workers_text(tracker).plain.split("\n")

        assert lines[0].split() == ["extractor-0", "extract", "c004", "3s"]
        assert lines[1].split() == ["loader-0", "load", "c001", "0s"]
        assert lines[2].split() == ["loader-1", "idle"]

    def test_no_workers_yet_is_a_quiet_message(self) -> None:
        assert "waiting" in workers_text(tracked()).plain


class TestActivity:
    def test_it_counts_the_busy_extractors_and_loaders_against_the_configured_numbers(self) -> None:
        tracker = tracked()
        tracker.apply(ChunkStarted("c000", Stage.EXTRACT, "extractor-0"))
        tracker.apply(ChunkStarted("c001", Stage.EXTRACT, "extractor-1"))
        tracker.apply(ChunkStarted("c002", Stage.LOAD, "loader-0"))

        lines = activity_text(tracker, extractors=4, loaders=3).plain.split("\n")

        assert lines[0].split() == ["extracting", "2", "of", "4", "busy"]
        assert lines[1].split() == ["loading", "1", "of", "3", "busy"]

    def test_before_the_job_starts_nothing_is_busy(self) -> None:
        lines = activity_text(JobTracker(), extractors=2, loaders=2).plain.split("\n")

        assert lines[0].split() == ["extracting", "0", "of", "2", "busy"]
        assert lines[1].split() == ["loading", "0", "of", "2", "busy"]

    def test_the_containers_line_says_that_every_worker_runs_in_this_process(self) -> None:
        text = activity_text(tracked(), extractors=1, loaders=1).plain

        assert text.split("\n")[2].split()[:2] == ["containers", "1"]
        assert "this process" in text


def test_text_is_rich_text() -> None:
    assert isinstance(bar(0.5, 4), Text)
