"""The summary every display prints when a job ends."""

from bitdrop_api.metadata_store import ChunkRecord, ChunkState
from bitdrop_core.summary import summary_lines
from bitdrop_core.transfer import TransferReport


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


class TestDuration:
    def test_a_quick_job_shows_seconds_with_a_decimal(self) -> None:
        quick = TransferReport("j", 1, 1, 0, 1, 1, 0, 1, 0.94, ())

        assert "0.9 s" in "\n".join(summary_lines(quick))

    def test_a_long_job_shows_minutes_and_seconds(self) -> None:
        long = TransferReport("j", 1, 1, 0, 1, 1, 0, 1, 3725.0, ())

        assert "1:02:05" in "\n".join(summary_lines(long))
