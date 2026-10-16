"""Numbers for people: the same wording in the plain output and in the TUI."""

import pytest

from bitdrop_core.humanize import format_bytes, format_count, format_duration, format_rate


class TestFormatBytes:
    @pytest.mark.parametrize(
        ("size", "text"),
        [
            (0, "0 B"),
            (1023, "1023 B"),
            (1024, "1.0 KiB"),
            (1536, "1.5 KiB"),
            (1024**2, "1.0 MiB"),
            (int(1.2 * 1024**3), "1.2 GiB"),
            (10 * 1024**3, "10 GiB"),
            (3 * 1024**4, "3.0 TiB"),
        ],
    )
    def test_uses_binary_units_with_two_significant_digits(self, size: int, text: str) -> None:
        assert format_bytes(size) == text

    def test_the_last_byte_of_a_unit_does_not_print_as_1024_of_it(self) -> None:
        assert format_bytes(1024**2 - 1) == "1.0 MiB"


class TestFormatCount:
    @pytest.mark.parametrize(
        ("count", "text"),
        [
            (0, "0"),
            (999, "999"),
            (1000, "1.0k"),
            (1234, "1.2k"),
            (12_345, "12k"),
            (123_456, "123k"),
            (999_999, "1.0M"),
            (1_234_567, "1.2M"),
            (12_345_678, "12M"),
            (2_000_000_000, "2.0B"),
        ],
    )
    def test_keeps_about_three_digits(self, count: int, text: str) -> None:
        assert format_count(count) == text


class TestFormatRate:
    def test_a_rate_is_a_count_per_second(self) -> None:
        assert format_rate(480_000) == "480k rows/s"

    def test_a_rate_below_one_still_reads_naturally(self) -> None:
        assert format_rate(0) == "0 rows/s"
        assert format_rate(0.4) == "0 rows/s"


class TestFormatDuration:
    @pytest.mark.parametrize(
        ("seconds", "text"),
        [
            (0, "0:00"),
            (7, "0:07"),
            (72.9, "1:12"),
            (3599, "59:59"),
            (3725, "1:02:05"),
            (90_000, "25:00:00"),
        ],
    )
    def test_minutes_and_seconds_then_hours(self, seconds: float, text: str) -> None:
        assert format_duration(seconds) == text

    def test_nonsense_is_shown_as_zero_instead_of_crashing(self) -> None:
        assert format_duration(-5) == "0:00"
        assert format_duration(float("nan")) == "0:00"
