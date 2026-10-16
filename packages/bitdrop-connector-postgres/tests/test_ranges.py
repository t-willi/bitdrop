"""Key-range arithmetic for splitting a table into chunks (no database needed)."""

import pytest

from bitdrop_connector_postgres._ranges import split_range


def covered(ranges: list[tuple[int, int]]) -> list[int]:
    return [key for lo, hi in ranges for key in range(lo, hi + 1)]


class TestSplitRange:
    def test_dense_keys_get_ranges_of_the_wanted_row_count(self) -> None:
        ranges, note = split_range(1, 100, rows_per_chunk=25, estimated_rows=100)

        assert ranges == [(1, 25), (26, 50), (51, 75), (76, 100)]
        assert note is None

    def test_the_last_range_is_shorter(self) -> None:
        ranges, _ = split_range(1, 10, rows_per_chunk=4, estimated_rows=10)

        assert ranges == [(1, 4), (5, 8), (9, 10)]

    @pytest.mark.parametrize("lo", [-50, 0, 1, 1000])
    @pytest.mark.parametrize("rows_per_chunk", [1, 3, 7, 1000])
    def test_ranges_are_disjoint_ordered_and_cover_everything(
        self, lo: int, rows_per_chunk: int
    ) -> None:
        hi = lo + 99

        ranges, _ = split_range(lo, hi, rows_per_chunk=rows_per_chunk, estimated_rows=100)

        assert covered(ranges) == list(range(lo, hi + 1))

    def test_sparse_keys_get_wider_ranges_to_hold_the_wanted_rows(self) -> None:
        """100 rows over 10,000 ids: a range of 25 rows must span about 2,500 ids."""
        ranges, _ = split_range(1, 10_000, rows_per_chunk=25, estimated_rows=100)

        assert len(ranges) == 4

    def test_a_single_key(self) -> None:
        assert split_range(5, 5, rows_per_chunk=10, estimated_rows=1)[0] == [(5, 5)]

    def test_the_extremes_of_bigint_neither_overflow_nor_explode(self) -> None:
        low, high = -(2**63), 2**63 - 1

        ranges, _ = split_range(low, high, rows_per_chunk=1, estimated_rows=5)

        assert len(ranges) == 5
        assert ranges[0][0] == low
        assert ranges[-1][1] == high
        assert all(-(2**63) <= a <= b <= 2**63 - 1 for a, b in ranges)

    def test_the_number_of_chunks_is_capped_and_the_cap_is_reported(self) -> None:
        ranges, note = split_range(
            1, 10_000, rows_per_chunk=1, estimated_rows=10_000, max_chunks=100
        )

        assert len(ranges) == 100
        assert note is not None
        assert "100" in note
        assert covered(ranges) == list(range(1, 10_001))

    def test_unknown_row_counts_fall_back_to_dense_keys(self) -> None:
        ranges, _ = split_range(1, 100, rows_per_chunk=50, estimated_rows=0)

        assert ranges == [(1, 50), (51, 100)]

    def test_an_inverted_range_is_a_bug(self) -> None:
        with pytest.raises(ValueError, match="lo"):
            split_range(10, 1, rows_per_chunk=5, estimated_rows=5)
