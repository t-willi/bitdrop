"""Opening a component from a mistyped URL must not print the password that was in it."""

from collections.abc import Callable

import pytest

from bitdrop_core.registry import (
    open_chunk_store,
    open_destination,
    open_metadata_store,
    open_source,
)

OPENERS = [open_source, open_destination, open_chunk_store, open_metadata_store]


@pytest.mark.parametrize("opener", OPENERS)
def test_a_url_without_a_scheme_is_rejected_unrepeated(opener: Callable[..., object]) -> None:
    with pytest.raises(ValueError, match="scheme") as caught:
        opener("alice:s3cret@db.example.com/shop")

    assert "s3cret" not in str(caught.value)
