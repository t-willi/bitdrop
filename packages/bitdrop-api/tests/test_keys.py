"""Chunk keys must be safe for every backend: Redis keys, memcached keys, and file names."""

import pytest

from bitdrop_api.errors import InvalidKey
from bitdrop_api.keys import MAX_KEY_LENGTH, validate_key


@pytest.mark.parametrize(
    "key",
    [
        "a",
        "A1",
        "job:42:table:orders:chunk:000007",
        "with.dots_and-dashes",
        "a" * MAX_KEY_LENGTH,
    ],
)
def test_valid_keys_are_returned_unchanged(key: str) -> None:
    assert validate_key(key) == key


@pytest.mark.parametrize(
    "key",
    [
        "",  # empty
        " ",  # whitespace only
        "has space",
        "has/slash",  # would be a path separator for the file adapter
        "has\\backslash",
        ".hidden",  # must not start with a dot: reserved for adapter temp files, avoids ".."
        "..",
        "-leading-dash",
        ":leading-colon",
        "trailing-newline\n",
        "ünïcode",
        "a" * (MAX_KEY_LENGTH + 1),
    ],
)
def test_invalid_keys_are_rejected(key: str) -> None:
    with pytest.raises(InvalidKey) as excinfo:
        validate_key(key)

    assert excinfo.value.key == key


def test_non_string_keys_are_rejected() -> None:
    with pytest.raises(InvalidKey):
        validate_key(b"bytes-are-not-keys")  # type: ignore[arg-type]
