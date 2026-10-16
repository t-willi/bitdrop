"""The key rules every chunk store has to honour.

One shared rule keeps keys portable: the same key must be valid in Redis, memcached, and as a file
name. Enforcing it in one place (instead of per adapter) means a key that works with one adapter
works with all of them.
"""

import re

from bitdrop_api.errors import InvalidKey

MAX_KEY_LENGTH = 200
"""Maximum key length. Well below memcached's 250 bytes and common file name limits of 255."""

# Must start with a letter or digit: this rules out "..", hidden files, and leaves names starting
# with "." free for adapters' temporary files. ``fullmatch`` also rejects a trailing newline.
_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*")


def validate_key(key: str) -> str:
    """Return ``key`` unchanged if it is valid.

    A valid key is 1 to :data:`MAX_KEY_LENGTH` characters from ``A-Z a-z 0-9 . _ : -`` and starts
    with a letter or digit.

    Raises:
        InvalidKey: if the key is not a string or breaks any of the rules above.
    """
    if not isinstance(key, str):
        raise InvalidKey(repr(key), f"expected str, got {type(key).__name__}")
    if len(key) > MAX_KEY_LENGTH:
        raise InvalidKey(key, f"longer than {MAX_KEY_LENGTH} characters")
    if _KEY_PATTERN.fullmatch(key) is None:
        raise InvalidKey(key, "must match [A-Za-z0-9][A-Za-z0-9._:-]*")
    return key
