"""URL-based configuration of components.

Every component is selected and configured by one URL, for example ``memory://?max_bytes=1000`` or
``file:///dev/shm/bitdrop``. The scheme picks the adapter; the rest is that adapter's own
configuration. This module gives adapters one shared, strict way to read such URLs.
"""

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from urllib.parse import parse_qsl, unquote, urlsplit


@dataclass(frozen=True, slots=True)
class ParsedUrl:
    """A validated component URL."""

    scheme: str
    path: str
    """The decoded path. Empty for URLs like ``memory://``."""

    params: Mapping[str, str]
    """The query parameters. Each appears once and is one of the adapter's allowed names."""

    def int_param(self, name: str) -> int | None:
        """Return parameter ``name`` as an integer, or ``None`` if the URL does not set it.

        Raises:
            ValueError: the parameter is set but is not an integer.
        """
        raw = self.params.get(name)
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError:
            raise ValueError(
                f"{self.scheme}:// parameter {name!r} must be an integer, got {raw!r}"
            ) from None


def parse_url(url: str, *, scheme: str, allowed_params: Collection[str]) -> ParsedUrl:
    """Parse and validate ``url`` for an adapter that handles ``scheme``.

    Component URLs have no host part (``scheme://?query`` or ``scheme:///path?query``).

    Raises:
        ValueError: the URL has another scheme, a host, an unknown or repeated parameter.
            The message always names the offending part.
    """
    parts = urlsplit(url)
    if parts.scheme != scheme or not url.startswith(f"{scheme}://"):
        raise ValueError(f"expected a URL with scheme {scheme!r} like {scheme}://..., got {url!r}")
    if parts.netloc:
        raise ValueError(f"{scheme}:// URLs take no host, found {parts.netloc!r} in {url!r}")

    params: dict[str, str] = {}
    for name, value in parse_qsl(parts.query, keep_blank_values=True):
        if name not in allowed_params:
            allowed = ", ".join(sorted(allowed_params)) or "none"
            raise ValueError(f"unknown parameter {name!r} for {scheme}:// (allowed: {allowed})")
        if name in params:
            raise ValueError(f"parameter {name!r} given more than once in {url!r}")
        params[name] = value

    return ParsedUrl(scheme=scheme, path=unquote(parts.path), params=params)
