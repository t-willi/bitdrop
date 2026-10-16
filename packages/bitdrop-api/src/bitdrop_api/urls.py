"""URL-based configuration of components.

Every component is selected and configured by one URL, for example ``memory://?max_bytes=1000`` or
``file:///dev/shm/bitdrop``. The scheme picks the adapter; the rest is that adapter's own
configuration. This module gives adapters one shared, strict way to read such URLs:

* :func:`parse_url` for local components (no host part), and
* :func:`parse_network_url` for services reached over the network (``redis://user:pw@host/0``),
  whose URL is mostly the client library's business.
"""

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit


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


OPTION_PREFIX = "bitdrop_"
"""Query options starting with this belong to bitdrop; all others belong to the client library."""


@dataclass(frozen=True, slots=True)
class NetworkUrl:
    """A connection URL split into the client library's part and bitdrop's own options."""

    url: str
    """What the client library gets: the URL without the ``bitdrop_*`` options."""

    redacted: str
    """Safe to log and to put in error messages: passwords replaced by ``***``."""

    options: Mapping[str, str]
    """The ``bitdrop_*`` options that were present, with their raw values."""

    def int_option(self, name: str, *, default: int, minimum: int = 1) -> int:
        """Return option ``name`` as an integer of at least ``minimum``, or ``default`` if absent.

        Raises:
            ValueError: the value is not an integer, or is below ``minimum``.
        """
        raw = self.options.get(name)
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError:
            value = minimum - 1
        if value < minimum:
            raise ValueError(f"{name} must be an integer of at least {minimum}, got {raw!r}")
        return value

    def str_option(self, name: str, *, default: str) -> str:
        """Return option ``name`` as given, or ``default`` if absent."""
        return self.options.get(name, default)


def parse_network_url(
    url: str, *, schemes: Collection[str], options: Collection[str]
) -> NetworkUrl:
    """Split a network service URL into the client library's part and bitdrop's own options.

    Query options named ``bitdrop_*`` are consumed here (they must be listed in ``options``); all
    others go to the client library untouched. A misspelled ``bitdrop_*`` option is an error, so a
    typo cannot silently fall back to a default.

    Raises:
        ValueError: the scheme is not one of ``schemes``, or a ``bitdrop_*`` option is unknown.
    """
    parts = urlsplit(url)
    if parts.scheme not in schemes:
        raise ValueError(f"expected a {next(iter(schemes))}:// URL, got scheme {parts.scheme!r}")

    client_options: list[tuple[str, str]] = []
    bitdrop_options: dict[str, str] = {}
    for name, value in parse_qsl(parts.query, keep_blank_values=True):
        if not name.startswith(OPTION_PREFIX):
            client_options.append((name, value))
        elif name in options:
            bitdrop_options[name] = value
        else:
            raise ValueError(f"unknown option {name!r} (allowed: {', '.join(sorted(options))})")

    client_url = urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(client_options, quote_via=quote),
            parts.fragment,
        )
    )
    return NetworkUrl(url=client_url, redacted=_redact(client_url), options=bitdrop_options)


def _redact(url: str) -> str:
    """Replace passwords (in the user info and in a ``password`` query option) with ``***``."""
    parts = urlsplit(url)
    userinfo, at, hostport = parts.netloc.rpartition("@")
    user, colon, password = userinfo.partition(":")
    netloc = f"{user}:***@{hostport}" if at and colon and password else parts.netloc
    query = urlencode(
        [
            (name, "***" if name.lower() == "password" else value)
            for name, value in parse_qsl(parts.query, keep_blank_values=True)
        ],
        safe="*",
        quote_via=quote,
    )
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))
