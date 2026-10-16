"""Connection URLs and a small thread-backed connection pool.

ADBC connections are synchronous, so every call runs in a worker thread (``asyncio.to_thread``;
the driver releases the GIL while it works). At most ``pool_size`` calls run at once, each on its
own connection. Connections are created on first use and reused.
"""

import asyncio
import threading
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import adbc_driver_manager
import adbc_driver_postgresql.dbapi as adbc

from bitdrop_api.errors import ConnectorError

DEFAULT_POOL_SIZE = 4
_BITDROP_OPTIONS = ("bitdrop_pool_size",)
_SCHEMES = ("postgresql", "postgres")


@dataclass(frozen=True, slots=True)
class ConnectionSettings:
    """A parsed connection URL."""

    uri: str
    """What libpq gets: the URL without the ``bitdrop_*`` options."""

    pool_size: int

    redacted: str
    """Safe to log and to put in error messages: passwords replaced by ``***``."""


def parse_connection_url(url: str) -> ConnectionSettings:
    """Split a ``postgresql://`` URL into libpq's part and bitdrop's own options.

    Options named ``bitdrop_*`` are consumed here; everything else goes to libpq untouched
    (``sslmode``, ``application_name``, ...). Unknown ``bitdrop_*`` options are an error, so a typo
    cannot silently fall back to a default.

    Raises:
        ValueError: wrong scheme, or a bad ``bitdrop_*`` option.
    """
    parts = urlsplit(url)
    if parts.scheme not in _SCHEMES:
        raise ValueError(f"expected a postgresql:// URL, got scheme {parts.scheme!r}")

    libpq_options: list[tuple[str, str]] = []
    pool_size = DEFAULT_POOL_SIZE
    for name, value in parse_qsl(parts.query, keep_blank_values=True):
        if not name.startswith("bitdrop_"):
            libpq_options.append((name, value))
        elif name == "bitdrop_pool_size":
            try:
                pool_size = int(value)
            except ValueError:
                pool_size = 0
            if pool_size < 1:
                raise ValueError(f"bitdrop_pool_size must be a positive integer, got {value!r}")
        else:
            raise ValueError(f"unknown option {name!r} (allowed: {', '.join(_BITDROP_OPTIONS)})")

    uri = urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(libpq_options, quote_via=quote),
            parts.fragment,
        )
    )
    return ConnectionSettings(uri=uri, pool_size=pool_size, redacted=_redact(uri))


def _redact(uri: str) -> str:
    parts = urlsplit(uri)
    netloc = parts.netloc
    userinfo, at, hostport = netloc.rpartition("@")
    user, colon, password = userinfo.partition(":")
    if at and colon and password:
        netloc = f"{user}:***@{hostport}"
    query = urlencode(
        [
            (k, "***" if k.lower() == "password" else v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
        ],
        safe="*",
        quote_via=quote,
    )
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


class ConnectionPool:
    """Runs blocking database work on pooled connections, in worker threads."""

    def __init__(self, settings: ConnectionSettings, *, autocommit: bool) -> None:
        """Create an empty pool. ``autocommit=False`` makes callers commit explicitly."""
        self._settings = settings
        self._autocommit = autocommit
        self._slots = asyncio.Semaphore(settings.pool_size)
        self._lock = threading.Lock()
        self._idle: list[adbc.Connection] = []

    async def run[T](self, work: Callable[[adbc.Connection], T]) -> T:
        """Run ``work(connection)`` on a pooled connection in a worker thread.

        ``work`` commits its own transaction when the pool is not autocommit. A connection that
        raised is closed instead of reused, since its state is unknown.
        """
        async with self._slots:
            return await asyncio.to_thread(self._run, work)

    async def aclose(self) -> None:
        """Close all idle connections."""
        await asyncio.to_thread(self._close_idle)

    def _run[T](self, work: Callable[[adbc.Connection], T]) -> T:
        connection = self._checkout()
        try:
            result = work(connection)
        except adbc_driver_manager.Error as error:
            with suppress(Exception):
                connection.close()
            raise ConnectorError(
                f"{self._settings.redacted}: {type(error).__name__}: {error}"
            ) from error
        except BaseException:
            with suppress(Exception):
                connection.close()
            raise
        if not self._autocommit:
            connection.rollback()  # end any transaction `work` left open; harmless after a commit
        with self._lock:
            self._idle.append(connection)
        return result

    def _checkout(self) -> adbc.Connection:
        with self._lock:
            if self._idle:
                return self._idle.pop()
        try:
            return adbc.connect(self._settings.uri, autocommit=self._autocommit)
        except Exception as error:
            raise ConnectorError(f"cannot connect to {self._settings.redacted}: {error}") from error

    def _close_idle(self) -> None:
        with self._lock:
            idle, self._idle = self._idle, []
        for connection in idle:
            with suppress(Exception):
                connection.close()
