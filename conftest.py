"""Repository-wide pytest configuration and shared fixtures."""

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

# The conformance kits live in an importable package, not in a test module. Registering them here
# (before they are imported) makes pytest show detailed assertion diffs for their checks too.
pytest.register_assert_rewrite("bitdrop_api.testing")

POSTGRES_IMAGE = "postgres:17-alpine"

# Servers speaking the Redis protocol. Persistence is off, as it would be for a data-plane node:
# the source is the durable copy, and a fork for snapshots could double memory use.
_REDIS_ARGS = ["--save", "", "--appendonly", "no", "--maxmemory-policy", "noeviction"]
KV_SERVERS: dict[str, tuple[str, list[str]]] = {
    "redis": ("redis:7.4-alpine", ["redis-server", *_REDIS_ARGS]),
    "valkey": ("valkey/valkey:8.1-alpine", ["valkey-server", *_REDIS_ARGS]),
    "dragonfly": (
        "docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2",
        ["dragonfly", "--dbfilename=", "--proactor_threads=2"],
    ),
}
KV_FLAVORS = tuple(KV_SERVERS)


@dataclass(frozen=True)
class PostgresServer:
    """A throwaway PostgreSQL server running in a container for the whole test session."""

    host: str
    port: int
    user: str
    password: str

    def uri(self, database: str) -> str:
        """Return a connection URL for ``database`` on this server."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{database}"

    def create_database(self) -> str:
        """Create an empty database with a unique name and return its connection URL."""
        import adbc_driver_postgresql.dbapi as adbc

        name = f"bitdrop_{uuid.uuid4().hex[:12]}"
        with (
            adbc.connect(self.uri("postgres"), autocommit=True) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(f'CREATE DATABASE "{name}"')
        return self.uri(name)


@pytest.fixture(scope="session")
def postgres_server() -> Iterator[PostgresServer]:
    """Start one PostgreSQL container for the session; skip the tests if Docker is unavailable.

    Tests using this fixture should be marked ``@pytest.mark.docker``.
    """
    try:
        from testcontainers.community.postgres import PostgresContainer

        container = PostgresContainer(POSTGRES_IMAGE, driver=None)
        container.start()
    except Exception as error:
        pytest.skip(f"cannot start a PostgreSQL container ({type(error).__name__}: {error})")

    try:
        yield PostgresServer(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(5432)),
            user=container.username,
            password=container.password,
        )
    finally:
        container.stop()


@pytest.fixture
def postgres_url(postgres_server: PostgresServer) -> str:
    """Return the URL of a fresh, empty database (its own database on the shared server)."""
    return postgres_server.create_database()


SAMPLE_TABLE_DDL = (
    "CREATE TABLE {table} (id bigint NOT NULL PRIMARY KEY, name text, score double precision,"
    " amount numeric(12,2), active boolean, born date, seen timestamptz, raw bytea)"
)
"""A Postgres table matching ``SAMPLE_SCHEMA``."""


class PostgresClient:
    """Tiny helper for test setup and inspection: one short-lived connection per call."""

    def execute(self, url: str, sql: str) -> None:
        """Run one statement and commit."""
        import adbc_driver_postgresql.dbapi as adbc

        with adbc.connect(url, autocommit=True) as connection, connection.cursor() as cursor:
            cursor.execute(sql)

    def query(self, url: str, sql: str) -> list[tuple[Any, ...]]:
        """Run a query and return its rows as tuples."""
        import adbc_driver_postgresql.dbapi as adbc

        with adbc.connect(url, autocommit=True) as connection, connection.cursor() as cursor:
            cursor.execute(sql)
            rows: list[tuple[Any, ...]] = cursor.fetchall()
            return rows

    def scalar(self, url: str, sql: str) -> Any:
        """Run a query that returns one value."""
        return self.query(url, sql)[0][0]

    def load(self, url: str, table: str, data: pa.Table) -> None:
        """Append an Arrow table to an existing table."""
        import adbc_driver_postgresql.dbapi as adbc

        with adbc.connect(url) as connection:
            schema, _, name = table.rpartition(".")
            with connection.cursor() as cursor:
                cursor.adbc_ingest(name, data, mode="append", db_schema_name=schema or None)
            connection.commit()

    def create_sample_table(self, url: str, table: str, rows: int = 0) -> None:
        """Create a table shaped like ``SAMPLE_SCHEMA`` and fill it with ``rows`` rows."""
        from bitdrop_api.testing import make_rows

        self.execute(url, SAMPLE_TABLE_DDL.format(table=table))
        if rows:
            self.load(url, table, make_rows(1, rows))

    def read_sample(self, url: str, table: str) -> pa.Table:
        """Read a sample-shaped table back as a table with exactly ``SAMPLE_SCHEMA``."""
        import adbc_driver_postgresql.dbapi as adbc

        from bitdrop_api.testing import SAMPLE_SCHEMA

        with adbc.connect(url, autocommit=True) as connection, connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table} ORDER BY id")
            raw = cursor.fetch_arrow_table()
        columns = []
        for field in SAMPLE_SCHEMA:
            column: Any = raw.column(field.name)
            if pa.types.is_decimal(field.type):  # numeric arrives as an opaque string extension
                column = pa.chunked_array(  # type: ignore[call-overload]
                    [pc.cast(chunk.storage, field.type) for chunk in column.chunks], type=field.type
                )
            columns.append(column)
        return pa.Table.from_arrays(columns, schema=SAMPLE_SCHEMA)

    def column_types(self, url: str, table: str) -> list[tuple[str, str]]:
        """Return ``(column, format_type)`` for every column of ``table``, in order."""
        rows = self.query(
            url,
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a"
            f" WHERE a.attrelid = '{table}'::regclass AND a.attnum > 0 AND NOT a.attisdropped"
            " ORDER BY a.attnum",
        )
        return [(str(name), str(type_)) for name, type_ in rows]


@pytest.fixture
def pg() -> PostgresClient:
    """A helper for creating, filling, and inspecting tables in test databases."""
    return PostgresClient()


@dataclass(frozen=True)
class KvServer:
    """A throwaway server speaking the Redis protocol, running in a container."""

    flavor: str
    host: str
    port: int
    password: str | None = None

    @property
    def url(self) -> str:
        """Connection URL for database 0."""
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/0"

    def client(self) -> Any:
        """Return a synchronous client for setup and inspection in tests."""
        import redis

        return redis.Redis.from_url(self.url)


class KvServers:
    """Starts Redis-protocol servers on demand and stops them all at the end of the session."""

    def __init__(self) -> None:
        self._running: dict[tuple[str, tuple[str, ...], str | None], tuple[Any, KvServer]] = {}

    def get(self, flavor: str, *extra_args: str, password: str | None = None) -> KvServer:
        """Return a running server of ``flavor`` (started on first use, then reused).

        ``extra_args`` are appended to the server command line; they select a separate instance.
        """
        key = (flavor, extra_args, password)
        if key not in self._running:
            self._running[key] = self._start(flavor, extra_args, password)
        return self._running[key][1]

    def stop_all(self) -> None:
        """Stop and remove every container started through this object."""
        for container, _ in self._running.values():
            container.stop()
        self._running.clear()

    @staticmethod
    def _start(
        flavor: str, extra_args: tuple[str, ...], password: str | None
    ) -> tuple[Any, KvServer]:
        import time

        import redis
        from testcontainers.core.container import DockerContainer

        image, command = KV_SERVERS[flavor]
        command = [*command, *extra_args]
        if password:
            command += ["--requirepass", password]
        container = DockerContainer(image).with_exposed_ports(6379).with_command(command)
        if flavor == "dragonfly":
            container = container.with_kwargs(ulimits=[{"Name": "memlock", "Soft": -1, "Hard": -1}])
        try:
            container.start()
        except Exception as error:
            pytest.skip(f"cannot start a {flavor} container ({type(error).__name__}: {error})")

        server = KvServer(
            flavor,
            container.get_container_host_ip(),
            int(container.get_exposed_port(6379)),
            password,
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                server.client().ping()
                return container, server
            except redis.exceptions.RedisError:
                if time.monotonic() > deadline:
                    container.stop()
                    pytest.skip(f"{flavor} did not become ready within 30 seconds")
                time.sleep(0.2)


@pytest.fixture(scope="session")
def kv_servers() -> Iterator[KvServers]:
    """Redis-protocol servers (redis, valkey, dragonfly), started on demand.

    Tests using this fixture should be marked ``@pytest.mark.docker``.
    """
    servers = KvServers()
    yield servers
    servers.stop_all()


@pytest.fixture(params=KV_FLAVORS)
def kv(request: pytest.FixtureRequest, kv_servers: KvServers) -> KvServer:
    """A running Redis-protocol server, emptied before the test.

    Parametrized: every test requesting it runs once per server flavor (Redis, Valkey, Dragonfly).
    """
    server = kv_servers.get(request.param)
    server.client().flushall()
    return server
