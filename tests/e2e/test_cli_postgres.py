"""The ``bitdrop`` command as a person runs it, between two real PostgreSQL databases (Docker).

Each test starts the command as a separate process, with the connection URLs only in the
environment, and checks the result in the databases: a fingerprint of every row (row count plus an md5
over each row's text, in key order). State lives in files (SQLite for the chunk states, a directory
for the staged chunks), so a second process can pick a job up where the first one stopped.
"""

import json
import os
import select
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from conftest import PostgresClient, PostgresServer

pytestmark = pytest.mark.docker

JOB = """
[job]
id = "people-1"

[source]
url = "${{SOURCE_URL}}"
table = "people"

[destination]
url = "${{DESTINATION_URL}}"
table = "people"

[staging]
url = "file://{staging}"

[metadata]
url = "sqlite://{state}"

[tuning]
chunk_mib = 0.1
extractors = 2
loaders = 2
"""


class Run:
    """One ``bitdrop`` process whose output can be read while it works."""

    def __init__(self, job: Path, env: dict[str, str], *args: str) -> None:
        """Start ``bitdrop run JOB ARGS``, or ``bitdrop status JOB ARGS`` if ``args`` begin so."""
        command, *rest = args if args[:1] == ("status",) else ("run", *args)
        self.process = subprocess.Popen(
            [sys.executable, "-m", "bitdrop_cli", command, str(job), *rest],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, **env},
        )

    def events(self, *, timeout: float = 60) -> Iterator[dict[str, Any]]:
        """Yield the JSON lines the process prints, as they arrive."""
        assert self.process.stdout is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ready, _, _ = select.select([self.process.stdout], [], [], 0.2)
            if ready:
                line = self.process.stdout.readline()
                if not line:
                    return
                yield json.loads(line)
            elif self.process.poll() is not None:
                return
        raise AssertionError("the process went quiet")

    def finish(self, timeout: float = 120) -> tuple[int, str, str]:
        stdout, stderr = self.process.communicate(timeout=timeout)
        return self.process.returncode, stdout, stderr


def fingerprint(pg: PostgresClient, url: str, table: str) -> tuple[int, str]:
    ((count, digest),) = pg.query(
        url, f"SELECT count(*), md5(string_agg(t::text, '|' ORDER BY t.id)) FROM {table} t"
    )
    return int(count), str(digest)


@pytest.fixture
def setup(tmp_path: Path, postgres_server: PostgresServer) -> tuple[Path, dict[str, str], str, str]:
    """A job file, the environment that carries the URLs, and the two database URLs."""
    source_url = postgres_server.create_database()
    destination_url = postgres_server.create_database()
    job = tmp_path / "job.toml"
    job.write_text(JOB.format(staging=tmp_path / "staging", state=tmp_path / "state.db"))
    env = {"SOURCE_URL": source_url, "DESTINATION_URL": destination_url}
    return job, env, source_url, destination_url


def test_the_command_copies_a_table_and_every_row_matches(
    setup: tuple[Path, dict[str, str], str, str],
    pg: PostgresClient,
    postgres_server: PostgresServer,
) -> None:
    job, env, source_url, destination_url = setup
    pg.create_sample_table(source_url, "people", rows=20_000)

    code, stdout, stderr = Run(job, env, "--ui", "json").finish()

    assert code == 0, stderr
    report = [json.loads(line) for line in stdout.splitlines()][-1]
    assert report["event"] == "report"
    assert report["succeeded"] is True
    assert report["rows_loaded"] == 20_000
    assert fingerprint(pg, destination_url, "people") == fingerprint(pg, source_url, "people")
    assert pg.column_types(destination_url, "people") == pg.column_types(source_url, "people")
    for stream in (stdout, stderr):
        assert postgres_server.password not in stream, "a password reached the output"


def test_ctrl_c_then_the_same_command_finishes_the_job_without_a_duplicate_row(
    setup: tuple[Path, dict[str, str], str, str], pg: PostgresClient
) -> None:
    """Stop a real load part-way with Ctrl-C, then run the very same command again."""
    job, env, source_url, destination_url = setup
    pg.create_sample_table(source_url, "people", rows=30_000)
    # The destination is slow on purpose: every COPY into it takes 0.4 s, so there is time to stop.
    pg.create_sample_table(destination_url, "people", rows=0)
    pg.execute(
        destination_url,
        "CREATE FUNCTION slow_copy() RETURNS trigger LANGUAGE plpgsql AS"
        " $$ BEGIN PERFORM pg_sleep(0.4); RETURN NULL; END $$",
    )
    pg.execute(
        destination_url,
        "CREATE TRIGGER slow BEFORE INSERT ON people FOR EACH STATEMENT EXECUTE FUNCTION slow_copy()",
    )

    first = Run(job, env, "--ui", "json")
    for event in first.events():
        if event["event"] == "progress" and event["chunks"]["loaded"] >= 4:
            first.process.send_signal(signal.SIGINT)
            break
    code, _, stderr = first.finish()

    assert code == 130, stderr
    partial = pg.scalar(destination_url, "SELECT count(*) FROM people")
    assert 0 < partial < 30_000, f"{partial} rows after the stop: the test proves nothing"
    status = Run(job, env, "status", "--json")
    status_code, out, err = status.finish()
    chunks = json.loads(out)["chunks"]
    assert status_code == 0, err
    assert chunks["extracting"] == chunks["loading"] == 0, "the stop left chunks held"
    assert chunks["failed"] == 0, "the stop cost attempts"

    second = Run(job, env, "--ui", "json")
    code, stdout, stderr = second.finish()

    assert code == 0, stderr
    assert json.loads(stdout.splitlines()[-1])["succeeded"] is True
    assert pg.scalar(destination_url, "SELECT count(*) FROM people") == 30_000
    assert fingerprint(pg, destination_url, "people") == fingerprint(pg, source_url, "people")
