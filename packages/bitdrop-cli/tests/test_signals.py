"""Ctrl-C and SIGTERM stop a real ``bitdrop run`` process cleanly.

These start the command as a subprocess and send it the signal, because that is the only way to
know the signal handling works in the process a person actually runs. The job state lives in a file,
so a second process (``bitdrop status``) can see what the first left behind.
"""

import json
import signal
import subprocess
import sys
from pathlib import Path

import pytest

JOB = """
[job]
id = "slow-1"

[source]
url = "memory://?demo_rows=400000&delay_ms=300"
table = "demo"

[destination]
url = "memory://"
table = "orders"

[staging]
url = "memory://"

[metadata]
url = "sqlite://{state}"

[tuning]
chunk_mib = 0.1
extractors = 2
loaders = 2
lease_seconds = 60
"""


def bitdrop(*args: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-m", "bitdrop_cli", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


@pytest.mark.parametrize("received", [signal.SIGINT, signal.SIGTERM], ids=["ctrl-c", "sigterm"])
def test_a_signal_stops_the_job_and_hands_every_chunk_back(
    tmp_path: Path, received: signal.Signals
) -> None:
    state = tmp_path / "state.db"
    job = tmp_path / "job.toml"
    job.write_text(JOB.format(state=state))
    process = bitdrop("run", str(job), "--ui", "plain")
    assert process.stderr is not None
    process.stderr.readline()  # the plan line: the job is set up and about to work
    process.send_signal(received)

    stdout, stderr = process.communicate(timeout=30)

    assert process.returncode == 130, stderr
    assert "stopped" in stderr
    assert "resume" in stderr
    assert stdout == "", "a job that did not finish prints no summary"

    status = bitdrop("status", str(job), "--json")
    out, err = status.communicate(timeout=30)
    chunks = json.loads(out)["chunks"]
    assert status.returncode == 0, err
    assert chunks["extracting"] == chunks["loading"] == 0, "no chunk may stay held"
    assert chunks["failed"] == 0, "stopping on purpose must not cost attempts"
    assert chunks["loaded"] < sum(chunks.values()), "the job really was cut short"
