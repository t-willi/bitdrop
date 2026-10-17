"""SIGTERM and Ctrl-C stop a real ``bitdrop queue`` container: it finishes what it holds and leaves.

Started as a subprocess and sent the signal, because that is the only way to know the handling
works in the process a container runs. Every component is the in-memory one, so the container has
nothing to do and simply waits for work, which is exactly the state in which Docker stops it.
"""

import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

JOB = """
[job]
id = "idle-1"

[source]
url = "memory://?demo_rows=1000"
table = "demo"

[destination]
url = "memory://"
table = "orders"

[staging]
url = "memory://"

[queues]
work = "memory://"
files = "memory://"

[markers]
url = "memory://"

[tuning]
poll_seconds = 0.1
"""


@pytest.mark.parametrize("role", ["source", "destination"])
@pytest.mark.parametrize("received", [signal.SIGINT, signal.SIGTERM], ids=["ctrl-c", "sigterm"])
def test_a_waiting_container_leaves_cleanly_when_stopped(
    tmp_path: Path, role: str, received: signal.Signals
) -> None:
    job = tmp_path / "job.toml"
    job.write_text(JOB)
    process = subprocess.Popen(
        [sys.executable, "-m", "bitdrop_cli", "queue", role, str(job), "--name", "test-1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(1.5)  # long enough to be up and waiting for a message
    assert process.poll() is None, "the container left on its own"

    process.send_signal(received)
    stdout, stderr = process.communicate(timeout=30)

    assert process.returncode == 0, stderr
    assert stdout.startswith("test-1: 0 handled"), stdout
