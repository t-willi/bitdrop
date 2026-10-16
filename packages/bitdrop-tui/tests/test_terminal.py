"""The real thing: ``bitdrop run --ui tui`` in a pseudo-terminal, the way a person runs it.

Headless tests prove the screen's logic; only a real terminal proves that the whole chain works:
the command finds the display, the screen draws, a keypress reaches it, and the process exits with
the right code. The job is slow on purpose, so there is always something running to stop.
"""

import contextlib
import fcntl
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import termios
import time
from pathlib import Path

import pytest

JOB = """
[job]
id = "terminal-1"

[source]
url = "memory://?demo_rows=400000&delay_ms=300"
table = "demo"

[destination]
url = "memory://"
table = "orders"

[staging]
url = "memory://"

[metadata]
url = "memory://"

[tuning]
chunk_mib = 0.1
extractors = 2
loaders = 2
"""


class Terminal:
    """A process attached to a pseudo-terminal of 120 columns by 40 rows."""

    def __init__(self, *args: str) -> None:
        self._master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
        self.process = subprocess.Popen(
            [sys.executable, "-m", "bitdrop_cli", *args],
            stdin=slave,
            stdout=slave,
            stderr=slave,
            env={**os.environ, "TERM": "xterm-256color"},
            start_new_session=True,
        )
        os.close(slave)
        self.output = b""

    def wait_for(self, text: str, timeout: float = 20) -> None:
        deadline = time.monotonic() + timeout
        while text.encode() not in self.output:
            if time.monotonic() > deadline:
                raise AssertionError(f"never saw {text!r}; got {self.output[-600:]!r}")
            self._read(0.1)

    def send(self, data: bytes) -> None:
        os.write(self._master, data)

    def finish(self, timeout: float = 30) -> int:
        deadline = time.monotonic() + timeout
        while self.process.poll() is None:
            if time.monotonic() > deadline:
                self.process.kill()
                raise AssertionError(f"still running; got {self.output[-600:]!r}")
            self._read(0.1)
        self._read(0.2)
        os.close(self._master)
        return self.process.returncode

    def _read(self, wait: float) -> None:
        ready, _, _ = select.select([self._master], [], [], wait)
        if ready:
            with contextlib.suppress(OSError):  # the other end closed
                self.output += os.read(self._master, 65536)


@pytest.fixture
def job_file(tmp_path: Path) -> Path:
    path = tmp_path / "job.toml"
    path.write_text(JOB)
    return path


def test_the_screen_is_drawn_and_q_stops_the_job_with_exit_code_130(job_file: Path) -> None:
    terminal = Terminal("run", str(job_file), "--ui", "tui")

    terminal.wait_for("Stop safely")  # the footer: the screen is up
    terminal.wait_for("Workers")
    terminal.send(b"q")
    code = terminal.finish()

    assert code == 130, terminal.output[-600:]
    assert b"stopped" in terminal.output
    assert b"resume" in terminal.output


def test_auto_picks_the_screen_on_a_terminal(job_file: Path) -> None:
    terminal = Terminal("run", str(job_file))

    terminal.wait_for("Stop safely")
    terminal.send(b"q")

    assert terminal.finish() == 130


def test_sigterm_closes_the_screen_and_stops_the_job(job_file: Path) -> None:
    terminal = Terminal("run", str(job_file), "--ui", "tui")
    terminal.wait_for("Stop safely")

    terminal.process.send_signal(signal.SIGTERM)

    assert terminal.finish() == 130, terminal.output[-600:]


def test_ctrl_c_in_the_screen_stops_the_job_too(job_file: Path) -> None:
    terminal = Terminal("run", str(job_file), "--ui", "tui")
    terminal.wait_for("Stop safely")

    terminal.send(b"\x03")

    assert terminal.finish() == 130, terminal.output[-600:]
