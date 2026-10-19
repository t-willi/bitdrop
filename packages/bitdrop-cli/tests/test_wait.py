"""``run --wait``: open the display first, start the load from it."""

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from typer.testing import CliRunner

from bitdrop_cli.app import app
from bitdrop_cli.execute import ExitCode, run_job
from bitdrop_cli.jobfile import load_job
from bitdrop_connector_memory import create_destination, create_source
from bitdrop_core import registry
from bitdrop_core.progress import TransferEvent
from bitdrop_core.transfer import TransferReport
from bitdrop_core.ui import UiContext
from bitdrop_metadata_memory import create_store as create_metadata_store
from bitdrop_store_memory import create_store as create_chunk_store

JOB = """
[job]
id = "orders-1"

[source]
url = "memory://?demo_rows=2000"
table = "demo"

[destination]
url = "memory://"
table = "orders"

[staging]
url = "memory://"

[metadata]
url = "memory://"

[tuning]
extractors = 2
loaders = 3
"""


class WaitingDisplay:
    """A display that can start jobs. ``press`` says what the person does once it is up."""

    instances: ClassVar[list[WaitingDisplay]] = []
    press: ClassVar[Callable[[WaitingDisplay, asyncio.Task[TransferReport]], Any]]

    def __init__(self, context: UiContext) -> None:
        self.context = context
        self.events: list[TransferEvent] = []
        self.seen_while_waiting = -1
        WaitingDisplay.instances.append(self)

    def __call__(self, event: TransferEvent) -> None:
        self.events.append(event)

    def log_handler(self) -> logging.Handler | None:
        return None

    async def present(self, job: asyncio.Task[TransferReport]) -> None:
        await WaitingDisplay.press(self, job)
        await asyncio.wait({job})


@pytest.fixture
def install_display(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    WaitingDisplay.instances = []

    def apply(press: Callable[[WaitingDisplay, asyncio.Task[TransferReport]], Any]) -> None:
        WaitingDisplay.press = press
        points: dict[str, list[Callable[..., object]]] = {
            "bitdrop_sources": [create_source],
            "bitdrop_destinations": [create_destination],
            "bitdrop_chunk_stores": [create_chunk_store],
            "bitdrop_metadata_stores": [create_metadata_store],
            "bitdrop_uis": [WaitingDisplay],
        }
        names = {"bitdrop_uis": "waiting"}

        def discover(*, group: str) -> list[SimpleNamespace]:
            key = group.replace(".", "_")
            return [
                SimpleNamespace(name=names.get(key, "memory"), load=lambda f=f: f)
                for f in points.get(key, [])
            ]

        monkeypatch.setattr(registry, "entry_points", discover)

    return apply


def job_path(tmp_path: Path) -> Path:
    path = tmp_path / "job.toml"
    path.write_text(JOB)
    return path


class TestWaiting:
    def test_nothing_moves_until_the_display_starts_the_job(
        self, tmp_path: Path, install_display: Callable[..., None]
    ) -> None:
        async def press(display: WaitingDisplay, job: asyncio.Task[TransferReport]) -> None:
            await asyncio.sleep(0.3)  # long enough for a job that was not held back to finish
            display.seen_while_waiting = len(display.events)
            assert display.context.begin is not None
            display.context.begin()

        install_display(press)
        code = asyncio.run(run_job(load_job(job_path(tmp_path)), ui_name="waiting", wait=True))

        assert code is ExitCode.OK
        display = WaitingDisplay.instances[0]
        assert display.seen_while_waiting == 0, "the job ran before it was started"
        assert len(display.events) > 0, "starting the job did not run it"

    def test_the_display_is_told_how_many_workers_the_job_is_set_up_for(
        self, tmp_path: Path, install_display: Callable[..., None]
    ) -> None:
        async def press(display: WaitingDisplay, job: asyncio.Task[TransferReport]) -> None:
            assert display.context.begin is not None
            display.context.begin()

        install_display(press)
        asyncio.run(run_job(load_job(job_path(tmp_path)), ui_name="waiting", wait=True))

        context = WaitingDisplay.instances[0].context
        assert (context.extractors, context.loaders) == (2, 3)

    def test_leaving_before_the_start_moves_nothing_and_says_so(
        self,
        tmp_path: Path,
        install_display: Callable[..., None],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        async def press(display: WaitingDisplay, job: asyncio.Task[TransferReport]) -> None:
            await asyncio.sleep(0.1)
            job.cancel()

        install_display(press)
        code = asyncio.run(run_job(load_job(job_path(tmp_path)), ui_name="waiting", wait=True))

        assert code is ExitCode.INTERRUPTED
        error = capsys.readouterr().err
        assert "not started" in error
        assert "handed back" not in error

    def test_a_job_that_is_not_waiting_has_no_start_button(
        self, tmp_path: Path, install_display: Callable[..., None]
    ) -> None:
        async def press(display: WaitingDisplay, job: asyncio.Task[TransferReport]) -> None:
            pass

        install_display(press)
        asyncio.run(run_job(load_job(job_path(tmp_path)), ui_name="waiting"))

        assert WaitingDisplay.instances[0].context.begin is None


class TestTheCommand:
    def test_wait_needs_the_tui(self, tmp_path: Path) -> None:
        result = CliRunner().invoke(
            app, ["run", str(job_path(tmp_path)), "--ui", "plain", "--wait"]
        )

        assert result.exit_code == 2
        assert "--wait" in result.output
        assert "tui" in result.output
