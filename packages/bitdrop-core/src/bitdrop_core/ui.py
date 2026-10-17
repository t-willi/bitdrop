"""The ``ProgressUi`` contract: something that shows a running transfer to a person or a program.

A display is a component like any other (principle P1): the command line tool never imports one, it
asks the registry for it by name (``--ui plain``, ``--ui tui``). Each is its own package and
announces itself in the entry point group ``bitdrop.uis``.

A display has three jobs:

* It is the *observer* of the transfer: calling it with an event updates the display. It obeys the
  rules in :mod:`bitdrop_core.progress` (return quickly; the transfer never depends on it).
* It may hand out a ``logging`` handler, so that log lines go where the display wants them instead
  of corrupting a screen it owns.
* :meth:`ProgressUi.present` keeps the display up while the job runs. The display may cancel the job
  (the person asked to stop), which makes the runner hand its chunks back so a rerun resumes at
  once.
"""

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from bitdrop_core.progress import TransferEvent
from bitdrop_core.transfer import TransferReport

__all__ = ["ProgressUi", "UiContext"]


@dataclass(frozen=True, slots=True)
class UiContext:
    """What a display may show about the job it was opened for. Everything is safe to print."""

    job_id: str
    source_table: str
    destination_table: str
    endpoints: Mapping[str, str]
    """Role (``source``, ``destination``, ``staging``, ``metadata``) to its URL with passwords
    replaced by ``***``."""

    keep_open: bool = True
    """Whether a display that owns the screen stays up after the job, until the person quits."""

    extractors: int = 0
    loaders: int = 0
    """How many extractors and loaders the job is configured for, to show before the plan is
    announced (the plan then names them itself). 0 means the caller did not say."""


@runtime_checkable
class ProgressUi(Protocol):
    """Shows one transfer."""

    def __call__(self, event: TransferEvent) -> None:
        """Take one progress event. Must return quickly and never raise on purpose."""
        ...

    def log_handler(self) -> logging.Handler | None:
        """Return a handler for log records while the display is up, or ``None`` for the default."""
        ...

    async def present(self, job: asyncio.Task[TransferReport]) -> None:
        """Keep the display up until ``job`` has ended, and longer if the display wants to.

        The display may cancel ``job``. It must not raise the job's outcome; the caller awaits the
        job itself afterwards.
        """
        ...
