"""A live terminal UI for a running bitdrop job.

Install this package and ``bitdrop run job.toml`` shows it on a terminal (``--ui tui`` asks for it
explicitly). The command line tool never imports this package: it finds the display by name through
the entry point group ``bitdrop.uis``.
"""

from bitdrop_core.ui import UiContext
from bitdrop_tui.app import TuiUi

__all__ = ["create_ui"]


def create_ui(context: UiContext) -> TuiUi:
    """Build the display for ``--ui tui``. This is the plugin entry point."""
    return TuiUi(context)
