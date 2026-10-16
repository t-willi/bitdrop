"""The ``bitdrop`` command line tool: job files, run / plan / status, plain and JSON progress."""

__all__ = ["main"]


def main() -> None:
    """Run the command line tool. This is the ``bitdrop`` console script."""
    from bitdrop_cli.app import app

    app()
