"""The ``MarkerStore`` contract: small named values that are written once.

In the queue workflow (``docs/decoupled-queue-workflow.md``) the planner records the settings it
chunked a table with, and a marker that says the planning is complete. Both must be written by
exactly one planner even if two are started at once, and every container must read what the winner
wrote. A marker store is that: a handful of keys, each set once, readable by everyone.

The one write is *set if absent*. Over a shared service it is a single atomic step (Redis
``SET NX``, PostgreSQL ``INSERT ... ON CONFLICT DO NOTHING``), and it answers with the value that
is stored afterwards, so the loser learns at once what the winner wrote and can compare it with
its own.
"""

from typing import Protocol, runtime_checkable

__all__ = ["MarkerStore"]


@runtime_checkable
class MarkerStore(Protocol):
    """Keeps small values by key; a key, once set, never changes.

    Behaviour every implementation must provide (verified by the conformance kit):

    * ``set_if_absent`` is atomic: of any number of writers racing for a key, exactly one wins and
      all of them are told the winner's value.
    * Values are kept byte for byte, including empty ones (an empty value is not "absent").
    * Keys follow :func:`bitdrop_api.keys.validate_key`; anything else raises ``InvalidKey``.
    * Keys are independent of each other.
    """

    async def set_if_absent(self, key: str, value: bytes) -> bytes:
        """Store ``value`` under ``key`` unless it already has one, and return what is stored now.

        The result is ``value`` if this call set it, otherwise the value that was there first.
        The caller tells the two apart by comparing.
        """
        ...

    async def get(self, key: str) -> bytes | None:
        """Return the value under ``key``, or ``None`` if nobody has set it."""
        ...

    async def aclose(self) -> None:
        """Release connections and other resources. The store must not be used afterwards."""
        ...
