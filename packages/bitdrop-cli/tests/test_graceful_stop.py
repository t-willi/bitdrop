"""The first signal asks a container to stop after its message; the second cancels the message."""

import asyncio
import os
import signal

from bitdrop_cli.execute import _stopping_gracefully


async def test_one_signal_asks_for_a_stop_and_two_cancel_the_work() -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(asyncio.sleep(3600))

    with _stopping_gracefully(stop, task):
        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.sleep(0.05)
        assert stop.is_set()
        assert not task.cancelled(), "one signal must let the message in hand finish"

        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.sleep(0.05)

    assert task.cancelled()


async def test_sigterm_asks_for_a_stop_too() -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(asyncio.sleep(3600))

    with _stopping_gracefully(stop, task):
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(0.05)
        assert stop.is_set()
        assert not task.done()

    task.cancel()


async def test_the_handlers_are_removed_afterwards() -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(asyncio.sleep(0))
    before = signal.getsignal(signal.SIGTERM)

    with _stopping_gracefully(stop, task):
        pass

    assert signal.getsignal(signal.SIGTERM) == before
    await task
