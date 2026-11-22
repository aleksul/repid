from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping

from repid._concurrency import _DEFAULT_WORKER_CONCURRENCY, _Budget
from repid._intake import _Intake
from repid._runner import _Runner
from repid.connections.abc import ReceivedMessageT
from repid.data.actor import ActorData
from repid.data.concurrency import Concurrency

MessageHandler = Callable[[ReceivedMessageT], Awaitable[None]]


class CallbackIntake(_Intake):
    """A worker intake that runs one handler task per submitted delivery.

    It stands in for the runner in adapter tests: a delivery stays outstanding,
    holding capacity, until its handler returns.
    """

    def __init__(
        self,
        handlers: Mapping[str, MessageHandler],
        worker: Concurrency | None = None,
        configured: Mapping[str, Concurrency] | None = None,
    ) -> None:
        super().__init__(_Budget(worker or _DEFAULT_WORKER_CONCURRENCY, configured))
        self.tasks: set[asyncio.Task[None]] = set()
        self.submitted: list[ReceivedMessageT] = []
        self.errors: list[Exception] = []
        for channel, handler in handlers.items():
            self.add_channel(channel, self._submitter(handler))

    def _submitter(self, handler: MessageHandler) -> Callable[[ReceivedMessageT], None]:
        def submit(message: ReceivedMessageT) -> None:
            channel = message.channel
            self.charge(channel, 0)
            self.submitted.append(message)
            task = asyncio.create_task(self._run(handler, message))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

        return submit

    async def _run(self, handler: MessageHandler, message: ReceivedMessageT) -> None:
        try:
            await handler(message)
        except Exception as exc:
            # The runner logs failures inside the delivery's task; keep them for asserts.
            self.errors.append(exc)
        finally:
            self.release(message.channel, 0)

    async def join(self) -> None:
        """Wait for every handler started so far."""
        while self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
            # Since Python 3.12, gather over finished tasks returns without
            # yielding. Yield so their done callbacks drop them from the set.
            await asyncio.sleep(0)


def callback_intake(
    handlers: Mapping[str, MessageHandler],
    worker: Concurrency | None = None,
    configured: Mapping[str, Concurrency] | None = None,
) -> CallbackIntake:
    return CallbackIntake(handlers, worker, configured)


async def _ignore(_message: ReceivedMessageT) -> None:
    return None


def idle_intake(
    channels: Iterable[str],
    worker: Concurrency | None = None,
    configured: Mapping[str, Concurrency] | None = None,
) -> CallbackIntake:
    """An intake whose handlers return at once, for tests that only subscribe."""
    return CallbackIntake(dict.fromkeys(channels, _ignore), worker, configured)


async def handle_message(
    runner: _Runner,
    actors: list[ActorData],
    message: ReceivedMessageT,
) -> None:
    """Submit one delivery to a runner directly and wait until the runner is done with it."""
    runner._submit(actors, message)
    task = next(task for delivery, task in runner._tasks.items() if delivery.message is message)
    await asyncio.wait({task})
