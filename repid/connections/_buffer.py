"""Renew and dispose of deliveries only until submission callback entry."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Iterable
from contextlib import suppress
from typing import Any, cast

from repid.connections.abc import ReceivedMessageT


async def stop_task(task: asyncio.Task) -> None:
    """Cancellation is expected; failed adapter cleanup must remain observable."""
    task.cancel()
    results = await asyncio.gather(task, return_exceptions=True)
    result = results[0]
    if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
        raise result


class SubmissionBuffer:
    def __init__(self) -> None:
        self.owned: dict[int, tuple[ReceivedMessageT, asyncio.Task | None]] = {}
        self._failure: BaseException | None = None
        self._failed = asyncio.Event()

    def fail(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
            self._failed.set()

    def _renewal_done(self, task: asyncio.Task) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self.fail(error)

    async def _wait_failure(self) -> None:
        await self._failed.wait()
        raise cast(BaseException, self._failure)

    async def run(self, intake: Callable[[], Coroutine[Any, Any, None]]) -> None:
        """Supervise intake and fail immediately if a buffered renewal fails."""
        tasks = [asyncio.create_task(intake()), asyncio.create_task(self._wait_failure())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            await asyncio.gather(*done)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _renew(self, message: ReceivedMessageT, interval: float) -> None:
        while not message.is_acted_on:
            await asyncio.sleep(interval)
            if not message.is_acted_on:
                await message.keep_alive()

    def track(self, messages: Iterable[ReceivedMessageT]) -> None:
        for message in messages:
            interval = message.keep_alive_interval
            task = (
                asyncio.create_task(self._renew(message, interval))
                if interval is not None
                else None
            )
            self.owned[id(message)] = (message, task)
            if task is not None:
                task.add_done_callback(self._renewal_done)

    async def submit(
        self,
        message: ReceivedMessageT,
        callback: Callable[[ReceivedMessageT], Coroutine[Any, Any, None]],
    ) -> None:
        _, renewal = self.owned[id(message)]
        if renewal is not None:
            renewal.cancel()
            with suppress(asyncio.CancelledError):
                await renewal
        # No suspension between ownership removal and callback entry.
        self.owned.pop(id(message))
        await callback(message)

    async def dispose(self) -> None:
        messages = list(self.owned.values())
        self.owned.clear()
        errors: list[BaseException] = []
        for message, renewal in messages:
            if renewal is not None:
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
            if not message.is_acted_on:
                try:
                    await message.reject()
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)
        if self._failure is not None:
            errors.insert(0, self._failure)
        if errors:
            raise errors[0]
