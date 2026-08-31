"""Shared intake admission helpers for broker subscribers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Iterable, Mapping
from typing import TYPE_CHECKING, Any

from repid.admission import IntakeGate, OversizedReservationError, dispose_oversized
from repid.limits import MessageLimits

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.limits import ReservationLeaseT


async def cancel_and_drain(tasks: Iterable[asyncio.Task[Any]]) -> None:
    """Cancel tasks and wait for every one to settle."""
    all_tasks = tuple(tasks)
    for task in all_tasks:
        task.cancel()
    if all_tasks:
        await asyncio.gather(*all_tasks, return_exceptions=True)


async def run_supervised(
    *awaitables: asyncio.Task[Any] | Coroutine[Any, Any, Any],
) -> None:
    """Run loops together; when the first one ends, cancel the rest."""
    tasks = [
        awaitable if isinstance(awaitable, asyncio.Task) else asyncio.create_task(awaitable)
        for awaitable in awaitables
    ]
    if not tasks:
        return
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        await cancel_and_drain(task for task in tasks if not task.done())
        await asyncio.gather(*tasks, return_exceptions=True)
    for task in done:
        if not task.cancelled() and (exc := task.exception()) is not None:
            raise exc


class SubscriberDispatcher:
    """Apply prepared intake policy around subscriber callbacks."""

    def __init__(
        self,
        limits: MessageLimits | None = None,
        channel_limits: Mapping[str, MessageLimits] | None = None,
        *,
        active: bool = True,
    ) -> None:
        self._intake_gate = IntakeGate(limits, channel_limits)
        self._active = asyncio.Event()
        if active:
            self._active.set()

    def activate(self) -> None:
        """Allow delivery after subscriber setup and policy validation."""
        self._active.set()

    def native_message_limit(self, channel: str | None = None) -> int | None:
        """Return the native message cap for a channel or the whole subscription."""
        return self._intake_gate.native_message_limit(channel)

    def native_payload_limit(self, channel: str | None = None) -> int | None:
        """Return the native payload cap for a channel or the whole subscription."""
        return self._intake_gate.native_payload_limit(channel)

    async def reserve(self, message: ReceivedMessageT) -> ReservationLeaseT | None:
        """Reserve intake capacity, or dispose of an oversized payload."""
        await self._active.wait()
        try:
            return await self._intake_gate.reserve(message)
        except OversizedReservationError as exc:
            await dispose_oversized(message, exc.action)
            return None

    @staticmethod
    async def run_admitted(
        lease: ReservationLeaseT,
        message: ReceivedMessageT,
        callback: Callable[[ReceivedMessageT], Coroutine[Any, Any, None]],
    ) -> None:
        """Run one admitted callback and always release its intake lease."""
        try:
            await callback(message)
        finally:
            await lease.release()
