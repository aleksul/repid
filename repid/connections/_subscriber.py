"""Shared intake admission helpers for broker subscribers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from repid.admission import IntakeGate, OversizedReservationError, dispose_oversized
from repid.limits import BackpressureResource, MessageLimits

logger = logging.getLogger("repid.connections")

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.limits import ReservationLeaseT


async def _keep_alive_loop(message: ReceivedMessageT, interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        if message.is_acted_on:
            return
        try:
            await message.keep_alive()
        except Exception:  # noqa: BLE001
            logger.warning("message.keep_alive.error", extra={"message_id": message.message_id})


async def cancel_and_drain(
    tasks: Iterable[asyncio.Task[Any]],
    *,
    drain: bool = True,
) -> None:
    """Cancel tasks, optionally waiting for every one to settle."""
    all_tasks = tuple(tasks)
    for task in all_tasks:
        task.cancel()
    if all_tasks and drain:
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


@dataclass(slots=True)
class _AdmittedDelivery:
    lease: ReservationLeaseT | None
    on_cancel: Callable[[], Awaitable[None]] | None
    message: ReceivedMessageT | None = None
    started: bool = False
    cleanup_scheduled: bool = False


class AdmittedTaskTracker:
    """Own admitted callback tasks, including work cancelled before it starts."""

    def __init__(self) -> None:
        self.tasks: set[asyncio.Task[None]] = set()
        self._deliveries: dict[asyncio.Task[None], _AdmittedDelivery] = {}
        self._cleanup_tasks: dict[asyncio.Task[None], _AdmittedDelivery] = {}
        self._cancelling = False

    def start(
        self,
        dispatcher: SubscriberDispatcher,
        lease: ReservationLeaseT,
        message: ReceivedMessageT,
        callback: Callable[[ReceivedMessageT], Coroutine[Any, Any, None]],
        *,
        on_cancel: Callable[[], Awaitable[None]] | None = None,
    ) -> asyncio.Task[None]:
        """Start an admitted callback and retain cancellation ownership."""
        return self.start_task(
            lambda: dispatcher.run_admitted(lease, message, callback),
            lease=lease,
            on_cancel=on_cancel,
            message=message,
        )

    def start_task(
        self,
        run: Callable[[], Coroutine[Any, Any, None]],
        *,
        lease: ReservationLeaseT | None = None,
        on_cancel: Callable[[], Awaitable[None]] | None = None,
        message: ReceivedMessageT | None = None,
    ) -> asyncio.Task[None]:
        """Start owned work whose pre-start cancellation needs asynchronous cleanup."""
        delivery = _AdmittedDelivery(lease=lease, on_cancel=on_cancel, message=message)

        async def wrapped() -> None:
            delivery.started = True
            await run()

        task = asyncio.create_task(wrapped())
        self.tasks.add(task)
        self._deliveries[task] = delivery
        task.add_done_callback(self._task_done)
        if self._cancelling:
            task.cancel()
            self._schedule_cleanup(delivery)
        return task

    @property
    def owned_messages(self) -> tuple[ReceivedMessageT, ...]:
        """Messages whose cancellation cleanup is owned by this tracker."""
        return tuple(
            delivery.message
            for delivery in (*self._deliveries.values(), *self._cleanup_tasks.values())
            if delivery.message is not None
        )

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self.tasks.discard(task)
        delivery = self._deliveries.pop(task, None)
        if delivery is None:
            return
        if not delivery.started:
            self._schedule_cleanup(delivery)
            return
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.exception("subscriber.delivery.error", exc_info=exc)

    def _schedule_cleanup(self, delivery: _AdmittedDelivery) -> None:
        if delivery.cleanup_scheduled:
            return
        delivery.cleanup_scheduled = True
        task = asyncio.create_task(self._cleanup_delivery(delivery))
        self._cleanup_tasks[task] = delivery
        task.add_done_callback(self._cleanup_done)

    async def _cleanup_delivery(self, delivery: _AdmittedDelivery) -> None:
        try:
            if delivery.on_cancel is not None:
                await delivery.on_cancel()
        finally:
            if delivery.lease is not None:
                await delivery.lease.release()

    def _cleanup_done(self, task: asyncio.Task[None]) -> None:
        self._cleanup_tasks.pop(task, None)
        if task.cancelled():
            return
        if (exc := task.exception()) is not None:
            logger.exception("subscriber.delivery.cleanup.error", exc_info=exc)

    async def cancel_and_drain(self, *, drain: bool = True) -> None:
        """Request cancellation once, optionally waiting for all owned cleanup."""
        if not self._cancelling:
            self._cancelling = True
            for task in tuple(self.tasks):
                delivery = self._deliveries.get(task)
                task.cancel()
                if delivery is not None and not delivery.started:
                    self._schedule_cleanup(delivery)

        if drain:
            await self.drain()

    async def drain(self) -> None:
        """Wait for previously cancelled callbacks and cleanup without cancelling again."""
        while self.tasks or self._cleanup_tasks:
            await self._drain_cleanups()
            tasks = tuple(self.tasks)
            if not tasks:
                continue
            await asyncio.gather(*tasks, return_exceptions=True)
            # Normally the event loop runs registered done callbacks before
            # gather resumes. Finalize explicitly as well so externally supplied
            # or unusually scheduled completed tasks cannot keep the drain alive.
            for task in tasks:
                if task.done():
                    self._task_done(task)

    async def _drain_cleanups(self) -> None:
        while self._cleanup_tasks:
            await asyncio.gather(*self._cleanup_tasks, return_exceptions=True)


class SubscriberDispatcher:
    """Apply prepared intake policy around subscriber callbacks."""

    def __init__(
        self,
        limits: MessageLimits | None = None,
        channel_limits: Mapping[str, tuple[MessageLimits, ...]] | None = None,
        *,
        active: bool = True,
    ) -> None:
        self._intake_gate = IntakeGate(limits, channel_limits)
        self._active = asyncio.Event()
        self._pre_admission_keepalives: dict[int, asyncio.Task[None]] = {}
        self._keepalive_tasks: set[asyncio.Task[None]] = set()
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

    def start_keep_alive(self, message: ReceivedMessageT) -> None:
        """Renew a fetched delivery until it is admitted or disposed of."""
        interval = message.keep_alive_interval
        if interval is None or interval <= 0 or id(message) in self._pre_admission_keepalives:
            return
        task = asyncio.create_task(_keep_alive_loop(message, interval))
        self._pre_admission_keepalives[id(message)] = task
        self._keepalive_tasks.add(task)
        task.add_done_callback(self._keepalive_tasks.discard)

    async def stop_keep_alive(self, message: ReceivedMessageT) -> None:
        task = self._pre_admission_keepalives.pop(id(message), None)
        if task is not None:
            task.cancel()
            await asyncio.shield(asyncio.gather(task, return_exceptions=True))

    def native_limit_is_independent(
        self,
        channel: str,
        channels: Iterable[str],
        resource: BackpressureResource,
    ) -> bool:
        """Whether this channel's native resource limit is independent."""
        return self._intake_gate.native_limit_is_independent(channel, channels, resource)

    def native_limit_is_uniform(
        self,
        channels: Iterable[str],
        resource: BackpressureResource,
    ) -> bool:
        """Whether every subscribed channel resolves to the subscription-wide native cap."""
        return self._intake_gate.native_limit_is_uniform(channels, resource)

    async def reserve(self, message: ReceivedMessageT) -> ReservationLeaseT | None:
        """Reserve intake capacity, or dispose of an oversized payload."""
        lease: ReservationLeaseT | None = None
        self.start_keep_alive(message)
        try:
            await self._active.wait()
            try:
                lease = await self._intake_gate.reserve(message)
            except OversizedReservationError as exc:
                await dispose_oversized(message, exc.action)
                return None
            return lease
        finally:
            try:
                await self.stop_keep_alive(message)
            except BaseException:
                if lease is not None:
                    await lease.release()
                raise

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
