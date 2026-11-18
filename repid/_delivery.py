from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

    from repid._concurrency import _Budget
    from repid.connections.abc import ReceivedMessageT

logger = logging.getLogger("repid")


class LeaseRenewal:
    """Renew one message's lease from a loop timer, running a task only while renewing.

    Most deliveries settle before their first renewal is due, so an idle
    renewal costs one timer handle rather than a task.

    A subscriber that holds a delivery before calling its callback, for example
    while paused, can renew it with this class and submit it inside `handoff()`.
    The worker then adopts the renewal with its pending deadline, so the lease
    has no gap between the subscriber's schedule and the worker's.
    """

    __slots__ = (
        "_claimed",
        "_closed",
        "_deadline",
        "_interval",
        "_message",
        "_task",
        "_timer",
    )

    def __init__(self, message: ReceivedMessageT) -> None:
        self._message = message
        self._interval: float | None = None
        self._deadline: float | None = None
        self._timer: asyncio.TimerHandle | None = None
        # Set only while a due renewal runs; it schedules the next one itself.
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._claimed = False

    def renew_at(self, interval: float | None) -> None:
        """Renew every `interval` seconds, keeping an earlier pending deadline.

        A missing or non-positive interval stops scheduling renewals.
        """
        if interval is not None and interval <= 0:
            interval = None
        if self._closed or interval == self._interval:
            return
        self._interval = interval
        if interval is None:
            self._deadline = None
        else:
            deadline = asyncio.get_running_loop().time() + interval
            if self._deadline is None or deadline < self._deadline:
                self._deadline = deadline
        self._schedule()

    def _schedule(self) -> None:
        deadline = self._deadline
        timer = self._timer
        if self._closed or self._task is not None or deadline is None:
            if timer is not None:
                timer.cancel()
                self._timer = None
            return
        if timer is not None:
            # renew_at often keeps the pending deadline; keep its timer too.
            if timer.when() == deadline:
                return
            timer.cancel()
        self._timer = asyncio.get_running_loop().call_at(deadline, self._renew_due, deadline)

    def _renew_due(self, deadline: float) -> None:
        self._timer = None
        self._task = asyncio.create_task(self._renew(deadline))

    async def _renew(self, deadline: float) -> None:
        # The schedule may have changed between the timer firing and this task starting.
        if deadline == self._deadline:
            try:
                if await self._message.keep_alive() is False:
                    self._closed = True
            except Exception:  # noqa: BLE001
                logger.warning(
                    "message.keep_alive.error",
                    extra={"message_id": self._message.message_id},
                )
            interval = self._interval
            self._deadline = (
                None if interval is None else asyncio.get_running_loop().time() + interval
            )
        self._task = None
        self._schedule()

    def stop(self) -> None:
        self._closed = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._task is not None:
            self._task.cancel()

    async def close(self) -> None:
        """Stop renewing and join a running renewal, including on cancellation."""
        self.stop()
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)

    @contextmanager
    def handoff(self) -> Generator[None, None, None]:
        """Offer this renewal to the worker while submitting its delivery."""
        token = _offered_renewal.set(self)
        try:
            yield
        finally:
            _offered_renewal.reset(token)

    @classmethod
    def _claim_for(cls, message: ReceivedMessageT) -> LeaseRenewal | None:
        """Adopt the renewal a subscriber offered for this message, if any."""
        renewal = _offered_renewal.get()
        if (
            renewal is None
            or renewal._message is not message
            or renewal._claimed
            or renewal._closed
        ):
            return None
        renewal._claimed = True
        # Tasks the worker spawns from here must not inherit the offer.
        _offered_renewal.set(None)
        return renewal

    async def close_if_unclaimed(self) -> None:
        """Close the renewal unless the worker adopted it during `handoff()`."""
        if not self._claimed:
            await self.close()


_offered_renewal: ContextVar[LeaseRenewal | None] = ContextVar("offered_renewal", default=None)


class _Delivery:
    """One in-flight message: its budget hold, its lease renewal and its processing state."""

    __slots__ = (
        "_budget",
        "_held",
        "_renewal",
        "_size",
        "message",
        "shutdown_rejection",
        "started",
    )

    def __init__(self, message: ReceivedMessageT, budget: _Budget, size: int = 0) -> None:
        self.message = message
        # Created on the first interval unless the subscriber handed one over.
        self._renewal = LeaseRenewal._claim_for(message)
        self.started = False
        self.shutdown_rejection: asyncio.Task[None] | None = None
        self._budget = budget
        self._size = size
        self._held = False

    # Budget

    @property
    def held(self) -> bool:
        return self._held

    def try_acquire(self) -> bool:
        if not self._held:
            self._held = self._budget.try_acquire(self.message.channel, self._size)
        return self._held

    async def acquire(self) -> bool:
        if not self._held:
            self._held = await self._budget.acquire(self.message.channel, self._size)
        return self._held

    def release(self) -> None:
        if self._held:
            self._held = False
            self._budget.release(self.message.channel, self._size)

    # Lease renewal

    def renew_at(self, interval: float | None) -> None:
        if self._renewal is None:
            if interval is None or interval <= 0:
                return
            self._renewal = LeaseRenewal(self.message)
        self._renewal.renew_at(interval)

    def stop_renewal(self) -> None:
        if self._renewal is not None:
            self._renewal.stop()

    # Completion

    async def finish(self) -> None:
        """Join the renewal, then return the budget, including on cancellation."""
        try:
            if self._renewal is not None:
                await self._renewal.close()
        finally:
            self.release()

    async def reject(self) -> None:
        if not self.message.is_acted_on:
            await self.message.reject()
