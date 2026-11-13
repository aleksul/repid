from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from repid.connections.abc import ReceivedMessageT

logger = logging.getLogger("repid")
_current_renewal: ContextVar[LeaseRenewal | None] = ContextVar("delivery_renewal", default=None)


class LeaseRenewal:
    """Schedule renewals for one delivery with a single, lazily started task."""

    __slots__ = ("_changed", "_claimed", "_closed", "_deadline", "_interval", "_message", "_task")

    def __init__(self, message: ReceivedMessageT) -> None:
        self._message = message
        self._changed = asyncio.Event()
        self._claimed = False
        self._closed = False
        self._deadline: float | None = None
        self._interval: float | None = None
        self._task: asyncio.Task[None] | None = None

    def set_interval(self, interval: float | None) -> None:
        if self._closed or interval == self._interval:
            return
        self._interval = interval
        if interval is None:
            self._deadline = None
        else:
            deadline = asyncio.get_running_loop().time() + interval
            if self._deadline is None or deadline < self._deadline:
                self._deadline = deadline

        if self._task is not None:
            self._changed.set()
            return

        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._closed:
            deadline = self._deadline
            self._changed.clear()
            try:
                async with asyncio.timeout_at(deadline):
                    await self._changed.wait()
                continue
            except TimeoutError:
                if deadline != self._deadline:
                    continue

            try:
                if await self._message.keep_alive() is False:
                    self._closed = True
                    return
            except Exception:  # noqa: BLE001
                logger.warning(
                    "message.keep_alive.error",
                    extra={"message_id": self._message.message_id},
                )
            interval = self._interval
            self._deadline = None if interval is None else loop.time() + interval

    def cancel(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._task is not None:
            self._task.cancel()

    async def close(self) -> None:
        self.cancel()
        if self._task is not None:
            # Join the renewal before releasing the delivery, including on cancellation.
            await asyncio.gather(self._task, return_exceptions=True)

    @contextmanager
    def handoff(self) -> Iterator[None]:
        """Expose this renewal while submitting its delivery."""
        token = _current_renewal.set(self)
        try:
            yield
        finally:
            _current_renewal.reset(token)

    @classmethod
    def claim_for(cls, message: ReceivedMessageT) -> LeaseRenewal | None:
        """Take responsibility for a renewal started by the subscriber."""
        renewal = _current_renewal.get()
        if (
            renewal is None
            or renewal._message is not message
            or renewal._claimed
            or renewal._closed
        ):
            return None
        renewal._claimed = True
        return renewal

    async def close_if_unclaimed(self) -> None:
        """Close a renewal that the delivery callback left with the subscriber."""
        if not self._claimed:
            await self.close()
