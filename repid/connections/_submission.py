from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Generic, TypeVar

from repid._delivery import LeaseRenewal
from repid.connections.abc import ReceivedMessageT

if TYPE_CHECKING:
    from repid.connections.abc import ChannelIntake

_MessageT = TypeVar("_MessageT", bound=ReceivedMessageT)


def _channel_prefetch(prefetch: int | None, limit: int | None, batch: int) -> int:
    """Read-ahead for one channel: as configured, or one fetch batch capped by its limit.

    A worker that runs few messages at once should not lock many more on the broker.
    """
    if prefetch is not None:
        return prefetch
    return batch if limit is None else min(batch, limit)


class _SubmissionQueue(Generic[_MessageT]):
    """Hold a channel's deliveries until the worker has room, then submit them in order.

    A delivery is submitted at once when nothing is held and the worker has room,
    so the queue only costs anything while the worker is busy or paused. Held
    deliveries keep their leases through `LeaseRenewal`, which the worker adopts
    on submission.
    """

    __slots__ = (
        "_closed",
        "_held",
        "_intake",
        "_is_stale",
        "_keep_alive",
        "_on_submitted",
        "_paused",
        "_waiting",
    )

    def __init__(
        self,
        intake: ChannelIntake,
        *,
        keep_alive: bool,
        is_stale: Callable[[_MessageT], bool] | None = None,
        on_submitted: Callable[[_MessageT], None] | None = None,
    ) -> None:
        self._intake = intake
        self._keep_alive = keep_alive
        # Held deliveries the broker no longer lets this worker settle are dropped.
        self._is_stale = is_stale
        # Called once the worker owns a delivery.
        self._on_submitted = on_submitted
        self._held: deque[tuple[_MessageT, LeaseRenewal | None]] = deque()
        self._paused = False
        self._closed = False
        self._waiting = False

    def __len__(self) -> int:
        return len(self._held)

    def put(self, message: _MessageT) -> None:
        if self._closed:
            return
        if not self._held and not self._paused and self._intake.ready.done():
            self._submit(message)
            return
        renewal = None
        if self._keep_alive:
            renewal = LeaseRenewal(message)
            renewal.renew_at(message.keep_alive_interval)
        self._held.append((message, renewal))
        self.drain()

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False
        self.drain()

    def close(self) -> list[_MessageT]:
        """Stop submitting and return the deliveries still held, renewals stopped."""
        self._closed = True
        held = [message for message, _ in self._held]
        for _, renewal in self._held:
            if renewal is not None:
                renewal.stop()
        self._held.clear()
        return held

    def drain(self) -> None:
        """Submit held deliveries while the worker has room."""
        held = self._held
        intake = self._intake
        is_stale = self._is_stale
        while held and not self._paused and not self._closed:
            ready = intake.ready
            if not ready.done():
                if not self._waiting:
                    self._waiting = True
                    ready.add_done_callback(self._on_ready)
                return
            message, renewal = held.popleft()
            if is_stale is not None and is_stale(message):
                if renewal is not None:
                    renewal.stop()
                continue
            if renewal is None:
                self._submit(message)
            else:
                with renewal.handoff():
                    self._submit(message)
                # The worker adopts the renewal; one it did not take must not leak.
                renewal.stop_if_unclaimed()

    def _submit(self, message: _MessageT) -> None:
        self._intake.submit(message)
        if self._on_submitted is not None:
            self._on_submitted(message)

    def _on_ready(self, _future: asyncio.Future[None]) -> None:
        self._waiting = False
        self.drain()
