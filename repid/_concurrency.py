from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Mapping
from math import inf

from repid.data.concurrency import Concurrency

_DEFAULT_WORKER_CONCURRENCY = Concurrency(messages=1000)


def _tighter_limit(worker_limit: int | None, channel_limit: int | None) -> int | None:
    if worker_limit is None:
        return channel_limit
    if channel_limit is None:
        return worker_limit
    return min(worker_limit, channel_limit)


class _Scope:
    __slots__ = ("byte_limit", "limits", "message_limit", "messages", "payload_bytes")

    def __init__(self, limits: Concurrency) -> None:
        self.limits = limits
        self.message_limit = inf if limits.messages is None else limits.messages
        self.byte_limit = inf if limits.payload_bytes is None else limits.payload_bytes
        self.messages = 0
        self.payload_bytes = 0

    def fits(self, size: int) -> bool:
        # An empty scope can admit a single oversized delivery.
        return self.messages < self.message_limit and (
            self.messages == 0 or self.payload_bytes + size <= self.byte_limit
        )

    def charge(self, size: int) -> None:
        self.messages += 1
        self.payload_bytes += size

    def release(self, size: int) -> None:
        self.messages -= 1
        self.payload_bytes -= size


_Waiter = asyncio.Future[bool]
_WaitQueue = OrderedDict[_Waiter, int]


class _Budget:
    """Atomically check and charge worker and channel budgets on the worker's event loop."""

    __slots__ = (
        "_candidates",
        "_waiters",
        "channels",
        "closed",
        "worker",
    )

    def __init__(
        self,
        worker: Concurrency,
        channels: Mapping[str, Concurrency] | None = None,
    ) -> None:
        self.worker = _Scope(worker)
        # Only channels with their own limit get a scope; others share the worker's.
        self.channels: dict[str, _Scope] = {
            name: _Scope(limits)
            for name, limits in (channels or {}).items()
            if limits.messages is not None or limits.payload_bytes is not None
        }
        self.closed = False
        self._waiters: dict[str, _WaitQueue] = {}
        # Channels rotate here until their head hits a local limit. The waiting
        # map retains parked queues until a release or head change reactivates them.
        self._candidates: OrderedDict[str, _WaitQueue] = OrderedDict()

    def limits(self, channel: str) -> Concurrency:
        """Return the tighter of the worker and channel limits for a channel."""
        worker = self.worker.limits
        if (scope := self.channels.get(channel)) is None:
            return worker
        return Concurrency(
            messages=_tighter_limit(worker.messages, scope.limits.messages),
            payload_bytes=_tighter_limit(worker.payload_bytes, scope.limits.payload_bytes),
        )

    def close(self) -> None:
        self.closed = True
        for queue in self._waiters.values():
            for waiter in queue:
                if not waiter.done():
                    waiter.set_result(False)
        self._waiters.clear()
        self._candidates.clear()

    def try_acquire(self, channel: str, size: int) -> bool:
        if self.closed or (self._waiters and (self._candidates or channel in self._waiters)):
            return False
        scope = self.channels.get(channel)
        if not self.worker.fits(size) or (scope is not None and not scope.fits(size)):
            return False
        self._charge(channel, size)
        return True

    async def acquire(self, channel: str, size: int) -> bool:
        if self.try_acquire(channel, size) or self.closed:
            return not self.closed

        waiter: _Waiter = asyncio.get_running_loop().create_future()
        queue = self._waiters.get(channel)
        if queue is None:
            self._waiters[channel] = queue = OrderedDict(((waiter, size),))
            self._candidates[channel] = queue
            self._admit_waiters()
        else:
            queue[waiter] = size
        try:
            granted = await waiter
        except asyncio.CancelledError:
            if waiter.cancelled():
                if self._waiters.get(channel) is queue:
                    was_head = next(iter(queue)) is waiter
                    queue.pop(waiter, None)
                    if was_head:
                        self._head_changed(channel, queue)
                        self._admit_waiters()
            elif waiter.result():
                self.release(channel, size)
            raise
        if self.closed and granted:
            self.release(channel, size)
            return False
        return granted

    def release(self, channel: str, size: int) -> None:
        self.worker.release(size)
        if (scope := self.channels.get(channel)) is not None:
            scope.release(size)
        if self._waiters:
            if (queue := self._waiters.get(channel)) is not None:
                self._candidates[channel] = queue
            self._admit_waiters()

    def _charge(self, channel: str, size: int) -> None:
        self.worker.charge(size)
        if (scope := self.channels.get(channel)) is not None:
            scope.charge(size)

    def _head_changed(self, channel: str, queue: _WaitQueue) -> None:
        if queue:
            self._candidates[channel] = queue
        else:
            del self._waiters[channel]
            self._candidates.pop(channel, None)

    def _admit_waiters(self) -> None:
        worker = self.worker
        candidates = self._candidates
        while candidates and worker.fits(0):
            channel, queue = next(iter(candidates.items()))
            waiter, size = next(iter(queue.items()))
            if waiter.cancelled():
                queue.popitem(last=False)
                self._head_changed(channel, queue)
                continue

            scope = self.channels.get(channel)
            if scope is not None and not scope.fits(size):
                # A local limit can only change when this channel releases capacity.
                del candidates[channel]
                continue
            if not worker.fits(size):
                # Let the shared budget drain for the next eligible channel.
                return

            self._charge(channel, size)
            queue.popitem(last=False)
            if queue:
                candidates.move_to_end(channel)
            else:
                del self._waiters[channel]
                del candidates[channel]
            waiter.set_result(True)
