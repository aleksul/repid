from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from math import inf

from repid._utils._typing import override
from repid.data.concurrency import Concurrency

_DEFAULT_WORKER_CONCURRENCY = Concurrency(messages=1000)


def _tighter_limit(worker_limit: int | None, channel_limit: int | None) -> int | None:
    if worker_limit is None:
        return channel_limit
    if channel_limit is None:
        return worker_limit
    return min(worker_limit, channel_limit)


def _channel_hints(
    channels: Iterable[str],
    worker: Concurrency,
    limits: Mapping[str, Concurrency],
) -> dict[str, Concurrency]:
    return {
        channel: (
            Concurrency(
                messages=_tighter_limit(worker.messages, limit.messages),
                payload_bytes=_tighter_limit(worker.payload_bytes, limit.payload_bytes),
            )
            if (limit := limits.get(channel)) is not None
            else worker
        )
        for channel in channels
    }


class _Usage:
    __slots__ = ("byte_limit", "message_limit", "messages", "payload_bytes")

    def __init__(self, limits: Concurrency) -> None:
        self.message_limit = inf if limits.messages is None else limits.messages
        self.byte_limit = inf if limits.payload_bytes is None else limits.payload_bytes
        self.messages = 0
        self.payload_bytes = 0

    def fits(self, size: int) -> bool:
        return self.messages < self.message_limit and (
            self.messages == 0 or self.payload_bytes + size <= self.byte_limit
        )

    def try_reserve(self, size: int) -> bool:
        messages = self.messages
        if messages >= self.message_limit:
            return False
        payload_bytes = self.payload_bytes + size
        # An empty scope can admit a single oversized delivery.
        if messages and payload_bytes > self.byte_limit:
            return False
        self.messages = messages + 1
        self.payload_bytes = payload_bytes
        return True

    def release(self, size: int) -> None:
        self.messages -= 1
        self.payload_bytes -= size


class _ChannelUsage(_Usage):
    __slots__ = ("worker",)

    def __init__(self, limits: Concurrency, worker: _Usage) -> None:
        super().__init__(limits)
        self.worker = worker

    @override
    def try_reserve(self, size: int) -> bool:
        worker = self.worker
        messages = self.messages
        worker_messages = worker.messages
        if worker_messages >= worker.message_limit or messages >= self.message_limit:
            return False
        payload_bytes = self.payload_bytes + size
        worker_payload_bytes = worker.payload_bytes + size
        if (worker_messages and worker_payload_bytes > worker.byte_limit) or (
            messages and payload_bytes > self.byte_limit
        ):
            return False
        self.messages = messages + 1
        self.payload_bytes = payload_bytes
        worker.messages = worker_messages + 1
        worker.payload_bytes = worker_payload_bytes
        return True

    @override
    def release(self, size: int) -> None:
        worker = self.worker
        self.messages -= 1
        self.payload_bytes -= size
        worker.messages -= 1
        worker.payload_bytes -= size


@dataclass(slots=True)
class _Reservation:
    controller: _ConcurrencyController
    channel: str
    usage: _Usage
    size: int
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        self.usage.release(self.size)
        if self.controller._waiters:
            self.controller._capacity_changed(self.channel)


_Waiter = asyncio.Future[_Reservation | None]
_WaitQueue = OrderedDict[_Waiter, int]


class _ConcurrencyController:
    """Atomically check and charge both budgets on the worker's event loop."""

    __slots__ = ("_candidates", "_waiters", "channels", "closed", "worker")

    def __init__(
        self,
        worker: Concurrency,
        channels: Mapping[str, Concurrency] | None = None,
    ) -> None:
        self.worker = _Usage(worker)
        self.channels: dict[str, _Usage] = {
            name: _ChannelUsage(limits, self.worker)
            for name, limits in (channels or {}).items()
            if limits.messages is not None or limits.payload_bytes is not None
        }
        self.closed = False
        self._waiters: dict[str, _WaitQueue] = {}
        # Channels rotate here until their head hits a local limit. The waiting
        # map retains parked queues until a release or head change reactivates them.
        self._candidates: OrderedDict[str, _WaitQueue] = OrderedDict()

    def close(self) -> None:
        self.closed = True
        for queue in self._waiters.values():
            for waiter in queue:
                if not waiter.done():
                    waiter.set_result(None)
        self._waiters.clear()
        self._candidates.clear()

    def try_acquire(self, channel: str, size: int) -> _Reservation | None:
        if self.closed or (self._waiters and (self._candidates or channel in self._waiters)):
            return None
        usage = self.channels.get(channel, self.worker)
        if usage.try_reserve(size):
            return _Reservation(self, channel, usage, size)
        return None

    async def acquire(self, channel: str, size: int) -> _Reservation | None:
        reservation = self.try_acquire(channel, size)
        if reservation is not None or self.closed:
            return reservation

        waiter: _Waiter = asyncio.get_running_loop().create_future()
        queue = self._waiters.get(channel)
        if queue is None:
            self._waiters[channel] = queue = OrderedDict(((waiter, size),))
            self._candidates[channel] = queue
            self._admit_waiters()
        else:
            queue[waiter] = size
        try:
            reservation = await waiter
        except asyncio.CancelledError:
            if waiter.cancelled():
                if self._waiters.get(channel) is queue:
                    was_head = next(iter(queue)) is waiter
                    queue.pop(waiter, None)
                    if was_head:
                        self._head_changed(channel, queue)
                        self._admit_waiters()
            elif granted := waiter.result():
                granted.release()
            raise
        if self.closed and reservation is not None:
            reservation.release()
            return None
        return reservation

    def _capacity_changed(self, channel: str) -> None:
        if (queue := self._waiters.get(channel)) is not None:
            self._candidates[channel] = queue
        self._admit_waiters()

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

            usage = self.channels.get(channel, worker)
            if usage is not worker and not usage.fits(size):
                # A local limit can only change when this channel releases capacity.
                del candidates[channel]
                continue
            if not usage.try_reserve(size):
                # Let the shared budget drain for the next eligible channel.
                return

            queue.popitem(last=False)
            if queue:
                candidates.move_to_end(channel)
            else:
                del self._waiters[channel]
                del candidates[channel]
            waiter.set_result(_Reservation(self, channel, usage, size))
