from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True, slots=True)
class Concurrency:
    """Message count and serialized payload byte budgets.

    Each field accepts a positive integer or None for unlimited. A delivery
    larger than a byte budget waits for that scope to empty, then runs alone
    within it. Worker and channel budgets both apply when configured.
    """

    messages: int | None = None
    payload_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.messages is not None and (
            isinstance(self.messages, bool)
            or not isinstance(self.messages, int)
            or self.messages <= 0
        ):
            raise ValueError("messages must be a positive integer or None")
        if self.payload_bytes is not None and (
            isinstance(self.payload_bytes, bool)
            or not isinstance(self.payload_bytes, int)
            or self.payload_bytes <= 0
        ):
            raise ValueError("payload_bytes must be a positive integer or None")


_DEFAULT_WORKER_CONCURRENCY = Concurrency(messages=1000)


def _channel_hints(
    channels: Iterable[str],
    worker: Concurrency,
    limits: Mapping[str, Concurrency],
) -> dict[str, Concurrency]:
    def tighter(worker_limit: int | None, channel_limit: int | None) -> int | None:
        if worker_limit is None:
            return channel_limit
        if channel_limit is None:
            return worker_limit
        return min(worker_limit, channel_limit)

    return {
        channel: (
            Concurrency(
                messages=tighter(worker.messages, limit.messages),
                payload_bytes=tighter(worker.payload_bytes, limit.payload_bytes),
            )
            if (limit := limits.get(channel)) is not None
            else worker
        )
        for channel in channels
    }


@dataclass(slots=True)
class _Usage:
    message_limit: int | None
    byte_limit: int | None
    messages: int = 0
    payload_bytes: int = 0
    address: str = ""

    def fits(self, size: int) -> bool:
        limit = self.message_limit
        if limit is not None and self.messages >= limit:
            return False
        cap = self.byte_limit
        if cap is None or self.payload_bytes + size <= cap:
            return True
        return size > cap and self.messages == 0


@dataclass(slots=True)
class _Reservation:
    controller: _ConcurrencyController
    channel: _Usage | None
    size: int
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        worker = self.controller.worker
        size = self.size
        worker.messages -= 1
        if size:
            worker.payload_bytes -= size
        channel = self.channel
        if channel is not None:
            channel.messages -= 1
            if size:
                channel.payload_bytes -= size
        if self.controller._waiters:
            self.controller._capacity_changed(channel)


_Waiter = asyncio.Future[_Reservation | None]
_WaitQueue = OrderedDict[_Waiter, int]


class _ConcurrencyController:
    """Atomically check and charge both budgets on the worker's event loop."""

    def __init__(
        self,
        worker: Concurrency,
        channels: Mapping[str, Concurrency] | None = None,
    ) -> None:
        self.worker = _Usage(worker.messages, worker.payload_bytes)
        self.channels = {
            name: _Usage(limits.messages, limits.payload_bytes, address=name)
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

    def _reserve(self, usage: _Usage | None, size: int) -> _Reservation:
        worker = self.worker
        worker.messages += 1
        if size:
            worker.payload_bytes += size
        if usage is not None:
            usage.messages += 1
            if size:
                usage.payload_bytes += size
        return _Reservation(self, usage, size)

    def try_acquire(self, channel: str, size: int) -> _Reservation | None:
        if self.closed or (self._waiters and (self._candidates or channel in self._waiters)):
            return None
        usage = self.channels.get(channel)
        if self.worker.fits(size) and (usage is None or usage.fits(size)):
            return self._reserve(usage, size)
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

    def _capacity_changed(self, usage: _Usage | None) -> None:
        if usage is not None and (queue := self._waiters.get(usage.address)) is not None:
            self._candidates[usage.address] = queue
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

            usage = self.channels.get(channel)
            if usage is not None and not usage.fits(size):
                # A local limit can only change when this channel releases capacity.
                del candidates[channel]
                continue
            if not worker.fits(size):
                # Let the shared budget drain for the next eligible channel.
                return

            queue.popitem(last=False)
            if queue:
                candidates.move_to_end(channel)
            else:
                del self._waiters[channel]
                del candidates[channel]
            waiter.set_result(self._reserve(usage, size))
