from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType

# Finished entries a long run at the head may pin before the queue compacts.
_COMPACTION_SLACK = 64


class _Deadline:
    __slots__ = ("cancelling", "done", "expired", "task", "when")

    def __init__(self, task: asyncio.Task[object], when: float) -> None:
        self.task = task
        self.when = when
        self.cancelling = task.cancelling()
        self.expired = False
        # Set once the entry no longer counts as live: finished or expired.
        self.done = False


class _DeadlineQueue:
    """Expire tasks that share one timeout using a single loop timer.

    Equal timeouts make deadlines monotonic in start order, so a FIFO stands in
    for the loop's timer heap and each run costs an append instead of a timer.
    """

    __slots__ = ("_entries", "_live", "_loop", "_timeout", "_timer")

    def __init__(self, loop: asyncio.AbstractEventLoop, timeout: float) -> None:
        self._loop = loop
        self._timeout = timeout
        self._entries: deque[_Deadline] = deque()
        self._live = 0
        self._timer: asyncio.TimerHandle | None = None

    def add(self, task: asyncio.Task[object]) -> _Deadline:
        entry = _Deadline(task, self._loop.time() + self._timeout)
        self._entries.append(entry)
        self._live += 1
        if self._timer is None:
            self._timer = self._loop.call_at(entry.when, self._expire)
        return entry

    def discard(self, entry: _Deadline) -> None:
        if entry.done:
            return
        entry.done = True
        self._live -= 1
        entries = self._entries
        while entries and entries[0].done:
            entries.popleft()
        # An emptied queue keeps its timer: when it fires it finds nothing to do and
        # stops. Cancelling would cost a new timer whenever in-flight runs drain.
        if len(entries) > 2 * self._live + _COMPACTION_SLACK:
            self._entries = deque(e for e in entries if not e.done)

    def _expire(self) -> None:
        self._timer = None
        now = self._loop.time()
        entries = self._entries
        while entries:
            entry = entries[0]
            if entry.done:
                entries.popleft()
            elif entry.when > now:
                self._timer = self._loop.call_at(entry.when, self._expire)
                return
            else:
                entries.popleft()
                entry.done = True
                entry.expired = True
                self._live -= 1
                entry.task.cancel()


class _Timeout:
    """Async context manager raising `TimeoutError` like `asyncio.timeout`."""

    __slots__ = ("_entry", "_queue")

    def __init__(self, queue: _DeadlineQueue) -> None:
        self._queue = queue
        self._entry: _Deadline | None = None

    async def __aenter__(self) -> None:
        task = asyncio.current_task()
        if task is None:  # pragma: no cover
            raise RuntimeError("Timeout should be used inside a task")
        self._entry = self._queue.add(task)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        entry = self._entry
        if entry is None:  # pragma: no cover
            return
        self._queue.discard(entry)
        # Only a cancellation this timeout issued, and nobody else requested, becomes a timeout.
        if (
            entry.expired
            and entry.task.uncancel() <= entry.cancelling
            and exc_type is asyncio.CancelledError
        ):
            raise TimeoutError from exc


class _Deadlines:
    """Actor timeouts sharing one loop timer per timeout value. Bound to one event loop."""

    __slots__ = ("_queues",)

    def __init__(self) -> None:
        self._queues: dict[float, _DeadlineQueue] = {}

    def timeout(self, delay: float) -> _Timeout:
        queue = self._queues.get(delay)
        if queue is None:
            queue = self._queues[delay] = _DeadlineQueue(asyncio.get_running_loop(), delay)
        return _Timeout(queue)
