from __future__ import annotations

import asyncio
from math import inf
from typing import TYPE_CHECKING

from repid._concurrency import _Scope
from repid.connections.abc import Demand

if TYPE_CHECKING:
    from collections.abc import Callable

    from repid._concurrency import _Budget
    from repid.connections.abc import ReceivedMessageT
    from repid.data.concurrency import Concurrency

_CLOSED_DEMAND = Demand(messages=0, payload_bytes=0)


def _has_room(scope: _Scope) -> bool:
    # An empty scope can take a single oversized delivery, as admission allows.
    return scope.messages < scope.message_limit and (
        scope.messages == 0 or scope.payload_bytes < scope.byte_limit
    )


def _remaining(limit: float, used: int) -> int | None:
    return None if limit == inf else max(0, int(limit) - used)


def _tighter(left: int | None, right: int | None) -> int | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


class _ReadySignal:
    """Readiness of one scope, as futures handed to readers.

    While the scope has room, every read returns the same done future, so the
    check allocates nothing. Without room, each read returns a future of its own,
    so a reader awaiting it can be cancelled without affecting other readers.
    """

    __slots__ = ("_done", "_waiters")

    def __init__(self) -> None:
        self._done: asyncio.Future[None] | None = None
        self._waiters: list[asyncio.Future[None]] = []

    def get(self, *, has_room: bool) -> asyncio.Future[None]:
        if has_room:
            done = self._done
            if done is None:
                done = self._done = asyncio.get_running_loop().create_future()
                done.set_result(None)
            return done
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        return waiter

    def refresh(self, *, has_room: bool) -> None:
        if has_room and self._waiters:
            waiters = self._waiters
            self._waiters = []
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_result(None)


class _ChannelIntake:
    __slots__ = ("_intake", "_ready", "_scope", "name", "submit")

    def __init__(
        self,
        intake: _Intake,
        name: str,
        scope: _Scope | None,
        submit: Callable[[ReceivedMessageT], None],
    ) -> None:
        self._intake = intake
        self.name = name
        # Channels without a limit of their own share the worker's scope.
        self._scope = scope
        self._ready = _ReadySignal()
        self.submit = submit

    @property
    def limits(self) -> Concurrency:
        return self._intake._budget.limits(self.name)

    @property
    def demand(self) -> Demand:
        worker = self._intake.demand
        scope = self._scope
        if scope is None or worker is _CLOSED_DEMAND:
            return worker
        return Demand(
            messages=_tighter(worker.messages, _remaining(scope.message_limit, scope.messages)),
            payload_bytes=_tighter(
                worker.payload_bytes,
                _remaining(scope.byte_limit, scope.payload_bytes),
            ),
        )

    @property
    def ready(self) -> asyncio.Future[None]:
        return self._ready.get(has_room=self._has_room())

    def _has_room(self) -> bool:
        scope = self._scope
        return self._intake._has_room() and (scope is None or _has_room(scope))

    def _refresh(self) -> None:
        self._ready.refresh(has_room=self._has_room())


class _Intake:
    """Outstanding deliveries per scope, published to adapters as demand and readiness.

    A delivery is outstanding from submission until the worker finishes with it,
    whether it waits for budget or runs.
    """

    __slots__ = ("_budget", "_channel_scopes", "_closed", "_ready", "_worker", "channels")

    def __init__(self, budget: _Budget) -> None:
        self._budget = budget
        self._worker = _Scope(budget.worker.limits)
        self._channel_scopes: dict[str, _Scope] = {}
        self.channels: dict[str, _ChannelIntake] = {}
        self._ready = _ReadySignal()
        self._closed = False

    def add_channel(
        self,
        name: str,
        submit: Callable[[ReceivedMessageT], None],
    ) -> _ChannelIntake:
        scope = None
        if (budget_scope := self._budget.channels.get(name)) is not None:
            scope = self._channel_scopes[name] = _Scope(budget_scope.limits)
        channel = self.channels[name] = _ChannelIntake(self, name, scope, submit)
        return channel

    @property
    def limits(self) -> Concurrency:
        return self._budget.worker.limits

    @property
    def demand(self) -> Demand:
        if self._closed:
            return _CLOSED_DEMAND
        worker = self._worker
        return Demand(
            messages=_remaining(worker.message_limit, worker.messages),
            payload_bytes=_remaining(worker.byte_limit, worker.payload_bytes),
        )

    @property
    def ready(self) -> asyncio.Future[None]:
        return self._ready.get(has_room=self._has_room())

    def _has_room(self) -> bool:
        return not self._closed and _has_room(self._worker)

    def charge(self, channel: str, size: int) -> None:
        self._update(channel, size, 1)

    def release(self, channel: str, size: int) -> None:
        self._update(channel, -size, -1)

    def _update(self, channel: str, size: int, count: int) -> None:
        worker = self._worker
        worker_had_room = _has_room(worker)
        worker.messages += count
        worker.payload_bytes += size
        scope = self._channel_scopes.get(channel)
        channel_had_room = True
        if scope is not None:
            channel_had_room = _has_room(scope)
            scope.messages += count
            scope.payload_bytes += size
        if _has_room(worker) != worker_had_room:
            self._refresh_all()
        elif scope is not None and _has_room(scope) != channel_had_room:
            self.channels[channel]._refresh()

    def close(self) -> None:
        """Stop publishing room: readers stay waiting until the subscriber closes."""
        if not self._closed:
            self._closed = True
            self._refresh_all()

    def _refresh_all(self) -> None:
        self._ready.refresh(has_room=self._has_room())
        for channel in self.channels.values():
            channel._refresh()
