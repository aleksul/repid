"""Control batching for Pub/Sub ack/nack/reject/deadline operations.

Pub/Sub unary Acknowledge and ModifyAckDeadline RPCs accept lists of ack IDs
per request. This batcher groups concurrent per-message operations into larger
batches to reduce RPC overhead, while preserving the per-message future contract
(an action's future completes only after Pub/Sub accepted it).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from contextlib import suppress
from typing import Protocol

logger = logging.getLogger("repid.connections.pubsub.protocol")


class ClientProtocol(Protocol):
    async def acknowledge(self, subscription_path: str, ack_ids: Sequence[str]) -> None: ...

    async def modify_ack_deadline(
        self,
        subscription_path: str,
        ack_ids: Sequence[str],
        seconds: int,
    ) -> None: ...


class OperationInProgressOnCancellation(asyncio.CancelledError):
    """The caller was cancelled after its control operation left the pending batch.

    The RPC may already have reached Pub/Sub, so a message must not attempt a
    different disposition after receiving this exception.
    """


class _Operation:
    __slots__ = ("ack_id", "future")

    def __init__(self, ack_id: str, future: asyncio.Future[None]) -> None:
        self.ack_id = ack_id
        self.future = future


class _AckBatch:
    __slots__ = ("operations",)

    def __init__(self) -> None:
        self.operations: list[_Operation] = []

    @property
    def ids(self) -> list[str]:
        return [operation.ack_id for operation in self.operations]


class _ModifyBatch:
    __slots__ = ("operations", "seconds")

    def __init__(self, seconds: int) -> None:
        self.operations: list[_Operation] = []
        self.seconds = seconds

    @property
    def ids(self) -> list[str]:
        return [operation.ack_id for operation in self.operations]


class PubsubControlBatcher:
    """Batches Pub/Sub control operations (ack, nack, reject, extend_deadline).

    Groups operations by (operation_type, subscription_path, seconds) and flushes
    on a timer or when a batch reaches *max_batch_ids*. Each operation resolves
    only after Pub/Sub has accepted the entire batch. Cancellation withdraws an
    operation while it is definitely unsent; once a batch is detached for
    flushing, cancellation reports that its outcome is unknown.
    """

    def __init__(
        self,
        client: ClientProtocol,
        *,
        flush_interval: float = 0.01,
        max_batch_ids: int = 1000,
    ) -> None:
        self._client = client
        self._flush_interval = flush_interval
        self._max_batch_ids = max_batch_ids

        self._lock = asyncio.Lock()
        self._ack_batches: dict[str, _AckBatch] = {}
        self._modify_batches: dict[tuple[str, int], _ModifyBatch] = {}
        self._task: asyncio.Task[None] | None = None
        self._immediate_tasks: set[asyncio.Task[None]] = set()
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        self._shutdown_event.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self._flush_all()
        await self._drain_immediate_tasks()

    async def _drain_immediate_tasks(self) -> None:
        """Wait for detached full batches, including tasks added while draining."""
        while tasks := tuple(self._immediate_tasks):
            await asyncio.gather(*tasks, return_exceptions=True)
            self._immediate_tasks.difference_update(tasks)

    async def _flush_loop(self) -> None:
        try:
            while not self._shutdown_event.is_set():
                await asyncio.sleep(self._flush_interval)
                await self._flush_all()
        except asyncio.CancelledError:
            pass

    async def add_ack(self, subscription_path: str, ack_id: str) -> None:
        future = asyncio.get_running_loop().create_future()
        operation = _Operation(ack_id, future)
        immediate: _AckBatch | None = None

        async with self._lock:
            batch = self._ack_batches.get(subscription_path)
            if batch is None:
                batch = _AckBatch()
                self._ack_batches[subscription_path] = batch
            batch.operations.append(operation)

            if len(batch.operations) >= self._max_batch_ids:
                self._ack_batches.pop(subscription_path, None)
                immediate = batch

        if immediate is not None:
            self._track_immediate_task(
                asyncio.create_task(self._execute_ack_batch(subscription_path, immediate)),
            )

        await self._await_operation(operation)

    async def add_modify_deadline(
        self,
        subscription_path: str,
        ack_id: str,
        seconds: int,
    ) -> None:
        future = asyncio.get_running_loop().create_future()
        operation = _Operation(ack_id, future)
        key = (subscription_path, seconds)
        immediate: _ModifyBatch | None = None

        async with self._lock:
            batch = self._modify_batches.get(key)
            if batch is None:
                batch = _ModifyBatch(seconds)
                self._modify_batches[key] = batch
            batch.operations.append(operation)

            if len(batch.operations) >= self._max_batch_ids:
                self._modify_batches.pop(key, None)
                immediate = batch

        if immediate is not None:
            self._track_immediate_task(
                asyncio.create_task(self._execute_modify_batch(subscription_path, immediate)),
            )

        await self._await_operation(operation)

    def _track_immediate_task(self, task: asyncio.Task[None]) -> None:
        self._immediate_tasks.add(task)
        task.add_done_callback(self._immediate_tasks.discard)

    async def _await_operation(self, operation: _Operation) -> None:
        future = operation.future
        # Relay the batch future into a private outer future instead of using
        # asyncio.shield(): since Python 3.14, shield() attaches a callback to
        # the inner future that reports any exception to the loop's exception
        # handler once the awaiting side is cancelled, even if the batcher
        # consumes it afterwards.
        outer: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def _relay(inner: asyncio.Future[None]) -> None:
            # Always retrieve the outcome so the inner future is never left
            # with an unretrieved exception or result.
            if inner.cancelled():
                if not outer.done():
                    outer.cancel()
                return
            exc = inner.exception()
            if outer.done():
                return
            if exc is not None:
                outer.set_exception(exc)
            else:
                outer.set_result(inner.result())

        future.add_done_callback(_relay)
        try:
            # A caller cancelling its task must not cancel a shared batch future.
            await outer
        except asyncio.CancelledError:
            if outer.done() and not outer.cancelled():
                # The batch failed at the same moment the caller was cancelled;
                # consume the error so it cannot be reported as unhandled.
                outer.exception()
            if await self._withdraw_pending(operation):
                future.cancel()
                raise
            # No caller remains to await this detached operation. Consume a
            # later RPC failure so its future cannot become an unhandled error.
            future.add_done_callback(self._consume_abandoned_future)
            raise OperationInProgressOnCancellation from None

    @staticmethod
    def _consume_abandoned_future(future: asyncio.Future[None]) -> None:
        if not future.cancelled():
            future.exception()

    async def _withdraw_pending(self, operation: _Operation) -> bool:
        """Remove an operation only when it remains in a batch owned by the queue."""
        async with self._lock:
            for subscription_path, ack_batch in list(self._ack_batches.items()):
                if operation in ack_batch.operations:
                    ack_batch.operations.remove(operation)
                    if not ack_batch.operations:
                        self._ack_batches.pop(subscription_path)
                    return True
            for key, modify_batch in list(self._modify_batches.items()):
                if operation in modify_batch.operations:
                    modify_batch.operations.remove(operation)
                    if not modify_batch.operations:
                        self._modify_batches.pop(key)
                    return True
        return False

    async def _flush_all(self) -> None:
        async with self._lock:
            ack_batches = self._ack_batches
            modify_batches = self._modify_batches
            self._ack_batches = {}
            self._modify_batches = {}

        tasks: list[asyncio.Task[None]] = []
        for subscription_path, ack_batch in ack_batches.items():
            tasks.append(asyncio.create_task(self._execute_ack_batch(subscription_path, ack_batch)))
        for (subscription_path, _), modify_batch in modify_batches.items():
            tasks.append(
                asyncio.create_task(self._execute_modify_batch(subscription_path, modify_batch)),
            )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute_ack_batch(self, subscription_path: str, batch: _AckBatch) -> None:
        try:
            await self._client.acknowledge(subscription_path, batch.ids)
        except asyncio.CancelledError as exc:
            for operation in batch.operations:
                if not operation.future.done():
                    operation.future.set_exception(exc)
            raise
        except Exception as exc:
            logger.exception("batcher.ack.failed", extra={"count": len(batch.operations)})
            for operation in batch.operations:
                if not operation.future.done():
                    operation.future.set_exception(exc)
            return

        for operation in batch.operations:
            if not operation.future.done():
                operation.future.set_result(None)

    async def _execute_modify_batch(self, subscription_path: str, batch: _ModifyBatch) -> None:
        try:
            await self._client.modify_ack_deadline(subscription_path, batch.ids, batch.seconds)
        except asyncio.CancelledError as exc:
            for operation in batch.operations:
                if not operation.future.done():
                    operation.future.set_exception(exc)
            raise
        except Exception as exc:
            logger.exception(
                "batcher.modify.failed",
                extra={"count": len(batch.operations), "seconds": batch.seconds},
            )
            for operation in batch.operations:
                if not operation.future.done():
                    operation.future.set_exception(exc)
            return

        for operation in batch.operations:
            if not operation.future.done():
                operation.future.set_result(None)
