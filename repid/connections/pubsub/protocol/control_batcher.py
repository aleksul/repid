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


class _AckBatch:
    __slots__ = ("futures", "ids")

    def __init__(self) -> None:
        self.ids: list[str] = []
        self.futures: list[asyncio.Future[None]] = []


class _ModifyBatch:
    __slots__ = ("futures", "ids", "seconds")

    def __init__(self, seconds: int) -> None:
        self.ids: list[str] = []
        self.seconds = seconds
        self.futures: list[asyncio.Future[None]] = []


class PubsubControlBatcher:
    """Batches Pub/Sub control operations (ack, nack, reject, extend_deadline).

    Groups operations by (operation_type, subscription_path, seconds) and flushes
    on a timer or when a batch reaches *max_batch_ids*.  Each operation returns a
    future that resolves only after Pub/Sub has accepted the entire batch.
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

    async def _flush_loop(self) -> None:
        try:
            while not self._shutdown_event.is_set():
                await asyncio.sleep(self._flush_interval)
                await self._flush_all()
        except asyncio.CancelledError:
            pass

    async def add_ack(self, subscription_path: str, ack_id: str) -> None:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        immediate: _AckBatch | None = None

        async with self._lock:
            batch = self._ack_batches.get(subscription_path)
            if batch is None:
                batch = _AckBatch()
                self._ack_batches[subscription_path] = batch
            batch.ids.append(ack_id)
            batch.futures.append(future)

            if len(batch.ids) >= self._max_batch_ids:
                self._ack_batches.pop(subscription_path, None)
                immediate = batch

        if immediate is not None:
            await asyncio.shield(
                asyncio.create_task(self._execute_ack_batch(subscription_path, immediate)),
            )

        await future

    async def add_modify_deadline(
        self,
        subscription_path: str,
        ack_id: str,
        seconds: int,
    ) -> None:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        key = (subscription_path, seconds)
        immediate: _ModifyBatch | None = None

        async with self._lock:
            batch = self._modify_batches.get(key)
            if batch is None:
                batch = _ModifyBatch(seconds)
                self._modify_batches[key] = batch
            batch.ids.append(ack_id)
            batch.futures.append(future)

            if len(batch.ids) >= self._max_batch_ids:
                self._modify_batches.pop(key, None)
                immediate = batch

        if immediate is not None:
            await asyncio.shield(
                asyncio.create_task(self._execute_modify_batch(subscription_path, immediate)),
            )

        await future

    async def _flush_all(self) -> None:
        async with self._lock:
            ack_batches = self._ack_batches
            modify_batches = self._modify_batches
            self._ack_batches = {}
            self._modify_batches = {}

        tasks: list[asyncio.Task[None]] = []
        for subscription_path, ack_batch in ack_batches.items():
            tasks.append(
                asyncio.create_task(
                    self._execute_ack_batch(subscription_path, ack_batch),
                ),
            )
        for (subscription_path, _), modify_batch in modify_batches.items():
            tasks.append(
                asyncio.create_task(
                    self._execute_modify_batch(subscription_path, modify_batch),
                ),
            )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute_ack_batch(
        self,
        subscription_path: str,
        batch: _AckBatch,
    ) -> None:
        try:
            await self._client.acknowledge(subscription_path, batch.ids)
        except asyncio.CancelledError as e:
            for future in batch.futures:
                if not future.done():
                    future.set_exception(e)
            raise
        except Exception as e:
            logger.exception(
                "batcher.ack.failed",
                extra={"count": len(batch.ids)},
            )
            for future in batch.futures:
                if not future.done():
                    future.set_exception(e)
            return

        for future in batch.futures:
            if not future.done():
                future.set_result(None)

    async def _execute_modify_batch(
        self,
        subscription_path: str,
        batch: _ModifyBatch,
    ) -> None:
        try:
            await self._client.modify_ack_deadline(
                subscription_path,
                batch.ids,
                batch.seconds,
            )
        except asyncio.CancelledError as e:
            for future in batch.futures:
                if not future.done():
                    future.set_exception(e)
            raise
        except Exception as e:
            logger.exception(
                "batcher.modify.failed",
                extra={"count": len(batch.ids), "seconds": batch.seconds},
            )
            for future in batch.futures:
                if not future.done():
                    future.set_exception(e)
            return

        for future in batch.futures:
            if not future.done():
                future.set_result(None)
