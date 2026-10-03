from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from functools import partial
from typing import TYPE_CHECKING, Any

from repid.connections._buffer import SubmissionBuffer, stop_task
from repid.connections.abc import ReceivedMessageT, SubscriberT
from repid.connections.sqs.message import SqsReceivedMessage
from repid.limits import UNLIMITED_NATIVE_FLOW, NativeFlow

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient

    from repid.connections.sqs.message_broker import SqsServer

logger = logging.getLogger("repid.connections.sqs")


class SqsSubscriber(SubscriberT):
    def __init__(
        self,
        server: SqsServer,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
    ) -> None:
        self._server = server
        self._native_flow = native_flow
        self._channels_to_callbacks = channels_to_callbacks
        self._active = True
        self._finished = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()
        self._shutdown_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._main_task: asyncio.Task | None = None
        self._start_consuming()

    @property
    def native_flow(self) -> NativeFlow:
        return self._native_flow

    @property
    def is_active(self) -> bool:
        return self._active and self._main_task is not None and not self._main_task.done()

    @property
    def task(self) -> asyncio.Task:
        if self._main_task is None:
            raise RuntimeError("Subscriber is not active")
        return self._main_task

    def _start_consuming(self) -> None:
        if self._main_task is not None and not self._main_task.done():
            return
        self._main_task = asyncio.create_task(self._consume())

    async def _consume(self) -> None:
        self._tasks = [
            asyncio.create_task(self._consume_channel(channel))
            for channel in self._channels_to_callbacks
        ]
        try:
            await asyncio.gather(*self._tasks)
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._active = False

    async def _consume_channel(self, channel: str) -> None:
        client = self._server._client
        if client is None:
            raise RuntimeError("SQS client is not connected.")
        buffer = SubmissionBuffer()
        queue_url = await self._server._get_queue_url(channel)
        try:
            await buffer.run(partial(self._receive_channel, channel, queue_url, buffer, client))
        finally:
            await buffer.dispose()

    async def _receive_channel(
        self,
        channel: str,
        queue_url: str,
        buffer: SubmissionBuffer,
        client: SQSClient,
    ) -> None:
        while not self._shutdown_event.is_set():
            await self._paused_event.wait()
            if self._shutdown_event.is_set():
                break
            try:
                response = await client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=self._server._batch_size,
                    WaitTimeSeconds=self._server._receive_wait_time_seconds,
                    MessageAttributeNames=["All"],
                )
            except Exception:
                logger.exception("consuming.receive_error", extra={"channel": channel})
                await asyncio.sleep(1)
                continue
            messages = [
                SqsReceivedMessage(
                    self._server,
                    channel,
                    queue_url,
                    raw,
                    self._server._visibility_timeout,
                )
                for raw in response.get("Messages", [])
            ]
            buffer.track(messages)
            for message in messages:
                await self._paused_event.wait()
                if self._shutdown_event.is_set():
                    break
                await buffer.submit(message, self._channels_to_callbacks[channel])
            if not messages:
                await asyncio.sleep(0.1)

    async def _process_message(
        self,
        channel: str,
        queue_url: str,
        msg: Any,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        buffer = SubmissionBuffer()
        message = SqsReceivedMessage(
            self._server,
            channel,
            queue_url,
            msg,
            self._server._visibility_timeout,
        )
        buffer.track((message,))
        try:
            await buffer.submit(message, callback)
        finally:
            await buffer.dispose()

    async def _reject_unprocessed(
        self,
        channel: str,
        queue_url: str | None,
        messages: list[Any],
    ) -> None:
        if queue_url is not None:
            for raw in messages:
                await SqsReceivedMessage(
                    self._server,
                    channel,
                    queue_url,
                    raw,
                    self._server._visibility_timeout,
                ).reject()

    async def pause(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("SQS supports worker pause only")
        self._paused_event.clear()

    async def resume(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("SQS supports worker pause only")
        if not self._shutdown_event.is_set():
            self._paused_event.set()

    async def stop(self) -> None:
        if self._shutdown_event.is_set():
            return
        self._shutdown_event.set()
        self._active = False
        if self._main_task is not None:
            await stop_task(self._main_task)

    async def finish(self) -> None:
        try:
            await self.stop()
        finally:
            self._finished = True
            self._server._active_subscribers.discard(self)
