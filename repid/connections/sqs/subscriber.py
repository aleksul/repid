from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from functools import partial
from typing import TYPE_CHECKING, Any

from repid.connections.abc import MessageCallbackT, SubscriberT
from repid.connections.sqs.message import SqsReceivedMessage
from repid.data.concurrency import Concurrency

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient
    from types_aiobotocore_sqs.type_defs import ReceiveMessageRequestTypeDef

    from repid.connections.sqs.message_broker import SqsServer

logger = logging.getLogger("repid.connections.sqs")


class SqsSubscriber(SubscriberT):
    def __init__(
        self,
        server: SqsServer,
        channels_to_callbacks: dict[str, MessageCallbackT],
        channel_concurrency: Mapping[str, Concurrency] | None = None,
    ) -> None:
        self._server = server
        self._channel_concurrency = dict(channel_concurrency or {})
        self._channels_to_callbacks = channels_to_callbacks
        self._active = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()
        self._pause_signal: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._shutdown_event = asyncio.Event()
        self._close_lock = asyncio.Lock()
        self._message_tasks: set[asyncio.Task[None]] = set()
        self._main_task: asyncio.Task[None] | None = None

        self._start_consuming()

    @property
    def is_active(self) -> bool:
        return (
            self._main_task is not None
            and not self._main_task.done()
            and self._paused_event.is_set()
            and not self._shutdown_event.is_set()
        )

    @property
    def task(self) -> asyncio.Task[object]:
        if self._main_task is None:
            raise RuntimeError("Subscriber is not active.")
        return self._main_task

    def _start_consuming(self) -> None:
        if self._main_task is not None and not self._main_task.done():
            return
        self._active = True
        self._paused_event.set()
        self._shutdown_event.clear()
        self._main_task = asyncio.create_task(self._consume())

    async def _consume(self) -> None:
        logger.info("subscriber.start")
        tasks = {
            asyncio.create_task(self._consume_channel(ch)) for ch in self._channels_to_callbacks
        }
        try:
            if tasks:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                for task in done:
                    task.result()
        except asyncio.CancelledError:
            logger.info("subscriber.cancelled")
            raise
        finally:
            await self._cancel_and_drain(tasks)
            self._active = False
            if self._shutdown_event.is_set():
                self._server._active_subscribers.discard(self)

    def _make_message(self, channel: str, queue_url: str, raw: Any) -> SqsReceivedMessage | None:
        try:
            return SqsReceivedMessage(
                self._server,
                channel,
                queue_url,
                raw,
                self._server._visibility_timeout,
            )
        except Exception:
            logger.exception("message.creation_error", extra={"channel": channel})
            return None

    async def _return_message(self, message: SqsReceivedMessage) -> None:
        try:
            await message.reject()
        except Exception:
            logger.exception("message.reject_unprocessed_error")

    async def _reject_unprocessed(
        self,
        channel: str,
        queue_url: str | None,
        messages: list[Any],
    ) -> None:
        if not messages or queue_url is None:
            return

        returns = asyncio.gather(
            *(
                self._return_message(message)
                for raw in messages
                if (message := self._make_message(channel, queue_url, raw)) is not None
            ),
            return_exceptions=True,
        )
        try:
            await asyncio.shield(returns)
        except asyncio.CancelledError:
            # Disconnect must wait for receipts we have already started returning.
            await asyncio.wait({returns})
            raise

    async def _cancel_and_drain(self, tasks: set[asyncio.Task[Any]]) -> None:
        for task in tasks:
            if not task.cancelling():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _can_receive(self) -> bool:
        return self._active and not self._shutdown_event.is_set() and self._paused_event.is_set()

    async def _receive_batch(
        self,
        client: SQSClient,
        channel: str,
        queue_url: str,
        count: int,
    ) -> list[Any]:
        pause_signal = self._pause_signal
        request: ReceiveMessageRequestTypeDef = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": count,
            "WaitTimeSeconds": self._server._receive_wait_time_seconds,
            "MessageAttributeNames": ["All"],
        }
        # The first renewal uses this timeout to schedule its wakeup. Request
        # the same lease on receipt even if the queue's default is shorter.
        if self._server._visibility_timeout > 0:
            request["VisibilityTimeout"] = self._server._visibility_timeout
        receive = asyncio.create_task(client.receive_message(**request))
        waits: set[asyncio.Future[Any]] = {receive, pause_signal}
        handed_off = False
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
            if receive.done() and not pause_signal.done() and self._can_receive():
                messages = receive.result().get("Messages", [])
                handed_off = True
                return messages
            return []
        finally:
            if not handed_off:
                receive.cancel()
                (response,) = await asyncio.gather(receive, return_exceptions=True)
                if not isinstance(response, BaseException):
                    await self._reject_unprocessed(channel, queue_url, response.get("Messages", []))

    def _message_done(
        self,
        message: SqsReceivedMessage,
        active: set[asyncio.Task[None]],
        returning: set[asyncio.Task[None]],
        task: asyncio.Task[None],
    ) -> None:
        self._message_tasks.discard(task)
        active.discard(task)
        if not task.cancelled():
            exc = task.exception()
            if exc is None:
                return
            logger.error("message.callback_error", exc_info=exc, extra={"channel": message.channel})
        if not message.is_acted_on:
            # This also handles cancellation before the callback starts.
            cleanup = asyncio.create_task(self._return_message(message))
            returning.add(cleanup)
            cleanup.add_done_callback(returning.discard)

    async def _poll_channel(
        self,
        client: SQSClient,
        channel: str,
        queue_url: str,
        *,
        count: int,
        active: set[asyncio.Task[None]],
        returning: set[asyncio.Task[None]],
    ) -> bool:
        messages = await self._receive_batch(client, channel, queue_url, count)
        if not messages:
            return False
        callback = self._channels_to_callbacks[channel]
        try:
            while messages:
                message = self._make_message(channel, queue_url, messages[0])
                if message is not None:
                    task = asyncio.create_task(self._process_message(message, callback))
                    self._message_tasks.add(task)
                    active.add(task)
                    task.add_done_callback(partial(self._message_done, message, active, returning))
                messages.pop(0)
        except BaseException:
            await self._reject_unprocessed(channel, queue_url, messages)
            raise
        return True

    async def _consume_channel(self, channel: str) -> None:
        logger.debug("consuming.started", extra={"channel": channel})
        active_message_tasks: set[asyncio.Task[None]] = set()
        returning: set[asyncio.Task[None]] = set()

        client = self._server._client
        if client is None:
            raise RuntimeError("SQS client is not connected.")

        queue_url = None
        hint = self._channel_concurrency.get(channel)
        limit = hint.messages if hint is not None else None
        max_messages = min(self._server._batch_size, 10)
        intake_limit = limit if limit and limit > 0 else max_messages

        try:
            while self._active and not self._shutdown_event.is_set():
                await self._paused_event.wait()
                if not self._can_receive():
                    continue

                # This channel has only one receive loop. While it polls,
                # active callbacks can finish but no new ones can start.
                pending = active_message_tasks | returning
                count = min(max_messages, intake_limit - len(pending))
                if not count:
                    await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    continue

                try:
                    if queue_url is None:
                        queue_url = await self._server._get_queue_url(channel)

                    if not self._can_receive():
                        continue
                    received = await self._poll_channel(
                        client,
                        channel,
                        queue_url,
                        count=count,
                        active=active_message_tasks,
                        returning=returning,
                    )
                except Exception:
                    logger.exception("consuming.receive_error", extra={"channel": channel})
                    await asyncio.sleep(1)
                    continue

                if not received and self._server._receive_wait_time_seconds <= 0:
                    await asyncio.sleep(0.1)
        finally:
            await self._cancel_and_drain(active_message_tasks)
            if returning:
                await asyncio.wait(returning)
        logger.debug("consuming.stopped", extra={"channel": channel})

    async def _process_message(
        self,
        message: SqsReceivedMessage,
        callback: MessageCallbackT,
    ) -> None:
        try:
            await callback(message)
        except Exception:
            logger.exception("message.callback_error", extra={"channel": message.channel})
            if not message.is_acted_on:
                try:
                    await message.nack()
                except Exception:
                    logger.exception("message.nack_error", extra={"channel": message.channel})

    async def pause(self) -> None:
        self._paused_event.clear()
        if not self._pause_signal.done():
            self._pause_signal.set_result(None)
        logger.info("subscriber.paused")

    async def resume(self) -> None:
        if self._shutdown_event.is_set():
            return
        if not self._active:
            self._active = True
        if self._pause_signal.done():
            self._pause_signal = asyncio.get_running_loop().create_future()
        self._paused_event.set()
        if self._main_task is None or self._main_task.done():
            self._start_consuming()
        logger.info("subscriber.resumed")

    async def close(self) -> None:
        if asyncio.current_task() in self._message_tasks:
            # The consumer joins this callback during shutdown.
            self._request_close()
            return
        async with self._close_lock:
            logger.info("subscriber.closing")
            self._request_close()
            if self._main_task is not None:
                await asyncio.wait({self._main_task})
                if not self._main_task.cancelled():
                    self._main_task.exception()
                self._main_task = None
            self._server._active_subscribers.discard(self)
            logger.info("subscriber.closed")

    def _request_close(self) -> None:
        self._active = False
        self._shutdown_event.set()
        if not self._pause_signal.done():
            self._pause_signal.set_result(None)
        self._paused_event.set()
        if self._main_task is not None and not self._main_task.cancelling():
            self._main_task.cancel()
