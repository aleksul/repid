from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from repid.connections._submission import _channel_prefetch, _SubmissionQueue
from repid.connections.abc import Intake, SubscriberT
from repid.connections.sqs.message import SqsReceivedMessage, _finish_operation

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient
    from types_aiobotocore_sqs.type_defs import (
        MessageTypeDef,
        ReceiveMessageRequestTypeDef,
        ReceiveMessageResultTypeDef,
    )

    from repid.connections.sqs.message_broker import SqsServer

logger = logging.getLogger("repid.connections.sqs")


class SqsSubscriber(SubscriberT):
    def __init__(
        self,
        server: SqsServer,
        intake: Intake,
        prefetch: int | None = None,
    ) -> None:
        self._server = server
        self._intakes = intake.channels
        self._prefetch = {
            channel: _channel_prefetch(
                prefetch,
                channel_intake.limits.messages,
                min(server._batch_size, 10),
            )
            for channel, channel_intake in intake.channels.items()
        }
        # Received messages the worker has no room for yet, renewed until submitted.
        self._submissions: dict[str, _SubmissionQueue[SqsReceivedMessage]] = {
            channel: _SubmissionQueue(channel_intake, keep_alive=True)
            for channel, channel_intake in intake.channels.items()
        }
        self._active = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()
        self._pause_signal: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._shutdown_event = asyncio.Event()
        self._close_lock = asyncio.Lock()
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
    def task(self) -> asyncio.Task[None]:
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
        tasks = {asyncio.create_task(self._consume_channel(ch)) for ch in self._intakes}
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

    def _make_message(
        self,
        channel: str,
        queue_url: str,
        raw: MessageTypeDef,
    ) -> SqsReceivedMessage | None:
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
        messages: list[MessageTypeDef],
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
        await _finish_operation(returns)

    async def _cancel_and_drain(self, tasks: set[asyncio.Task[None]]) -> None:
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
    ) -> list[MessageTypeDef]:
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
        handed_off = False
        try:
            await asyncio.wait({receive, pause_signal}, return_when=asyncio.FIRST_COMPLETED)
            if receive.done() and not pause_signal.done() and self._can_receive():
                messages = receive.result().get("Messages", [])
                handed_off = True
                return messages
            return []
        finally:
            if not handed_off:
                receive.cancel()
                # Close may follow pause while the cancelled poll is still
                # returning a response. Drain it before closing the client.
                await _finish_operation(
                    self._return_unclaimed_response(receive, channel, queue_url),
                )

    async def _return_unclaimed_response(
        self,
        receive: asyncio.Task[ReceiveMessageResultTypeDef],
        channel: str,
        queue_url: str,
    ) -> None:
        (response,) = await asyncio.gather(receive, return_exceptions=True)
        if not isinstance(response, BaseException):
            await self._reject_unprocessed(channel, queue_url, response.get("Messages", []))

    async def _poll_channel(
        self,
        client: SQSClient,
        channel: str,
        queue_url: str,
        *,
        count: int,
    ) -> bool:
        messages = await self._receive_batch(client, channel, queue_url, count)
        if not messages:
            return False
        submissions = self._submissions[channel]
        try:
            while messages:
                message = self._make_message(channel, queue_url, messages[0])
                if message is not None:
                    submissions.put(message)
                messages.pop(0)
        except BaseException:
            await self._reject_unprocessed(channel, queue_url, messages)
            raise
        return True

    def _receive_count(self, channel: str) -> int:
        demand = self._intakes[channel].demand.messages
        max_messages = min(self._server._batch_size, 10)
        room = max_messages if demand is None else demand
        # Receive ahead of the worker's room by up to `prefetch` held messages.
        held = len(self._submissions[channel])
        return max(0, min(max_messages, room + self._prefetch[channel] - held))

    async def _consume_channel(self, channel: str) -> None:
        logger.debug("consuming.started", extra={"channel": channel})
        client = self._server._client
        if client is None:
            raise RuntimeError("SQS client is not connected.")

        queue_url = None
        intake = self._intakes[channel]
        try:
            while self._active and not self._shutdown_event.is_set():
                await self._paused_event.wait()
                if not self._can_receive():
                    continue

                count = self._receive_count(channel)
                if not count:
                    ready = intake.ready
                    if not ready.done():
                        await ready
                    continue

                try:
                    if queue_url is None:
                        queue_url = await self._server._get_queue_url(channel)

                    if not self._can_receive():
                        continue
                    received = await self._poll_channel(client, channel, queue_url, count=count)
                except Exception:
                    logger.exception("consuming.receive_error", extra={"channel": channel})
                    await asyncio.sleep(1)
                    continue

                if not received and self._server._receive_wait_time_seconds <= 0:
                    await asyncio.sleep(0.1)
        finally:
            await self._return_held(channel)
        logger.debug("consuming.stopped", extra={"channel": channel})

    async def _return_held(self, channel: str) -> None:
        held = self._submissions[channel].close()
        if held:
            # Messages the worker never received become visible again.
            await _finish_operation(asyncio.gather(*(self._return_message(m) for m in held)))

    async def pause(self) -> None:
        self._paused_event.clear()
        for submissions in self._submissions.values():
            submissions.pause()
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
        for submissions in self._submissions.values():
            submissions.resume()
        if self._main_task is None or self._main_task.done():
            self._start_consuming()
        logger.info("subscriber.resumed")

    async def close(self) -> None:
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
