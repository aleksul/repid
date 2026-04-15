from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from repid.connections.abc import ReceivedMessageT, SubscriberT
from repid.connections.sqs.message import SqsReceivedMessage

if TYPE_CHECKING:
    from repid.connections.sqs.message_broker import SqsServer

logger = logging.getLogger("repid.connections.sqs")


class SqsSubscriber(SubscriberT):
    def __init__(
        self,
        server: SqsServer,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> None:
        if concurrency_limit is not None and concurrency_limit <= 0:
            raise ValueError(  # pragma: no cover
                "concurrency_limit must be None or a positive integer.",
            )

        self._server = server
        self._channels_to_callbacks = channels_to_callbacks
        self._concurrency_limit = concurrency_limit
        self._semaphore = (
            asyncio.Semaphore(concurrency_limit) if concurrency_limit is not None else None
        )
        self._active = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()
        self._pause_requested_event = asyncio.Event()
        self._shutdown_event = asyncio.Event()
        self._shutdown_wait_task: asyncio.Task[bool] | None = None
        self._pause_wait_task: asyncio.Task[bool] | None = None
        self._tasks: list[asyncio.Task] = []
        self._main_task: asyncio.Task | None = None

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
    def task(self) -> asyncio.Task:
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
        for channel in self._channels_to_callbacks:
            self._tasks.append(asyncio.create_task(self._consume_channel(channel)))

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("subscriber.cancelled")
            raise
        finally:
            self._active = False
            self._tasks.clear()

    async def _reject_unprocessed(
        self,
        channel: str,
        queue_url: str | None,
        messages: list[Any],
    ) -> None:
        if not messages or queue_url is None:
            return

        for msg in messages:
            try:
                # Run reject in detached task to avoid blocking loop teardown.
                t = asyncio.create_task(
                    SqsReceivedMessage(self._server, channel, queue_url, msg).reject(),
                )

                def _ignore_exception(task: asyncio.Task) -> None:
                    with contextlib.suppress(Exception, asyncio.CancelledError):
                        task.result()

                t.add_done_callback(_ignore_exception)
            except Exception:
                logger.exception(
                    "message.reject_unprocessed_error",
                    extra={"channel": channel},
                )

    async def _cancel_and_drain(self, tasks: set[asyncio.Task[Any]]) -> None:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _get_shutdown_wait_task(self) -> asyncio.Task[bool]:
        if self._shutdown_wait_task is None or (
            self._shutdown_wait_task.done() and not self._shutdown_event.is_set()
        ):
            self._shutdown_wait_task = asyncio.create_task(self._shutdown_event.wait())
        return self._shutdown_wait_task

    def _get_pause_wait_task(self) -> asyncio.Task[bool]:
        if self._pause_wait_task is None or (
            self._pause_wait_task.done() and not self._pause_requested_event.is_set()
        ):
            self._pause_wait_task = asyncio.create_task(self._pause_requested_event.wait())
        return self._pause_wait_task

    async def _consume_channel(self, channel: str) -> None:  # noqa: C901, PLR0912, PLR0915
        logger.debug("consuming.started", extra={"channel": channel})
        callback = self._channels_to_callbacks[channel]
        active_message_tasks: set[asyncio.Task[Any]] = set()

        client = self._server._client
        if client is None:
            raise RuntimeError("SQS client is not connected.")

        queue_url = None
        max_messages = self._server._batch_size

        while self._active and not self._shutdown_event.is_set():
            await self._paused_event.wait()
            if self._shutdown_event.is_set():
                break

            messages = []
            try:
                if queue_url is None:
                    queue_url = await self._server._get_queue_url(channel)

                receive_task = asyncio.create_task(
                    client.receive_message(
                        QueueUrl=queue_url,
                        MaxNumberOfMessages=max_messages,
                        WaitTimeSeconds=self._server._receive_wait_time_seconds,
                        MessageAttributeNames=["All"],
                    ),
                )
                shutdown_task = self._get_shutdown_wait_task()
                pause_task = self._get_pause_wait_task()

                done, pending = await asyncio.wait(
                    {receive_task, shutdown_task, pause_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                pending_to_cancel = {task for task in pending if task is receive_task}
                await self._cancel_and_drain(pending_to_cancel)

                if receive_task not in done:
                    continue

                response = await receive_task

                messages = response.get("Messages", [])
                if not messages and self._server._receive_wait_time_seconds <= 0:
                    await asyncio.sleep(0.1)
                    continue
                for msg in messages.copy():
                    if self._shutdown_event.is_set() or not self._paused_event.is_set():
                        break

                    if self._semaphore:
                        acquired = False
                        while not acquired:
                            if self._shutdown_event.is_set() or not self._paused_event.is_set():
                                break
                            acquire_task = asyncio.create_task(self._semaphore.acquire())
                            shutdown_task = self._get_shutdown_wait_task()
                            pause_task = self._get_pause_wait_task()

                            done, pending = await asyncio.wait(
                                {acquire_task, shutdown_task, pause_task},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            pending_to_cancel = {task for task in pending if task is acquire_task}
                            await self._cancel_and_drain(pending_to_cancel)

                            if acquire_task not in done:
                                break
                            if acquire_task.cancelled():
                                raise asyncio.CancelledError
                            acquired = acquire_task.result()
                        if not acquired:
                            break

                    try:
                        task = asyncio.create_task(
                            self._process_message(channel, queue_url, msg, callback),
                        )
                        active_message_tasks.add(task)
                        messages.remove(msg)

                        def _done_callback(t: asyncio.Task) -> None:
                            active_message_tasks.discard(t)
                            if self._semaphore:
                                self._semaphore.release()

                        task.add_done_callback(_done_callback)
                    except Exception:
                        if self._semaphore:
                            self._semaphore.release()
                        raise

            except asyncio.CancelledError:
                await self._reject_unprocessed(channel, queue_url, messages)
                raise
            except Exception:
                logger.exception("consuming.receive_error", extra={"channel": channel})
                await asyncio.sleep(1)
            finally:
                if (self._shutdown_event.is_set() or not self._paused_event.is_set()) and messages:
                    await self._reject_unprocessed(channel, queue_url, messages)

        await self._cancel_and_drain(active_message_tasks)
        logger.debug("consuming.stopped", extra={"channel": channel})

    async def _process_message(
        self,
        channel: str,
        queue_url: str,
        msg: Any,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        try:
            message = SqsReceivedMessage(self._server, channel, queue_url, msg)
        except Exception:
            logger.exception("message.creation_error", extra={"channel": channel})
            return

        try:
            await callback(message)
        except asyncio.CancelledError:
            if not message.is_acted_on:
                # Run reject in a detached task because this task is already cancelled
                # and awaiting reject() would raise CancelledError instantly.
                t = asyncio.create_task(message.reject())

                # Suppress exceptions from the detached reject task
                def _ignore_exception(task: asyncio.Task) -> None:
                    with contextlib.suppress(Exception):
                        task.result()

                t.add_done_callback(_ignore_exception)
            raise
        except Exception:
            logger.exception("message.callback_error", extra={"channel": channel})
            if not message.is_acted_on:
                try:
                    await message.nack()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("message.nack_error", extra={"channel": channel})

    async def pause(self) -> None:
        self._paused_event.clear()
        self._pause_requested_event.set()
        logger.info("subscriber.paused")

    async def resume(self) -> None:
        if self._shutdown_event.is_set():
            return
        if not self._active:
            self._active = True
        self._pause_requested_event.clear()
        if self._pause_wait_task and self._pause_wait_task.done():
            self._pause_wait_task = None
        self._paused_event.set()
        if self._main_task is None or self._main_task.done():
            self._start_consuming()
        logger.info("subscriber.resumed")

    async def close(self) -> None:
        logger.info("subscriber.closing")
        self._active = False
        self._shutdown_event.set()
        self._pause_requested_event.set()
        self._paused_event.set()

        wait_tasks: set[asyncio.Task[Any]] = set()
        if self._pause_wait_task and not self._pause_wait_task.done():
            wait_tasks.add(self._pause_wait_task)
        if self._shutdown_wait_task and not self._shutdown_wait_task.done():
            wait_tasks.add(self._shutdown_wait_task)
        await self._cancel_and_drain(wait_tasks)
        self._pause_wait_task = None
        self._shutdown_wait_task = None

        await self._cancel_and_drain(set(self._tasks))
        self._tasks.clear()

        if self._main_task:
            await self._cancel_and_drain({self._main_task})
            self._main_task = None

        if self in self._server._active_subscribers:
            self._server._active_subscribers.discard(self)
        logger.info("subscriber.closed")
