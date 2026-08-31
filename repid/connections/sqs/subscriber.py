from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from functools import partial
from typing import TYPE_CHECKING, Any

from repid.connections._subscriber import (
    AdmittedTaskTracker,
    SubscriberDispatcher,
    cancel_and_drain,
)
from repid.connections.abc import ReceivedMessageT
from repid.connections.sqs.message import SqsReceivedMessage

if TYPE_CHECKING:
    from repid.connections.sqs.message_broker import SqsServer

logger = logging.getLogger("repid.connections.sqs")


def _resumed_event() -> asyncio.Event:
    event = asyncio.Event()
    event.set()
    return event


def _cleared_event() -> asyncio.Event:
    return asyncio.Event()


class SqsSubscriber:
    def __init__(
        self,
        server: SqsServer,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        dispatcher: SubscriberDispatcher,
    ) -> None:
        self._server = server
        self._channels_to_callbacks = channels_to_callbacks
        self._dispatcher = dispatcher
        self._active = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()
        self._pause_requested_event = asyncio.Event()
        self._channel_paused_events = {
            channel: _resumed_event() for channel in channels_to_callbacks
        }
        self._channel_pause_requested_events = {
            channel: _cleared_event() for channel in channels_to_callbacks
        }
        self._shutdown_event = asyncio.Event()
        self._shutdown_wait_task: asyncio.Task[bool] | None = None
        self._pause_wait_task: asyncio.Task[bool] | None = None
        self._channel_pause_wait_tasks: dict[str, asyncio.Task[bool] | None] = {}
        self._tasks: list[asyncio.Task] = []
        self._admitted_tasks = AdmittedTaskTracker()
        self._requeue_tasks: set[asyncio.Task[None]] = set()
        self._main_task: asyncio.Task | None = None
        self._close_lock = asyncio.Lock()
        self._close_started = False

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
            await cancel_and_drain(set(self._tasks))
            self._tasks.clear()
            # Keep the main task (and therefore server ownership) alive until
            # channel cancellation has finished scheduling every requeue.
            await self._drain_requeues()

    def _reject_unprocessed(self, messages: list[SqsReceivedMessage]) -> None:
        """Schedule owned requeue cleanup for fetched, unadmitted deliveries."""
        for message in messages:
            task = asyncio.create_task(self._stop_keep_alive_and_reject(message))
            self._requeue_tasks.add(task)
            task.add_done_callback(self._requeue_done)

    async def _stop_keep_alive_and_reject(self, message: SqsReceivedMessage) -> None:
        await self._dispatcher.stop_keep_alive(message)
        await message.reject()

    def _requeue_done(self, task: asyncio.Task[None]) -> None:
        self._requeue_tasks.discard(task)
        if task.cancelled():
            return
        if (exc := task.exception()) is not None:
            logger.exception("message.reject_unprocessed_error", exc_info=exc)

    async def _drain_requeues(self) -> None:
        while self._requeue_tasks:
            await asyncio.gather(*tuple(self._requeue_tasks), return_exceptions=True)

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

    def _get_channel_pause_wait_task(self, channel: str) -> asyncio.Task[bool]:
        task = self._channel_pause_wait_tasks.get(channel)
        requested = self._channel_pause_requested_events[channel]
        if task is None or (task.done() and not requested.is_set()):
            task = asyncio.create_task(requested.wait())
            self._channel_pause_wait_tasks[channel] = task
        return task

    async def _consume_channel(self, channel: str) -> None:  # noqa: C901, PLR0912, PLR0915
        logger.debug("consuming.started", extra={"channel": channel})
        callback = self._channels_to_callbacks[channel]

        client = self._server._client
        if client is None:
            raise RuntimeError("SQS client is not connected.")

        queue_url = None
        channel_paused = self._channel_paused_events[channel]
        native_limit = self._dispatcher.native_message_limit(channel)
        max_messages = (
            self._server._batch_size
            if native_limit is None
            else min(self._server._batch_size, native_limit)
        )

        while self._active and not self._shutdown_event.is_set():
            await self._paused_event.wait()
            await channel_paused.wait()
            if self._shutdown_event.is_set():
                break

            messages: list[SqsReceivedMessage] = []
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
                channel_pause_task = self._get_channel_pause_wait_task(channel)

                try:
                    done, _ = await asyncio.wait(
                        {receive_task, shutdown_task, pause_task, channel_pause_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    if not receive_task.done():
                        await cancel_and_drain((receive_task,))

                if receive_task not in done:
                    continue

                response = await receive_task

                messages = []
                for raw_message in response.get("Messages", []):
                    try:
                        messages.append(
                            SqsReceivedMessage(
                                self._server,
                                channel,
                                queue_url,
                                raw_message,
                                self._server._visibility_timeout,
                            ),
                        )
                    except Exception as exc:
                        logger.exception(
                            "message.construct_error",
                            extra={"channel": channel},
                            exc_info=exc,
                        )
                for message in messages:
                    self._dispatcher.start_keep_alive(message)
                if not messages and self._server._receive_wait_time_seconds <= 0:
                    await asyncio.sleep(0.1)
                    continue
                for message in messages.copy():
                    if (
                        self._shutdown_event.is_set()
                        or not self._paused_event.is_set()
                        or not channel_paused.is_set()
                    ):
                        break

                    lease = await self._dispatcher.reserve(message)
                    messages.remove(message)
                    if lease is not None:
                        self._admitted_tasks.start(
                            self._dispatcher,
                            lease,
                            message,
                            partial(self._process_message, channel, callback=callback),
                            on_cancel=message.reject,
                        )

            except asyncio.CancelledError:
                self._reject_unprocessed(messages)
                messages.clear()
                raise
            except Exception:
                logger.exception("consuming.receive_error", extra={"channel": channel})
                self._reject_unprocessed(messages)
                messages.clear()
                await asyncio.sleep(1)
            finally:
                if (
                    self._shutdown_event.is_set()
                    or not self._paused_event.is_set()
                    or not channel_paused.is_set()
                ) and messages:
                    self._reject_unprocessed(messages)
                    messages.clear()

        logger.debug("consuming.stopped", extra={"channel": channel})

    async def _process_message(
        self,
        channel: str,
        message: SqsReceivedMessage,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        try:
            await callback(message)
        except asyncio.CancelledError:
            if not message.is_acted_on:
                with contextlib.suppress(Exception):
                    await message.reject()
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
        for event in self._channel_paused_events.values():
            event.clear()
        for event in self._channel_pause_requested_events.values():
            event.set()
        logger.info("subscriber.paused")

    async def resume(self) -> None:
        if self._shutdown_event.is_set():
            return
        if not self._active:
            self._active = True
        self._pause_requested_event.clear()
        for event in self._channel_pause_requested_events.values():
            event.clear()
        for event in self._channel_paused_events.values():
            event.set()
        if self._pause_wait_task and self._pause_wait_task.done():
            self._pause_wait_task = None
        for channel, task in self._channel_pause_wait_tasks.items():
            if task is not None and task.done():
                self._channel_pause_wait_tasks[channel] = None
        self._paused_event.set()
        if self._main_task is None or self._main_task.done():
            self._start_consuming()
        logger.info("subscriber.resumed")

    async def pause_channel(self, channel: str) -> None:
        paused = self._channel_paused_events.get(channel)
        requested = self._channel_pause_requested_events.get(channel)
        if paused is None or requested is None:
            return
        paused.clear()
        requested.set()
        logger.info("subscriber.channel_paused", extra={"channel": channel})

    async def resume_channel(self, channel: str) -> None:
        paused = self._channel_paused_events.get(channel)
        requested = self._channel_pause_requested_events.get(channel)
        if paused is None or requested is None:
            return
        requested.clear()
        paused.set()
        if self._main_task is None or self._main_task.done():
            self._start_consuming()
        logger.info("subscriber.channel_resumed", extra={"channel": channel})

    async def stop(self) -> None:
        """Stop intake: cancel fetch/channel tasks, retaining their requeue cleanup."""
        async with self._close_lock:
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
            for channel_wait_task in self._channel_pause_wait_tasks.values():
                if channel_wait_task is not None and not channel_wait_task.done():
                    wait_tasks.add(channel_wait_task)
            channel_tasks = set(self._tasks)
            main_task = self._main_task
            owned_tasks: set[asyncio.Task[Any]] = wait_tasks | channel_tasks
            if main_task is not None:
                owned_tasks.add(main_task)
            # Deliver cancellation to intake tasks only once. A later `finish()`
            # must not interrupt a task already requeueing an owned message.
            if not self._close_started:
                self._close_started = True
                await cancel_and_drain(owned_tasks, drain=False)
            # Cancelled main/channel tasks can still schedule requeues in
            # cancellation cleanup. Retain their ownership for a later drain.
            logger.info("subscriber.closed")

    async def finish(self) -> None:
        """Cancel remaining callbacks, drain retained cleanup, release resources."""
        await self.stop()
        # Cancel whatever callbacks are still running (exactly once), then
        # wait for them and for cleanup retained from `stop()`. Never wait
        # under the close lock: a blocked callback must not keep a concurrent
        # `stop()`/`finish()` from proceeding.
        await self._admitted_tasks.cancel_and_drain()
        await self._drain_and_release()

    async def settle(self) -> None:
        """Drain retained cleanup without cancelling; release server registration.

        Background janitor for a consume loop that ended on its own: it waits
        for already-running callbacks and retained cleanup, but never cancels
        — cancellation belongs to the owner (the runner or `finish()`). The
        waits happen without the close lock so `stop()`/`finish()` stay free
        to cancel those callbacks while the janitor waits.
        """
        await self._admitted_tasks.drain()
        await self._drain_and_release()

    def _owned_tasks(self) -> set[asyncio.Task[Any]]:
        wait_tasks: set[asyncio.Task[Any]] = set()
        if self._pause_wait_task and not self._pause_wait_task.done():
            wait_tasks.add(self._pause_wait_task)
        if self._shutdown_wait_task and not self._shutdown_wait_task.done():
            wait_tasks.add(self._shutdown_wait_task)
        owned_tasks: set[asyncio.Task[Any]] = wait_tasks | set(self._tasks)
        if self._main_task is not None:
            owned_tasks.add(self._main_task)
        return owned_tasks

    async def _drain_and_release(self) -> None:
        # Cancel idle event waits first: they may wait for events that are
        # never set once intake has ended, so neither waiting on them nor
        # holding the close lock across such a wait may block release.
        idle_wait_tasks: set[asyncio.Task[Any]] = set()
        for idle_task in (
            self._pause_wait_task,
            self._shutdown_wait_task,
            *self._channel_pause_wait_tasks.values(),
        ):
            if idle_task is not None and not idle_task.done():
                idle_wait_tasks.add(idle_task)
        for idle_task in idle_wait_tasks:
            idle_task.cancel()
        if idle_wait_tasks:
            await asyncio.gather(*idle_wait_tasks, return_exceptions=True)

        owned_tasks = self._owned_tasks()
        if owned_tasks:
            await asyncio.gather(*owned_tasks, return_exceptions=True)

        await self._drain_requeues()
        async with self._close_lock:
            main_task = self._main_task
            self._pause_wait_task = None
            self._shutdown_wait_task = None
            self._channel_pause_wait_tasks = dict.fromkeys(
                self._channel_pause_wait_tasks,
                None,
            )
            self._tasks = [task for task in self._tasks if not task.done()]
            if self._main_task is main_task and (main_task is None or main_task.done()):
                self._main_task = None
            self._server._active_subscribers.discard(self)
            logger.info("subscriber.closed")
