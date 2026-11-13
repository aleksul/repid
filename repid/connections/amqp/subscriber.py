from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine, Mapping
from functools import partial
from typing import Any
from uuid import uuid4

from repid.connections.abc import MessageCallbackT
from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.protocol import ManagedSession, ReceiverLink

logger = logging.getLogger("repid.connections.amqp")


class AmqpSubscriber:
    """Submit deliveries independently of the AMQP frame reader.

    Link credit bounds outstanding deliveries per queue. The runner controls
    actor execution through its admission controller.
    """

    __slots__ = (
        "_close_lock",
        "_managed_session",
        "_resumed",
        "_stop_event",
        "_submission_tasks",
        "_subscription_keys",
        "_task",
    )

    def __init__(self, managed_session: ManagedSession) -> None:
        self._managed_session = managed_session
        self._subscription_keys: list[str] = []
        self._submission_tasks: set[asyncio.Task[None]] = set()
        self._resumed = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._close_lock = asyncio.Lock()
        self._task = asyncio.create_task(self._stop_event.wait())

    @property
    def is_active(self) -> bool:
        return self._resumed.is_set() and not self._stop_event.is_set()

    @property
    def task(self) -> asyncio.Task[bool]:
        return self._task

    async def pause(self) -> None:
        self._resumed.clear()

    async def resume(self) -> None:
        if not self._stop_event.is_set():
            self._resumed.set()

    @classmethod
    async def create(
        cls,
        *,
        managed_session: ManagedSession,
        queues_to_callbacks: dict[str, MessageCallbackT],
        naming_strategy: Callable[[str], str],
        publish_fn: Callable[..., Coroutine[Any, Any, None]],
        prefetch_by_queue: Mapping[str, int],
    ) -> AmqpSubscriber:
        subscriber = cls(managed_session)
        receiver_pool = managed_session.receiver_pool
        try:
            for queue, callback in queues_to_callbacks.items():
                key = uuid4().hex
                await receiver_pool.subscribe(
                    naming_strategy(queue),
                    partial(subscriber._receive, callback, queue, publish_fn),
                    f"receiver-{queue}",
                    prefetch=prefetch_by_queue[queue],
                    subscription_key=key,
                )
                subscriber._subscription_keys.append(key)
        except BaseException:
            try:
                await subscriber.close()
            except Exception:
                logger.exception("subscriber.create.cleanup_error")
            raise

        await subscriber.resume()
        return subscriber

    async def _receive(  # noqa: PLR0917
        self,
        callback: MessageCallbackT,
        queue: str,
        publish_fn: Callable[..., Coroutine[Any, Any, None]],
        payload: bytes,
        headers: dict[str, Any] | None,
        delivery_id: int,
        delivery_tag: bytes,
        link: ReceiverLink,
        properties: Properties | None = None,
    ) -> None:
        link.defer_delivery_credit(delivery_id)
        if self._stop_event.is_set():
            return

        message = AmqpReceivedMessage(
            payload=payload,
            headers=headers,
            link=link,
            delivery_id=delivery_id,
            delivery_tag=delivery_tag,
            channel_name=queue,
            managed_session=self._managed_session,
            publish_fn=publish_fn,
            properties=properties,
        )
        task = asyncio.create_task(self._submit(callback, message))
        self._submission_tasks.add(task)
        task.add_done_callback(self._submission_done)
        link.track_delivery_task(task)

    async def _submit(self, callback: MessageCallbackT, message: AmqpReceivedMessage) -> None:
        try:
            while not self._resumed.is_set():
                await self._resumed.wait()
            await callback(message)
        except asyncio.CancelledError:
            await message.reject()
            raise
        except Exception:
            logger.exception("subscriber.callback.error")
            await message.reject()

    def _submission_done(self, task: asyncio.Task[None]) -> None:
        self._submission_tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("subscriber.callback.rejection_error", exc_info=exc)

    async def close(self) -> None:
        async with self._close_lock:
            self._stop_event.set()
            self._resumed.clear()
            # Closing a link requeues its unsettled deliveries on the broker.
            # Detach before canceling tasks to avoid returning live link credit.
            receiver_pool = self._managed_session.receiver_pool
            try:
                for key in self._subscription_keys:
                    await receiver_pool.unsubscribe(key)
                self._subscription_keys.clear()
            finally:
                current_task = asyncio.current_task()
                tasks = self._submission_tasks - {current_task}
                for task in tasks:
                    if not task.cancelling():
                        task.cancel()
                await asyncio.gather(self._task, *tasks, return_exceptions=True)
