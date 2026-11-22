from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from functools import partial
from typing import TYPE_CHECKING
from uuid import uuid4

from repid.connections._submission import _SubmissionQueue
from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.protocol import ManagedSession, ReceiverLink

if TYPE_CHECKING:
    from repid.connections.abc import Intake
    from repid.connections.amqp.helpers import _ReplyPublisher
    from repid.connections.amqp.protocol.links import _ReceivedHeaders

logger = logging.getLogger("repid.connections.amqp")


def _cannot_settle(message: AmqpReceivedMessage) -> bool:
    return not message.can_settle


class AmqpSubscriber:
    """Hand deliveries to the worker from the AMQP frame reader.

    Link credit bounds outstanding deliveries per queue. Deliveries the worker
    has no room for yet, or that arrive while paused, wait in a submission queue.
    """

    __slots__ = (
        "_close_lock",
        "_managed_session",
        "_queues",
        "_resumed",
        "_stop_event",
        "_subscription_keys",
        "_task",
    )

    def __init__(self, managed_session: ManagedSession) -> None:
        self._managed_session = managed_session
        self._subscription_keys: list[str] = []
        self._queues: list[_SubmissionQueue[AmqpReceivedMessage]] = []
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
        for queue in self._queues:
            queue.pause()

    async def resume(self) -> None:
        if not self._stop_event.is_set():
            self._resumed.set()
            for queue in self._queues:
                queue.resume()

    @classmethod
    async def create(
        cls,
        *,
        managed_session: ManagedSession,
        intake: Intake,
        naming_strategy: Callable[[str], str],
        publish_fn: _ReplyPublisher,
        prefetch_by_queue: Mapping[str, int],
    ) -> AmqpSubscriber:
        subscriber = cls(managed_session)
        receiver_pool = managed_session.receiver_pool
        try:
            for queue, channel_intake in intake.channels.items():
                # A detached link's deliveries are requeued by the broker, so a
                # held one must not be processed after a reconnect.
                submissions: _SubmissionQueue[AmqpReceivedMessage] = _SubmissionQueue(
                    channel_intake,
                    keep_alive=False,
                    is_stale=_cannot_settle,
                )
                # Hold deliveries until the subscriber resumes below.
                submissions.pause()
                subscriber._queues.append(submissions)
                key = uuid4().hex
                await receiver_pool.subscribe(
                    naming_strategy(queue),
                    partial(subscriber._receive, submissions, queue, publish_fn),
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

    def _receive(  # noqa: PLR0917
        self,
        submissions: _SubmissionQueue[AmqpReceivedMessage],
        queue: str,
        publish_fn: _ReplyPublisher,
        payload: bytes,
        headers: _ReceivedHeaders | None,
        delivery_id: int,
        delivery_tag: bytes,
        link: ReceiverLink,
        properties: Properties | None = None,
    ) -> None:
        # Credit returns when the worker settles the delivery, not when this returns.
        link.defer_delivery_credit(delivery_id)
        if self._stop_event.is_set():
            return

        submissions.put(
            AmqpReceivedMessage(
                payload=payload,
                headers=headers,
                link=link,
                delivery_id=delivery_id,
                delivery_tag=delivery_tag,
                channel_name=queue,
                managed_session=self._managed_session,
                publish_fn=publish_fn,
                properties=properties,
            ),
        )

    async def close(self) -> None:
        async with self._close_lock:
            self._stop_event.set()
            self._resumed.clear()
            # Closing a link requeues its unsettled deliveries on the broker,
            # including those still held for submission.
            for queue in self._queues:
                queue.close()
            receiver_pool = self._managed_session.receiver_pool
            try:
                for key in self._subscription_keys:
                    await receiver_pool.unsubscribe(key)
                self._subscription_keys.clear()
            finally:
                await asyncio.gather(self._task, return_exceptions=True)
