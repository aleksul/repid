from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from functools import partial
from typing import TYPE_CHECKING, Any

from repid.connections._subscriber import AdmittedTaskTracker, SubscriberDispatcher
from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.protocol import ManagedSession, ReceiverLink

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT

logger = logging.getLogger("repid.connections.amqp")


def _resumed_event() -> asyncio.Event:
    event = asyncio.Event()
    event.set()
    return event


class AmqpSubscriber:
    """
    Implementation of SubscriberT for AmqpServer.

    This subscriber uses ManagedSession's ReceiverPool, which automatically
    handles reconnection and link recreation.

    The intake gate's native message limit is forwarded as the AMQP link-credit
    (prefetch) so the broker limits in-flight deliveries at the protocol level.
    """

    def __init__(
        self,
        *,
        managed_session: ManagedSession,
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        dispatcher: SubscriberDispatcher,
        paused_event: asyncio.Event | None = None,
        naming_strategy: Callable[[str], str],
    ) -> None:
        self._managed_session = managed_session
        self._queues_to_callbacks = queues_to_callbacks
        self._dispatcher = dispatcher
        self._is_active = True
        self._paused_event = paused_event or asyncio.Event()
        self._paused_event.set()
        self._queue_paused_events = {queue: _resumed_event() for queue in queues_to_callbacks}
        self._naming_strategy = naming_strategy
        self._stop_event = asyncio.Event()
        self._admitted_tasks = AdmittedTaskTracker()
        self._stopped = False
        self._unsubscribed = False

        # Start the background task that monitors the connection
        # Note: With ManagedSession, reconnection is handled automatically by ReceiverPool
        self._task = asyncio.create_task(self._run_forever())

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def task(self) -> asyncio.Task:
        return self._task

    async def pause(self) -> None:
        self._is_active = False
        self._paused_event.clear()
        for event in self._queue_paused_events.values():
            event.clear()

    async def resume(self) -> None:
        self._is_active = True
        self._paused_event.set()
        for event in self._queue_paused_events.values():
            event.set()

    async def pause_channel(self, channel: str) -> None:
        event = self._queue_paused_events.get(channel)
        if event is not None:
            event.clear()

    async def resume_channel(self, channel: str) -> None:
        event = self._queue_paused_events.get(channel)
        if event is not None:
            event.set()

    async def _run_forever(self) -> None:
        """
        Run until the subscriber receives its stop signal.

        Reconnection handling is done by ManagedSession's ReceiverPool; this
        task just waits for shutdown.
        """
        await self._stop_event.wait()

    @classmethod
    async def create(
        cls,
        *,
        managed_session: ManagedSession,
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        dispatcher: SubscriberDispatcher,
        naming_strategy: Callable[[str], str],
        publish_fn: Callable[..., Coroutine[Any, Any, None]],
    ) -> AmqpSubscriber:
        """
        Create a new subscriber.

        Args:
            managed_session: The managed session to use
            queues_to_callbacks: Mapping of queue names to callback functions
            dispatcher: Intake accounting and callback dispatch. Its native
                message limit is also used as the AMQP link-credit (prefetch)
                so the broker limits in-flight deliveries at the protocol level.
            naming_strategy: Function to convert queue names to AMQP addresses

        Returns:
            A new AmqpSubscriber instance
        """
        subscriber = cls(
            managed_session=managed_session,
            queues_to_callbacks=queues_to_callbacks,
            dispatcher=dispatcher,
            naming_strategy=naming_strategy,
        )
        try:
            await subscriber._subscribe(publish_fn=publish_fn)
        except BaseException:
            # ``__init__`` starts the monitor task before link creation. Roll
            # it back, along with any links created before the failed one, so
            # a failed subscription attempt does not leave a live subscriber.
            try:
                await subscriber.stop()
                await subscriber.finish()
            except Exception:
                logger.exception("subscriber.create.cleanup.error")
            raise
        return subscriber

    async def _reject_unacted_message(self, message: AmqpReceivedMessage) -> None:
        if not message.is_acted_on:
            try:
                await message.reject()
            except Exception as exc:
                logger.exception("message.reject.error", exc_info=exc)

    async def _reject_unacted_delivery(
        self,
        message: AmqpReceivedMessage,
        link: ReceiverLink,
        delivery_id: int,
    ) -> None:
        await self._reject_unacted_message(message)
        try:
            await link.release_delivery_credit(delivery_id)
        except Exception as exc:
            logger.exception("message.credit.release.error", exc_info=exc)

    async def _dispatch_message(
        self,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        message: AmqpReceivedMessage,
        link: ReceiverLink,
        delivery_id: int,
    ) -> None:
        admitted = False
        try:
            await self._paused_event.wait()
            queue_event = self._queue_paused_events.get(message.channel)
            if queue_event is not None:
                await queue_event.wait()
            lease = await self._dispatcher.reserve(message)
            if lease is not None:
                admitted = True
                await self._dispatcher.run_admitted(lease, message, callback)
        except asyncio.CancelledError:
            await self._reject_unacted_message(message)
            raise
        except Exception as exc:
            logger.error(
                "message.callback.error" if admitted else "message.admission.error",
                exc_info=exc,
            )
            if not message.is_acted_on:
                try:
                    await (message.nack() if admitted else message.reject())
                except Exception as cleanup_exc:
                    logger.exception("message.disposition.error", exc_info=cleanup_exc)
        finally:
            try:
                await link.release_delivery_credit(delivery_id)
            except Exception as exc:
                logger.exception("message.credit.release.error", exc_info=exc)

    async def _process_message(  # noqa: PLR0917
        self,
        queue: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        publish_fn: Callable[..., Coroutine[Any, Any, None]],
        payload: bytes,
        headers: dict[str, Any] | None,
        delivery_id: int,
        delivery_tag: bytes,
        link_ref: ReceiverLink,
        properties: Properties | None = None,
    ) -> None:
        if self._stop_event.is_set():
            return
        link_ref.defer_delivery_credit(delivery_id)
        message = AmqpReceivedMessage(
            payload=payload,
            headers=headers,
            link=link_ref,
            delivery_id=delivery_id,
            delivery_tag=delivery_tag,
            channel_name=queue,
            managed_session=self._managed_session,
            publish_fn=publish_fn,
            properties=properties,
        )
        self._admitted_tasks.start_task(
            lambda: self._dispatch_message(callback, message, link_ref, delivery_id),
            on_cancel=partial(self._reject_unacted_delivery, message, link_ref, delivery_id),
            message=message,
        )

    async def _subscribe(self, *, publish_fn: Callable[..., Coroutine[Any, Any, None]]) -> None:
        receiver_pool = self._managed_session.receiver_pool
        for queue, callback in self._queues_to_callbacks.items():
            address = self._naming_strategy(queue)
            native_limit = self._dispatcher.native_message_limit(queue)
            prefetch = 100 if native_limit is None else native_limit
            await receiver_pool.subscribe(
                address,
                partial(self._process_message, queue, callback, publish_fn),
                f"receiver-{queue}",
                prefetch=prefetch,
            )

    async def stop(self) -> None:
        """Stop intake: cancel the consume loop without touching callbacks."""
        if self._stopped:
            return
        self._stopped = True
        self._is_active = False
        self._paused_event.clear()
        self._stop_event.set()
        self._task.cancel()

    async def finish(self) -> None:
        """Cancel remaining callbacks, drain their cleanup, unsubscribe."""
        await self.stop()
        await self._admitted_tasks.cancel_and_drain()
        if asyncio.current_task() is not self._task:
            await asyncio.gather(self._task, return_exceptions=True)
        if self._unsubscribed:
            return
        self._unsubscribed = True

        unsubscribe_error: BaseException | None = None
        receiver_pool = self._managed_session.receiver_pool
        for queue in self._queues_to_callbacks:
            try:
                await receiver_pool.unsubscribe(self._naming_strategy(queue))
            except BaseException as exc:
                if unsubscribe_error is None:
                    unsubscribe_error = exc
                else:
                    logger.exception("subscriber.close.unsubscribe.error", exc_info=exc)

        if unsubscribe_error is not None:
            raise unsubscribe_error
