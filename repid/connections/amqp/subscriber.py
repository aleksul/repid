from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from functools import partial
from typing import TYPE_CHECKING, Any

from repid.connections._subscriber import SubscriberDispatcher, cancel_and_drain
from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.protocol import ManagedSession, ReceiverLink

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.limits import BackpressureResource

logger = logging.getLogger("repid.connections.amqp")


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
        links: list[ReceiverLink],
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        dispatcher: SubscriberDispatcher | None = None,
        paused_event: asyncio.Event | None = None,
        naming_strategy: Callable[[str], str],
    ) -> None:
        self._managed_session = managed_session
        self._links = links
        self._queues_to_callbacks = queues_to_callbacks
        self._dispatcher = dispatcher or SubscriberDispatcher()
        self._is_active = True
        self._paused_event = paused_event or asyncio.Event()
        self._paused_event.set()
        self._naming_strategy = naming_strategy
        self._stop_event = asyncio.Event()
        self._callback_tasks: set[asyncio.Task[None]] = set()

        # Start the background task that monitors the connection
        # Note: With ManagedSession, reconnection is handled automatically by ReceiverPool
        self._task = asyncio.create_task(self._run_forever())

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def task(self) -> asyncio.Task:
        return self._task

    def supports_native_flow_control(
        self,
        channel: str,
        resource: BackpressureResource,
    ) -> bool:
        worker_limit = self._dispatcher.native_message_limit()
        return (
            resource == "messages"
            and channel in self._queues_to_callbacks
            and self._dispatcher.native_message_limit(channel) is not None
            and (worker_limit is None or len(self._queues_to_callbacks) == 1)
        )

    async def pause(self) -> None:
        self._is_active = False
        self._paused_event.clear()

    async def resume(self) -> None:
        self._is_active = True
        self._paused_event.set()

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
        dispatcher: SubscriberDispatcher | None = None,
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
            links=[],
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
                await subscriber.close()
            except Exception:
                logger.exception("subscriber.create.cleanup.error")
            raise
        return subscriber

    async def _dispatch_message(
        self,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        message: AmqpReceivedMessage,
        link: ReceiverLink,
        delivery_id: int,
    ) -> None:
        try:
            lease = await self._dispatcher.reserve(message)
            if lease is not None:
                await self._dispatcher.run_admitted(lease, message, callback)
        except Exception as exc:
            logger.error("message.callback.error", exc_info=exc)
        finally:
            await link.release_delivery_credit(delivery_id)

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
        await self._paused_event.wait()
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
        task = asyncio.create_task(
            self._dispatch_message(callback, message, link_ref, delivery_id),
        )
        self._callback_tasks.add(task)
        task.add_done_callback(self._callback_tasks.discard)

    async def _subscribe(self, *, publish_fn: Callable[..., Coroutine[Any, Any, None]]) -> None:
        receiver_pool = self._managed_session.receiver_pool
        for queue, callback in self._queues_to_callbacks.items():
            address = self._naming_strategy(queue)
            native_limit = self._dispatcher.native_message_limit(queue)
            # A zero cap still needs one delivery so the intake gate can apply
            # the configured oversized payload action to it.
            prefetch = 100 if native_limit is None else max(native_limit, 1)
            link = await receiver_pool.subscribe(
                address,
                partial(self._process_message, queue, callback, publish_fn),
                f"receiver-{queue}",
                prefetch=prefetch,
            )
            self._links.append(link)

    async def close(self) -> None:
        """Close the subscriber and release resources."""
        self._stop_event.set()
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        # Unsubscribe from all queues
        receiver_pool = self._managed_session.receiver_pool
        for queue in self._queues_to_callbacks:
            address = self._naming_strategy(queue)
            await receiver_pool.unsubscribe(address)
        await cancel_and_drain(self._callback_tasks)
