from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from repid.connections.amqp._uamqp.performatives import DetachFrame
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.protocol import ManagedSession, ReceiverLink

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT


class AmqpSubscriber:
    """
    Implementation of SubscriberT for AmqpServer.

    This subscriber uses ManagedSession's ReceiverPool, which automatically
    handles reconnection and link recreation.
    """

    def __init__(
        self,
        *,
        managed_session: ManagedSession,
        links: list[ReceiverLink],
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
        paused_event: asyncio.Event,
        naming_strategy: Callable[[str], str],
    ) -> None:
        self._managed_session = managed_session
        self._links = links
        self._queues_to_callbacks = queues_to_callbacks
        self._concurrency_limit = concurrency_limit
        self._is_active = True
        self._paused_event = paused_event
        self._naming_strategy = naming_strategy
        self._stop_event = asyncio.Event()

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

    async def resume(self) -> None:
        self._is_active = True
        self._paused_event.set()

    async def _run_forever(self) -> None:
        """
        Run forever, waiting for stop signal.

        Unlike the previous implementation, reconnection handling is now done
        by the ManagedSession's ReceiverPool. This task just waits for the stop signal.
        """
        with contextlib.suppress(asyncio.CancelledError):
            await self._stop_event.wait()

    @classmethod
    async def create(
        cls,
        *,
        managed_session: ManagedSession,
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
        naming_strategy: Callable[[str], str],
        publish_fn: Callable[..., Coroutine[Any, Any, None]],
    ) -> AmqpSubscriber:
        """
        Create a new subscriber.

        Args:
            managed_session: The managed session to use
            queues_to_callbacks: Mapping of queue names to callback functions
            concurrency_limit: Optional concurrency limit (reserved for future use)
            naming_strategy: Function to convert queue names to AMQP addresses

        Returns:
            A new AmqpSubscriber instance
        """
        links: list[ReceiverLink] = []
        receiver_pool = managed_session.receiver_pool

        paused_event = asyncio.Event()
        paused_event.set()

        for queue, callback in queues_to_callbacks.items():
            # Create wrapper callback that handles the message
            async def wrapped_callback(
                payload: bytes,
                headers: dict[str, Any] | None,
                delivery_id: int,
                delivery_tag: bytes,
                link_ref: ReceiverLink,
                queue: str = queue,
                callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]] = callback,
                paused_event: asyncio.Event = paused_event,
                managed_session: ManagedSession = managed_session,
            ) -> None:
                await paused_event.wait()
                msg = AmqpReceivedMessage(
                    payload=payload,
                    headers=headers,
                    link=link_ref,
                    delivery_id=delivery_id,
                    delivery_tag=delivery_tag,
                    channel_name=queue,
                    managed_session=managed_session,
                    publish_fn=publish_fn,
                )
                await callback(msg)

            # Subscribe using the receiver pool (handles reconnection automatically)
            address = naming_strategy(queue)
            link = await receiver_pool.subscribe(
                address,
                wrapped_callback,
                f"receiver-{queue}",
            )
            links.append(link)

        return cls(
            managed_session=managed_session,
            links=links,
            queues_to_callbacks=queues_to_callbacks,
            concurrency_limit=concurrency_limit,
            paused_event=paused_event,
            naming_strategy=naming_strategy,
        )

    async def close(self) -> None:
        """Close the subscriber and release resources."""
        self._stop_event.set()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

        # Unsubscribe from all queues
        receiver_pool = self._managed_session.receiver_pool
        for queue in self._queues_to_callbacks:
            address = self._naming_strategy(queue)
            await receiver_pool.unsubscribe(address)

        # Also try to detach links directly (graceful shutdown)
        for link in self._links:
            detach = DetachFrame(handle=link.handle, closed=True)
            with contextlib.suppress(Exception):
                session = await self._managed_session.get_session()
                await session.connection.send_performative(session.channel, detach)
