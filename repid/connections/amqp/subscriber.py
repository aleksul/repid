from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from repid.connections.amqp._uamqp.performatives import DetachFrame
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.protocol import ReceiverLink
from repid.connections.amqp.protocol.events import ConnectionEvent

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT

    from .message_broker import AmqpServer


class AmqpSubscriber:
    """Implementation of SubscriberT for AmqpServer."""

    def __init__(
        self,
        *,
        server: AmqpServer,
        links: list[ReceiverLink],
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
        paused_event: asyncio.Event,
        naming_strategy: Callable[[str], str],
    ) -> None:
        self._server = server
        self._links = links
        self._queues_to_callbacks = queues_to_callbacks
        self._concurrency_limit = concurrency_limit
        self._is_active = True
        self._paused_event = paused_event
        self._naming_strategy = naming_strategy
        self._stop_event = asyncio.Event()
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
        """Run forever, handling reconnections."""
        while not self._stop_event.is_set():
            if self._server._connection is None:
                # Wait a bit and check again
                await asyncio.sleep(0.1)
                continue

            # Register reconnect handler
            reconnected_event = asyncio.Event()

            async def on_reconnect(_: Any = None, evt: asyncio.Event = reconnected_event) -> None:
                evt.set()

            self._server._connection.events.on(ConnectionEvent.RECONNECTED, on_reconnect)

            try:
                # Wait for either incoming task to complete or reconnection
                incoming_task = self._server._connection._incoming_task
                if incoming_task is None:
                    await asyncio.sleep(0.1)
                    continue

                _, pending = await asyncio.wait(
                    [
                        asyncio.ensure_future(incoming_task),
                        asyncio.create_task(reconnected_event.wait()),
                        asyncio.create_task(self._stop_event.wait()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

                # If stopped, exit
                if self._stop_event.is_set():
                    break

                # If reconnected, recreate links
                if reconnected_event.is_set():
                    await self._recreate_links()

            except asyncio.CancelledError:
                break
            finally:
                self._server._connection.events.off(ConnectionEvent.RECONNECTED, on_reconnect)

    async def _recreate_links(self) -> None:
        """Recreate receiver links after reconnection."""
        # Ensure we have a valid session
        session = await self._server._ensure_session()

        self._links.clear()

        for queue, callback in self._queues_to_callbacks.items():

            async def wrapped_callback(
                payload: bytes,
                headers: dict[str, Any] | None,
                delivery_id: int,
                delivery_tag: bytes,
                link_ref: ReceiverLink,
                queue: str = queue,
                callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]] = callback,
            ) -> None:
                await self._paused_event.wait()
                msg = AmqpReceivedMessage(
                    payload=payload,
                    headers=headers,
                    link=link_ref,
                    delivery_id=delivery_id,
                    delivery_tag=delivery_tag,
                    channel_name=queue,
                    server=self._server,
                )
                await callback(msg)

            link = await session.create_receiver_link(
                self._naming_strategy(queue),
                f"receiver-{queue}",
                wrapped_callback,
            )
            self._links.append(link)

    @classmethod
    async def create(
        cls,
        *,
        server: AmqpServer,
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
        naming_strategy: Callable[[str], str],
    ) -> AmqpSubscriber:
        links = []
        if server._connection is None or server.session is None:
            raise ConnectionError("Server not connected")
        session = server.session

        paused_event = asyncio.Event()
        paused_event.set()

        for queue, callback in queues_to_callbacks.items():

            async def wrapped_callback(
                payload: bytes,
                headers: dict[str, Any] | None,
                delivery_id: int,
                delivery_tag: bytes,
                link_ref: ReceiverLink,
                queue: str = queue,
                callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]] = callback,
            ) -> None:
                await paused_event.wait()
                msg = AmqpReceivedMessage(
                    payload=payload,
                    headers=headers,
                    link=link_ref,
                    delivery_id=delivery_id,
                    delivery_tag=delivery_tag,
                    channel_name=queue,
                    server=server,
                )
                await callback(msg)

            link = await session.create_receiver_link(
                naming_strategy(queue),
                f"receiver-{queue}",
                wrapped_callback,
            )
            links.append(link)

        return cls(
            server=server,
            links=links,
            queues_to_callbacks=queues_to_callbacks,
            concurrency_limit=concurrency_limit,
            paused_event=paused_event,
            naming_strategy=naming_strategy,
        )

    async def close(self) -> None:
        self._stop_event.set()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        for link in self._links:
            detach = DetachFrame(handle=link.handle, closed=True)
            with contextlib.suppress(Exception):
                await link.session.connection.send_performative(link.session.channel, detach)
