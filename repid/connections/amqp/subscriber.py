from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from repid.connections.amqp._uamqp.performatives import DetachFrame
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.protocol import ReceiverLink

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
    ) -> None:
        self._server = server
        self._links = links
        self._queues_to_callbacks = queues_to_callbacks
        self._concurrency_limit = concurrency_limit
        self._is_active = True
        self._paused_event = paused_event
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
        if self._server._connection is None or self._server._connection._incoming_task is None:
            return
        with contextlib.suppress(asyncio.CancelledError):
            await self._server._connection._incoming_task

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
        )

    async def close(self) -> None:
        for link in self._links:
            detach = DetachFrame(handle=link.handle, closed=True)
            await link.session.connection.send_performative(link.session.channel, detach)
