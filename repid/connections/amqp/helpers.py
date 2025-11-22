from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from repid.connections.amqp.protocol import DetachFrame, DispositionFrame, ReceiverLink
from repid.data import MessageData


@dataclass
class Accepted:
    _code = 0x00000024
    _definition = ()


ACCEPTED_STATE = Accepted()

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT

    from .message_broker import AmqpServer


class AmqpReceivedMessage:
    """Implementation of ReceivedMessageT for AmqpServer."""

    def __init__(
        self,
        *,
        payload: bytes,
        headers: dict[str, Any] | None,
        link: ReceiverLink,
        delivery_id: int,
        delivery_tag: bytes,
        channel_name: str,
        server: AmqpServer,
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._link = link
        self._delivery_id = delivery_id
        self._delivery_tag = delivery_tag
        self._channel_name = channel_name
        self._server = server
        self._is_acted_on = False

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def headers(self) -> dict[str, str] | None:
        if self._headers:
            result = {}
            for k, v in self._headers.items():
                key = k.decode() if isinstance(k, bytes) else str(k)
                value = v.decode() if isinstance(v, bytes) else str(v)
                result[key] = value
            return result
        return None

    @property
    def content_type(self) -> str | None:
        return None

    @property
    def channel(self) -> str:
        return self._channel_name

    @property
    def is_acted_on(self) -> bool:
        return self._is_acted_on

    @property
    def message_id(self) -> str | None:
        return None

    async def ack(self) -> None:
        if self._is_acted_on:
            return
        disp = DispositionFrame(
            role=True,
            first=self._delivery_id,
            last=self._delivery_id,
            settled=True,
            state=ACCEPTED_STATE,
        )
        await self._link.session.connection.send_performative(self._link.session.channel, disp)
        self._is_acted_on = True

    async def nack(self) -> None:
        if self._is_acted_on:
            return
        disp = DispositionFrame(
            role=True,
            first=self._delivery_id,
            last=self._delivery_id,
            settled=True,
            state=None,
        )
        await self._link.session.connection.send_performative(self._link.session.channel, disp)
        self._is_acted_on = True

    async def reject(self) -> None:
        if self._is_acted_on:
            return
        disp = DispositionFrame(
            role=True,
            first=self._delivery_id,
            last=self._delivery_id,
            settled=True,
            state=None,
        )
        await self._link.session.connection.send_performative(self._link.session.channel, disp)
        self._is_acted_on = True

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        await self.ack()
        reply_channel = channel or self._channel_name
        await self._server.publish(
            channel=reply_channel,
            message=MessageData(
                payload=payload,
                headers=headers,
                content_type=content_type,
            ),
            server_specific_parameters=server_specific_parameters,
        )


class AmqpSubscriber:
    """Implementation of SubscriberT for AmqpServer."""

    def __init__(
        self,
        *,
        server: AmqpServer,
        links: list[ReceiverLink],
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> None:
        self._server = server
        self._links = links
        self._queues_to_callbacks = queues_to_callbacks
        self._concurrency_limit = concurrency_limit
        self._is_active = True
        self._task = asyncio.create_task(self._run_forever())

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def task(self) -> asyncio.Task:
        return self._task

    async def pause(self) -> None:
        self._is_active = False

    async def resume(self) -> None:
        self._is_active = True

    async def _run_forever(self) -> None:
        try:
            while self._is_active:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass

    @classmethod
    async def create(
        cls,
        *,
        server: AmqpServer,
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> AmqpSubscriber:
        links = []
        if server._connection is None or server.session is None:
            raise ConnectionError("Server not connected")
        session = server.session

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

            link = await session.create_receiver_link(queue, f"receiver-{queue}", wrapped_callback)
            links.append(link)

        return cls(
            server=server,
            links=links,
            queues_to_callbacks=queues_to_callbacks,
            concurrency_limit=concurrency_limit,
        )

    async def close(self) -> None:
        for link in self._links:
            detach = DetachFrame(handle=link.handle, closed=True)
            await link.session.connection.send_performative(link.session.channel, detach)
