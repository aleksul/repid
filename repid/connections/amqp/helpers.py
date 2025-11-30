from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from repid.connections.amqp._uamqp.outcomes import Accepted, Rejected, Released
from repid.connections.amqp._uamqp.performatives import DispositionFrame
from repid.connections.amqp.protocol import ManagedSession, ReceiverLink
from repid.data import MessageData

ACCEPTED_STATE = Accepted()
REJECTED_STATE = Rejected()
RELEASED_STATE = Released()


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
        managed_session: ManagedSession,
        publish_fn: Callable[..., Coroutine[Any, Any, None]],
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._link = link
        self._delivery_id = delivery_id
        self._delivery_tag = delivery_tag
        self._channel_name = channel_name
        self._managed_session = managed_session
        self._publish_fn = publish_fn
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
            state=REJECTED_STATE,
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
            state=RELEASED_STATE,
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

        await self._publish_fn(
            channel=reply_channel,
            message=MessageData(
                payload=payload,
                headers=headers,
                content_type=content_type,
            ),
            server_specific_parameters=server_specific_parameters,
        )
