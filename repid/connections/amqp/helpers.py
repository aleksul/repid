from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from repid.connections.abc import MessageAction
from repid.connections.amqp._uamqp.message import Properties
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
        properties: Properties | None = None,
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._link = link
        self._delivery_id = delivery_id
        self._delivery_tag = delivery_tag
        self._channel_name = channel_name
        self._managed_session = managed_session
        self._publish_fn = publish_fn
        self._properties = properties
        self._action: MessageAction | None = None

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
        if self._properties is None:
            return None
        content_type = self._properties.content_type
        if content_type is None:
            return None
        if isinstance(content_type, bytes):
            return content_type.decode()
        return str(content_type)

    @property
    def reply_to(self) -> str | None:
        if self._properties is None or self._properties.reply_to is None:
            return None
        reply_to = self._properties.reply_to
        if isinstance(reply_to, bytes):
            return reply_to.decode()
        return str(reply_to)

    @property
    def channel(self) -> str:
        return self._channel_name

    @property
    def is_acted_on(self) -> bool:
        return self._action is not None

    @property
    def action(self) -> MessageAction | None:
        return self._action

    @property
    def message_id(self) -> str | None:
        if self._properties is None or self._properties.message_id is None:
            return None
        mid = self._properties.message_id
        if isinstance(mid, bytes):
            return mid.decode()
        return str(mid)

    async def _do_ack(self) -> None:
        """Send the AMQP accepted disposition on the wire (does not update `_action`)."""
        disp = DispositionFrame(
            role=True,
            first=self._delivery_id,
            last=self._delivery_id,
            settled=True,
            state=ACCEPTED_STATE,
        )
        await self._link.session.connection.send_performative(self._link.session.channel, disp)

    async def ack(self) -> None:
        if self._action is not None:
            return
        await self._do_ack()
        self._action = MessageAction.acked

    async def nack(self) -> None:
        if self._action is not None:
            return
        disp = DispositionFrame(
            role=True,
            first=self._delivery_id,
            last=self._delivery_id,
            settled=True,
            state=REJECTED_STATE,
        )
        await self._link.session.connection.send_performative(self._link.session.channel, disp)
        self._action = MessageAction.nacked

    async def reject(self) -> None:
        if self._action is not None:
            return
        disp = DispositionFrame(
            role=True,
            first=self._delivery_id,
            last=self._delivery_id,
            settled=True,
            state=RELEASED_STATE,
        )
        await self._link.session.connection.send_performative(self._link.session.channel, disp)
        self._action = MessageAction.rejected

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        if self._action is not None:
            return
        reply_channel = channel or self.reply_to
        if reply_channel is None:
            raise ValueError(
                "Reply channel is not set. Provide `channel` or publish with `reply_to`.",
            )

        await self._do_ack()
        self._action = MessageAction.replied
        params = dict(server_specific_parameters or {})

        await self._publish_fn(
            channel=reply_channel,
            message=MessageData(
                payload=payload,
                headers=headers,
                content_type=content_type,
            ),
            server_specific_parameters=params,
        )
