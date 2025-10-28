from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any

from repid.data import MessageData

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT, ServerT
    from repid.dependencies._utils import DependencyContext
    from repid.serializer import SerializerT


class EnhancedReceivedMessage:
    def __init__(
        self,
        server: ServerT,
        message: ReceivedMessageT,
        default_serializer: SerializerT,
    ) -> None:
        self._server = server
        self._message = message
        self._default_serializer = default_serializer

    @property
    def payload(self) -> bytes:
        return self._message.payload

    @property
    def headers(self) -> dict[str, str] | None:
        return self._message.headers

    @property
    def content_type(self) -> str | None:
        return self._message.content_type

    @property
    def channel(self) -> str:
        return self._message.channel

    @property
    def is_acted_on(self) -> bool:
        return self._message.is_acted_on

    @property
    def message_id(self) -> str | None:
        """Unique identifier of a message if provided by the message broker."""
        return self._message.message_id

    async def ack(self) -> None:
        """Acknowledge the message."""
        await self._message.ack()

    async def nack(self) -> None:
        """Not-acknowledge the message."""
        await self._message.nack()

    async def reject(self) -> None:
        """Reject the message."""
        await self._message.reject()

    async def send_message(
        self,
        *,
        channel: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Send a new message to a specified channel."""
        await self._server.publish(
            channel=channel,
            message=MessageData(
                payload=payload,
                headers=headers,
                content_type=content_type,
            ),
            server_specific_parameters=server_specific_parameters,
        )

    async def send_message_json(
        self,
        *,
        channel: str,
        payload: Any,
        headers: dict[str, str] | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
        serializer: SerializerT | None = None,
    ) -> None:
        """Send a new message to a specified channel."""
        serializer = serializer if serializer is not None else self._default_serializer
        await self._server.publish(
            channel=channel,
            message=MessageData(
                payload=serializer(payload),
                headers=headers,
                content_type="application/json",
            ),
            server_specific_parameters=server_specific_parameters,
        )

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,  # if None, message will be sent to the same channel
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Atomically (if supporter by the server) ack and reply to the message."""
        await self._message.reply(
            payload=payload,
            headers=headers,
            content_type=content_type,
            channel=channel,
            server_specific_parameters=server_specific_parameters,
        )

    async def reply_json(
        self,
        *,
        payload: Any,
        headers: dict[str, str] | None = None,
        channel: str | None = None,  # if None, message will be sent to the same channel
        server_specific_parameters: dict[str, Any] | None = None,
        serializer: SerializerT | None = None,
    ) -> None:
        """Atomically (if supporter by the server) ack and reply with JSON to the message."""
        serializer = serializer if serializer is not None else self._default_serializer
        await self._message.reply(
            payload=serializer(payload),
            headers=headers,
            channel=channel,
            content_type="application/json",
            server_specific_parameters=server_specific_parameters,
        )


class MessageDependency:
    """Dependency annotation that indicates that the argument resolves to the received message."""

    async def resolve(self, *, context: DependencyContext) -> EnhancedReceivedMessage:
        return EnhancedReceivedMessage(
            server=context.server,
            message=context.message,
            default_serializer=context.default_serializer,
        )


# Type alias for convenience
Message = Annotated["EnhancedReceivedMessage", MessageDependency()]
