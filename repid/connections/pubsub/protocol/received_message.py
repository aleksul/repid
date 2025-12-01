"""Received message wrapper for Pub/Sub."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from repid.data import MessageData

from .proto import PubsubMessage, StreamingPullRequest

if TYPE_CHECKING:
    from repid.connections.pubsub.message_broker import PubsubServer


class PubsubReceivedMessage:
    """A received message from Pub/Sub via StreamingPull.

    Ack/nack operations are sent via the streaming pull write queue
    for efficiency.
    """

    def __init__(
        self,
        *,
        raw_message: PubsubMessage,
        ack_id: str,
        delivery_attempt: int,
        subscription_path: str,
        channel_name: str,
        write_queue: asyncio.Queue[StreamingPullRequest],
        server: PubsubServer,
    ) -> None:
        self._raw_message = raw_message
        self._ack_id = ack_id
        self._delivery_attempt = delivery_attempt
        self._subscription_path = subscription_path
        self._channel_name = channel_name
        self._write_queue = write_queue
        self._server = server
        self._is_acted_on = False

    @property
    def payload(self) -> bytes:
        return self._raw_message.data

    @property
    def headers(self) -> dict[str, str] | None:
        if self._raw_message.attributes:
            return dict(self._raw_message.attributes)
        return None

    @property
    def content_type(self) -> str | None:
        return self._raw_message.attributes.get("content_type")

    @property
    def channel(self) -> str:
        return self._channel_name

    @property
    def is_acted_on(self) -> bool:
        return self._is_acted_on

    @property
    def message_id(self) -> str | None:
        return self._raw_message.message_id or None

    @property
    def delivery_attempt(self) -> int:
        return self._delivery_attempt

    async def ack(self) -> None:
        """Acknowledge the message."""
        if self._is_acted_on:
            return

        request = StreamingPullRequest(ack_ids=[self._ack_id])
        await self._write_queue.put(request)
        self._is_acted_on = True

    async def nack(self) -> None:
        """Negative acknowledge the message."""
        if self._is_acted_on:
            return

        # Set ack deadline to 0 to nack
        request = StreamingPullRequest(
            modify_deadline_seconds=[0],
            modify_deadline_ack_ids=[self._ack_id],
        )
        await self._write_queue.put(request)
        self._is_acted_on = True

    async def reject(self) -> None:
        """Reject the message."""
        if self._is_acted_on:
            return

        # Set a short ack to reject message asap (pubsub doesn't have explicit reject)
        request = StreamingPullRequest(
            modify_deadline_seconds=[1],
            modify_deadline_ack_ids=[self._ack_id],
        )
        await self._write_queue.put(request)
        self._is_acted_on = True

    async def extend_deadline(self, seconds: int) -> None:
        """Extend the ack deadline for this message.

        Args:
            seconds: New deadline in seconds (10-600).
        """
        if self._is_acted_on:
            return

        seconds = max(10, min(600, seconds))
        request = StreamingPullRequest(
            modify_deadline_seconds=[seconds],
            modify_deadline_ack_ids=[self._ack_id],
        )
        await self._write_queue.put(request)

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Reply by publishing a message and acknowledging this one."""
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
