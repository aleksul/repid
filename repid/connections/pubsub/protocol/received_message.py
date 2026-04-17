"""Received message wrapper for Pub/Sub."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from repid.connections.abc import MessageAction

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
        self._action: MessageAction | None = None

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
    def reply_to(self) -> str | None:
        return self._raw_message.attributes.get("reply_to")

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
        return self._raw_message.message_id or None

    @property
    def delivery_attempt(self) -> int:
        return self._delivery_attempt

    async def _send_ack_request(self) -> None:
        """Send the ack request to the write queue without updating `_action`."""
        request = StreamingPullRequest(ack_ids=[self._ack_id])
        await self._write_queue.put(request)

    async def ack(self) -> None:
        """Acknowledge the message."""
        if self._action is not None:
            return
        await self._send_ack_request()
        self._action = MessageAction.acked

    async def nack(self) -> None:
        """Negative acknowledge the message."""
        if self._action is not None:
            return
        request = StreamingPullRequest(
            modify_deadline_seconds=[0],
            modify_deadline_ack_ids=[self._ack_id],
        )
        await self._write_queue.put(request)
        self._action = MessageAction.nacked

    async def reject(self) -> None:
        """Reject the message."""
        if self._action is not None:
            return
        # Set a short ack to reject message asap (pubsub doesn't have explicit reject)
        request = StreamingPullRequest(
            modify_deadline_seconds=[1],
            modify_deadline_ack_ids=[self._ack_id],
        )
        await self._write_queue.put(request)
        self._action = MessageAction.rejected

    async def extend_deadline(self, seconds: int) -> None:
        """Extend the ack deadline for this message.

        Args:
            seconds: New deadline in seconds (10-600).
        """
        if self._action is not None:
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
        if self._action is not None:
            return
        _ = (payload, headers, content_type, channel, server_specific_parameters)
        raise NotImplementedError("PubSub does not support native replies.")
