"""Received message wrapper for Pub/Sub."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from repid.connections.abc import MessageAction

from .control_batcher import OperationInProgressOnCancellation, PubsubControlBatcher
from .proto import PubsubMessage

if TYPE_CHECKING:
    from repid.connections.pubsub.message_broker import PubsubServer


class PubsubReceivedMessage:
    """A received message from Pub/Sub via StreamingPull.

    Ack/nack operations are sent via unary Acknowledge/ModifyAckDeadline
    RPCs for reliability (not via the streaming pull write queue).
    """

    def __init__(
        self,
        *,
        raw_message: PubsubMessage,
        ack_id: str,
        delivery_attempt: int,
        subscription_path: str,
        channel_name: str,
        server: PubsubServer,
        stream_ack_deadline_seconds: int,
    ) -> None:
        self._raw_message = raw_message
        self._ack_id = ack_id
        self._delivery_attempt = delivery_attempt
        self._subscription_path = subscription_path
        self._channel_name = channel_name
        self._server = server
        self._action: MessageAction | None = None
        self._keep_alive_during_settlement = False
        self._deadline_lock = asyncio.Lock()
        self._pending_deadline: asyncio.Future[None] | None = None
        self._stream_ack_deadline_seconds = stream_ack_deadline_seconds
        self._keep_alive_interval = stream_ack_deadline_seconds / 3

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

    @property
    def keep_alive_interval(self) -> float:
        return self._keep_alive_interval

    @property
    def _batcher(self) -> PubsubControlBatcher:
        batcher = self._server._control_batcher
        if batcher is None:
            raise ConnectionError("Control batcher not available.")
        return batcher

    async def keep_alive(self) -> bool | None:
        if self._action is not None and not self._keep_alive_during_settlement:
            return False
        await self.extend_deadline(self._stream_ack_deadline_seconds)
        return None

    async def ack(self) -> None:
        """Acknowledge the message via batched unary Acknowledge RPC."""
        if self._action is not None:
            return
        await self._settle(
            MessageAction.acked,
            lambda: self._batcher.add_ack(self._subscription_path, self._ack_id),
        )

    async def nack(self) -> None:
        """Negative acknowledge the message (set deadline to 0 for redelivery)."""
        await self._redeliver(MessageAction.nacked, 0)

    async def reject(self) -> None:
        """Reject the message (set deadline to 1 second)."""
        await self._redeliver(MessageAction.rejected, 1)

    async def _redeliver(self, action: MessageAction, seconds: int) -> None:
        if self._action is not None:
            return
        async with self._deadline_lock:
            await self._wait_for_pending_deadline()
            if self._action is not None:
                return
            await self._settle(
                action,
                lambda: self._batcher.add_modify_deadline(
                    self._subscription_path,
                    self._ack_id,
                    seconds,
                ),
            )

    async def _settle(
        self,
        action: MessageAction,
        operation: Callable[[], Awaitable[None]],
    ) -> None:
        self._action = action
        self._keep_alive_during_settlement = action is MessageAction.acked

        # The action is reserved before I/O, so shutdown can no longer reject
        # this delivery. Keep its settlement queued until the broker responds.
        settlement = asyncio.gather(self._perform_settlement(operation), return_exceptions=True)
        try:
            (result,) = await asyncio.shield(settlement)
        except asyncio.CancelledError:
            await asyncio.wait({settlement})
            raise
        if isinstance(result, BaseException):
            raise result

    async def _perform_settlement(self, operation: Callable[[], Awaitable[None]]) -> None:
        try:
            await operation()
        except Exception:
            self._action = None
            raise
        finally:
            self._keep_alive_during_settlement = False

    async def extend_deadline(self, seconds: int) -> None:
        """Extend the ack deadline for this message.

        Args:
            seconds: New deadline in seconds (10-600).
        """
        async with self._deadline_lock:
            await self._wait_for_pending_deadline()
            if self._action is not None and not self._keep_alive_during_settlement:
                return
            try:
                await self._batcher.add_modify_deadline(
                    self._subscription_path,
                    self._ack_id,
                    max(10, min(600, seconds)),
                )
            except OperationInProgressOnCancellation as exc:
                self._pending_deadline = exc.completion
                raise

    async def _wait_for_pending_deadline(self) -> None:
        # A cancelled deadline extension can leave its RPC running after releasing the lock.
        if self._pending_deadline is not None:
            await asyncio.wait({self._pending_deadline})
            self._pending_deadline = None

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
