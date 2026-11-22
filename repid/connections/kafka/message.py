from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from repid.connections.abc import MessageAction, ReceivedMessageT

if TYPE_CHECKING:
    from repid.connections.kafka.message_broker import KafkaServer
    from repid.connections.kafka.protocols import ConsumerRecordProtocol


class KafkaReceivedMessage(ReceivedMessageT):
    def __init__(
        self,
        server: KafkaServer,
        record: ConsumerRecordProtocol,
        mark_complete_callback: Callable[[], Awaitable[None]],
        is_owned: Callable[[], bool],
        on_settlement_failed: Callable[[], None] | None = None,
    ) -> None:
        self._server = server
        self._record = record
        self._mark_complete_callback = mark_complete_callback
        self._is_owned = is_owned
        self._on_settlement_failed = on_settlement_failed

        self._action: MessageAction | None = None
        self._published_action: MessageAction | None = None
        self._payload = record.value
        self._channel = record.topic
        self._message_id = f"{record.topic}:{record.partition}:{record.offset}"

        self._headers: dict[str, str] = {}
        self._content_type: str | None = None

        if record.headers:
            for k, v in record.headers:
                if k == "content-type":
                    self._content_type = v.decode(errors="replace") if v is not None else ""
                else:
                    self._headers[k] = v.decode(errors="replace") if v is not None else ""

    @property
    def is_owned(self) -> bool:
        """Whether this delivery may still settle its record for the current assignment."""
        return self._is_owned()

    @property
    def payload(self) -> bytes:
        return self._payload if self._payload is not None else b""

    @property
    def headers(self) -> dict[str, str] | None:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._content_type

    @property
    def reply_to(self) -> str | None:
        return None

    @property
    def channel(self) -> str:
        return str(self._channel)

    @property
    def action(self) -> MessageAction | None:
        return self._action

    @property
    def is_acted_on(self) -> bool:
        return self._action is not None

    @property
    def message_id(self) -> str:
        return self._message_id

    @property
    def keep_alive_interval(self) -> int | None:  # pragma: no cover
        return None

    async def keep_alive(self) -> None:  # pragma: no cover
        pass

    async def ack(self) -> None:
        await self._settle(MessageAction.acked)

    async def _settle(
        self,
        action: MessageAction,
        destination: Callable[[str], str] | None = None,
    ) -> None:
        if self._action is not None or not self._is_owned():
            return

        # A publish can succeed even if its subsequent offset commit fails.
        # Retrying that settlement only needs to complete the offset.
        action = self._published_action or action
        self._action = action
        settlement = asyncio.create_task(self._perform_settlement(action, destination))
        try:
            await asyncio.wait({settlement})
        except asyncio.CancelledError:
            # Publication and offset completion are one reserved settlement.
            # Cancelling only its caller must not strand the offset halfway through.
            settlement.add_done_callback(self._retrieve_settlement_error)
            await asyncio.wait({settlement})
            raise
        settlement.result()

    @staticmethod
    def _retrieve_settlement_error(task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            task.exception()

    async def _perform_settlement(
        self,
        action: MessageAction,
        destination: Callable[[str], str] | None,
    ) -> None:
        try:
            if not self._is_owned():
                return
            if destination is not None and self._published_action is None:
                await self._publish(action, destination(self._channel))
                self._published_action = action
            await self._mark_complete_callback()
        except Exception:
            self._action = None
            # Let a refetch of this record deliver it again.
            if self._on_settlement_failed is not None:
                self._on_settlement_failed()
            raise

    async def nack(self) -> None:
        await self._settle(MessageAction.nacked, self._server._dlq_topic_strategy)

    async def reject(self) -> None:
        await self._settle(MessageAction.rejected, self._server._reject_topic_strategy)

    async def _publish(self, action: MessageAction, topic: str) -> None:
        producer = self._server._producer
        if producer is None:  # pragma: no cover
            raise ConnectionError("Kafka producer is not connected.")

        headers = [(key, value.encode()) for key, value in self._headers.items()]
        if self._content_type:
            headers.append(("content-type", self._content_type.encode()))
        if action is MessageAction.nacked:
            headers.append(("x-original-topic", self._channel.encode()))
            headers.append(("x-original-offset", str(self._record.offset).encode()))
        await producer.send_and_wait(topic=topic, value=self._payload, headers=headers)

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
        _ = (payload, headers, content_type, channel, server_specific_parameters)
        raise NotImplementedError("Kafka does not support native replies.")
