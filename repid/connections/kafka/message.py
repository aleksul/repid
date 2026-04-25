from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from repid.connections.abc import MessageAction, ReceivedMessageT

if TYPE_CHECKING:
    from repid.connections.kafka.message_broker import KafkaServer
    from repid.connections.kafka.protocols import ConsumerRecordProtocol

logger = logging.getLogger("repid.connections.kafka")


class KafkaReceivedMessage(ReceivedMessageT):
    def __init__(
        self,
        server: KafkaServer,
        record: ConsumerRecordProtocol,
        mark_complete_callback: Callable[[ConsumerRecordProtocol], Coroutine[Any, Any, None]],
    ) -> None:
        self._server = server
        self._record = record
        self._mark_complete_callback = mark_complete_callback

        self._action: MessageAction | None = None
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

    async def ack(self) -> None:
        if self._action is not None:
            return

        self._action = MessageAction.acked
        try:
            await self._mark_complete_callback(self._record)
        except Exception:  # pragma: no cover
            self._action = None
            raise

    async def nack(self) -> None:
        if self._action is not None:
            return

        if self._server._producer is None:  # pragma: no cover
            # it should be impossible to get here since subscribing requires a connection,
            # but we'll check just in case
            raise ConnectionError("Kafka producer is not connected.")

        try:
            dlq_topic_strategy = self._server._dlq_topic_strategy
            if dlq_topic_strategy is not None and self._server._producer is not None:
                dlq_topic = dlq_topic_strategy(self._channel)
                headers = []
                if self._headers:
                    for k, v in self._headers.items():
                        headers.append((k, v.encode()))
                if self._content_type:
                    headers.append(("content-type", self._content_type.encode()))

                headers.append(("x-original-topic", self._channel.encode()))
                headers.append(("x-original-offset", str(self._record.offset).encode()))

                await self._server._producer.send_and_wait(
                    topic=dlq_topic,
                    value=self._payload,
                    headers=headers,
                )

            self._action = MessageAction.nacked
            await self._mark_complete_callback(self._record)
        except Exception:  # pragma: no cover
            self._action = None
            raise

    async def reject(self) -> None:
        if self._action is not None:
            return

        if self._server._producer is None:  # pragma: no cover
            # it should be impossible to get here since subscribing requires a connection,
            # but we'll check just in case
            raise ConnectionError("Kafka producer is not connected.")

        try:
            reject_topic_strategy = self._server._reject_topic_strategy
            if reject_topic_strategy is not None:
                reject_topic = reject_topic_strategy(self._channel)
                headers = []
                if self._headers:
                    for k, v in self._headers.items():
                        headers.append((k, v.encode()))
                if self._content_type:
                    headers.append(("content-type", self._content_type.encode()))

                await self._server._producer.send_and_wait(
                    topic=reject_topic,
                    value=self._payload,
                    headers=headers,
                )

            self._action = MessageAction.rejected
            await self._mark_complete_callback(self._record)
        except Exception:  # pragma: no cover
            self._action = None
            raise

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
