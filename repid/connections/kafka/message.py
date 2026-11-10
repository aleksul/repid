from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
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
        mark_complete_callback: Callable[[], Coroutine[Any, Any, None]],
        is_owned: Callable[[], bool],
    ) -> None:
        self._server = server
        self._record = record
        self._mark_complete_callback = mark_complete_callback
        self._is_owned = is_owned

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

    @property
    def keep_alive_interval(self) -> int | None:
        return None

    async def keep_alive(self) -> None:  # pragma: no cover
        pass

    async def ack(self) -> None:
        if self._action is not None or not self._is_owned():
            return

        await self._settle(MessageAction.acked)

    async def _settle(
        self,
        action: MessageAction,
        publish: Coroutine[Any, Any, Any] | None = None,
    ) -> None:
        self._action = action

        async def settle() -> None:
            try:
                if publish is not None:
                    if not self._is_owned():
                        publish.close()
                        return
                    await publish
                await self._mark_complete_callback()
            except Exception:
                self._action = None
                raise

        settlement = asyncio.create_task(settle())
        try:
            await asyncio.wait({settlement})
        except asyncio.CancelledError:
            # Publication and offset completion are one reserved settlement.
            # Cancelling only its caller must not strand the offset halfway through.
            await asyncio.gather(settlement, return_exceptions=True)
            raise
        settlement.result()

    async def nack(self) -> None:
        if self._action is not None or not self._is_owned():
            return

        if self._server._producer is None:  # pragma: no cover
            # it should be impossible to get here since subscribing requires a connection,
            # but we'll check just in case
            raise ConnectionError("Kafka producer is not connected.")

        publish = None
        dlq_topic_strategy = self._server._dlq_topic_strategy
        if dlq_topic_strategy is not None:
            dlq_topic = dlq_topic_strategy(self._channel)
            headers = []
            if self._headers:
                for k, v in self._headers.items():
                    headers.append((k, v.encode()))
            if self._content_type:
                headers.append(("content-type", self._content_type.encode()))

            headers.append(("x-original-topic", self._channel.encode()))
            headers.append(("x-original-offset", str(self._record.offset).encode()))

            publish = self._server._producer.send_and_wait(
                topic=dlq_topic,
                value=self._payload,
                headers=headers,
            )

        await self._settle(MessageAction.nacked, publish)

    async def reject(self) -> None:
        if self._action is not None or not self._is_owned():
            return

        if self._server._producer is None:  # pragma: no cover
            # it should be impossible to get here since subscribing requires a connection,
            # but we'll check just in case
            raise ConnectionError("Kafka producer is not connected.")

        publish = None
        reject_topic_strategy = self._server._reject_topic_strategy
        if reject_topic_strategy is not None:
            reject_topic = reject_topic_strategy(self._channel)
            headers = []
            if self._headers:
                for k, v in self._headers.items():
                    headers.append((k, v.encode()))
            if self._content_type:
                headers.append(("content-type", self._content_type.encode()))

            publish = self._server._producer.send_and_wait(
                topic=reject_topic,
                value=self._payload,
                headers=headers,
            )

        await self._settle(MessageAction.rejected, publish)

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
