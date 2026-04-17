from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from repid.connections.abc import CapabilitiesT, SentMessageT, ServerT, SubscriberT
from repid.connections.kafka.subscriber import KafkaSubscriber

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.connections.abc import ReceivedMessageT
    from repid.connections.kafka.protocols import AIOKafkaProducerProtocol
    from repid.data import ExternalDocs, Tag

logger = logging.getLogger("repid.connections.kafka")


class KafkaServer(ServerT):
    def __init__(
        self,
        dsn: str,
        *,
        client_id: str | None = None,
        security_protocol: str = "PLAINTEXT",
        ssl_context: Any | None = None,
        sasl_mechanism: str = "PLAIN",
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        sasl_kerberos_service_name: str = "kafka",
        sasl_kerberos_domain_name: str | None = None,
        sasl_oauth_token_provider: Any | None = None,
        connections_max_idle_ms: int = 540000,
        request_timeout_ms: int = 40000,
        retry_backoff_ms: int = 100,
        metadata_max_age_ms: int = 300000,
        dlq_topic_strategy: Callable[[str], str] | None = lambda channel: f"repid_{channel}_dlq",
        reject_topic_strategy: Callable[[str], str] | None = lambda channel: channel,
        group_id: str | None = "repid-group",
        group_instance_id: str | None = None,
        auto_offset_reset: str = "earliest",
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        variables: Mapping[str, ServerVariable] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: ServerBindingsObject | None = None,
    ) -> None:
        self.dsn = dsn
        self._producer: AIOKafkaProducerProtocol | None = None
        self._active_subscribers: list[KafkaSubscriber] = []
        self._dlq_topic_strategy = dlq_topic_strategy
        self._reject_topic_strategy = reject_topic_strategy
        self._group_id = group_id
        self._group_instance_id = group_instance_id
        self._auto_offset_reset = auto_offset_reset

        self._conn_kwargs: dict[str, Any] = {
            "security_protocol": security_protocol,
            "ssl_context": ssl_context,
            "sasl_mechanism": sasl_mechanism,
            "sasl_plain_username": sasl_plain_username,
            "sasl_plain_password": sasl_plain_password,
            "sasl_kerberos_service_name": sasl_kerberos_service_name,
            "sasl_kerberos_domain_name": sasl_kerberos_domain_name,
            "sasl_oauth_token_provider": sasl_oauth_token_provider,
            "connections_max_idle_ms": connections_max_idle_ms,
            "request_timeout_ms": request_timeout_ms,
            "retry_backoff_ms": retry_backoff_ms,
            "metadata_max_age_ms": metadata_max_age_ms,
        }
        if client_id is not None:
            self._conn_kwargs["client_id"] = client_id

        # AsyncAPI metadata
        self._title = title
        self._summary = summary
        self._description = description
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings

    @property
    def host(self) -> str:
        return self.dsn

    @property
    def protocol(self) -> str:
        return "kafka"

    @property
    def pathname(self) -> str | None:
        return None

    @property
    def title(self) -> str | None:
        return self._title

    @property
    def summary(self) -> str | None:
        return self._summary

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def protocol_version(self) -> str | None:
        return None

    @property
    def variables(self) -> Mapping[str, ServerVariable] | None:
        return self._variables

    @property
    def security(self) -> Sequence[Any] | None:
        return self._security

    @property
    def tags(self) -> Sequence[Tag] | None:
        return self._tags

    @property
    def external_docs(self) -> ExternalDocs | None:
        return self._external_docs

    @property
    def bindings(self) -> ServerBindingsObject | None:
        return self._bindings

    @property
    def capabilities(self) -> CapabilitiesT:
        return {
            "supports_acknowledgments": True,
            "supports_persistence": True,
            "supports_reply": False,
            "supports_lightweight_pause": False,
        }

    @property
    def is_connected(self) -> bool:
        return self._producer is not None

    async def connect(self) -> None:
        if self._producer is None:
            self._producer = AIOKafkaProducer(bootstrap_servers=self.dsn, **self._conn_kwargs)
            await self._producer.start()
            logger.info("server.connect", extra={"host": self.dsn})

    async def disconnect(self) -> None:
        # theoretically, all subscribers should have been closed before disconnect is called
        # but we'll attempt to close any that are still active just in case
        for subscriber in list(self._active_subscribers):  # pragma: no cover
            try:
                await subscriber.close()
            except Exception as exc:
                logger.exception("server.disconnect.subscriber_close_error", exc_info=exc)
        self._active_subscribers.clear()

        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
            logger.info("server.disconnect")

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[KafkaServer]:
        await self.connect()
        try:
            yield self
        finally:
            await self.disconnect()

    async def publish(
        self,
        *,
        channel: str,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not connected.")

        logger.debug("channel.publish", extra={"channel": channel})

        headers = []
        if message.headers:
            for k, v in message.headers.items():
                headers.append((k, v.encode()))
        if message.content_type:
            headers.append(("content-type", message.content_type.encode()))

        await self._producer.send_and_wait(
            topic=channel,
            value=message.payload,
            headers=headers,
        )

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        if not self.is_connected:  # pragma: no cover
            raise ConnectionError("Kafka producer is not connected.")

        logger.debug("channel.subscribe", extra={"channels": list(channels_to_callbacks.keys())})

        consumer = AIOKafkaConsumer(
            *channels_to_callbacks.keys(),
            bootstrap_servers=self.dsn,
            group_id=self._group_id,
            group_instance_id=self._group_instance_id,
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
            **self._conn_kwargs,
        )
        await consumer.start()

        subscriber = KafkaSubscriber(
            server=self,
            consumer=consumer,  # pyright: ignore[reportArgumentType]
            channels_to_callbacks=channels_to_callbacks,
            concurrency_limit=concurrency_limit,
        )
        self._active_subscribers.append(subscriber)

        def _on_done(_task: asyncio.Task[Any]) -> None:
            if subscriber in self._active_subscribers:
                self._active_subscribers.remove(subscriber)

        subscriber.task.add_done_callback(_on_done)

        return subscriber
