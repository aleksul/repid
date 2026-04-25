from __future__ import annotations

import base64
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from aiobotocore.session import get_session

from repid.connections.abc import CapabilitiesT, SentMessageT, ServerT, SubscriberT
from repid.connections.sqs.constants import (
    EMPTY_PAYLOAD_ATTRIBUTE,
    EMPTY_PAYLOAD_ATTRIBUTE_VALUE,
    EMPTY_PAYLOAD_BODY_PLACEHOLDER,
)
from repid.connections.sqs.subscriber import SqsSubscriber

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient

    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.connections.abc import ReceivedMessageT
    from repid.data import ExternalDocs, Tag

logger = logging.getLogger("repid.connections.sqs")
SQS_MAX_BATCH_SIZE = 10


class SqsServer(ServerT):
    def __init__(
        self,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        *,
        dlq_queue_strategy: Callable[[str], str] | None = lambda channel: f"repid_{channel}_dlq",
        receive_wait_time_seconds: int = 20,
        batch_size: int = 10,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        variables: Mapping[str, ServerVariable] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: ServerBindingsObject | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
    ) -> None:
        if batch_size <= 0 or batch_size > SQS_MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be between 1 and {SQS_MAX_BATCH_SIZE} for SQS.")

        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token

        self._dlq_queue_strategy = dlq_queue_strategy
        self._receive_wait_time_seconds = receive_wait_time_seconds
        self._batch_size = batch_size

        self._session = get_session()
        self._client: SQSClient | None = None
        self._client_cm: Any = None
        self._queue_url_cache: dict[str, str] = {}

        self._active_subscribers: set[SqsSubscriber] = set()

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
        return self.endpoint_url or "aws-sqs"

    @property
    def protocol(self) -> str:
        return "sqs"

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
            "supports_native_reply": False,
            "supports_lightweight_pause": False,
        }

    @property
    def is_connected(self) -> bool:
        return self._client is not None

    async def connect(self) -> None:
        if self._client is None:
            self._client_cm = self._session.create_client(
                "sqs",
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
                aws_session_token=self._aws_session_token,
            )
            self._client = await self._client_cm.__aenter__()
            logger.info("server.connect", extra={"host": self.host})

    async def disconnect(self) -> None:
        if self._client is not None:
            try:
                for subscriber in list(self._active_subscribers):
                    try:
                        await subscriber.close()
                    except Exception:
                        logger.exception("subscriber.closing_error")
            finally:
                self._active_subscribers.clear()

            await self._client_cm.__aexit__(None, None, None)
            self._client = None
            self._queue_url_cache.clear()
            logger.info("server.disconnect")

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[SqsServer]:
        await self.connect()
        try:
            yield self
        finally:
            await self.disconnect()

    async def _get_queue_url(self, queue_name: str) -> str:
        if self._client is None:
            raise RuntimeError("SQS client is not connected.")

        if queue_name in self._queue_url_cache:
            return self._queue_url_cache[queue_name]

        response = await self._client.get_queue_url(QueueName=queue_name)
        queue_url = response["QueueUrl"]
        self._queue_url_cache[queue_name] = queue_url
        return queue_url

    async def publish(
        self,
        *,
        channel: str,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        if self._client is None:
            raise RuntimeError("SQS client is not connected.")

        logger.debug("channel.publish", extra={"channel": channel})

        queue_url = await self._get_queue_url(channel)

        body_str = base64.b64encode(message.payload).decode("ascii")

        message_attributes: dict[str, Any] = {}
        if message.headers:
            for k, v in message.headers.items():
                message_attributes[k] = {"DataType": "String", "StringValue": v}
        if message.content_type:
            message_attributes["content-type"] = {
                "DataType": "String",
                "StringValue": message.content_type,
            }
        if not body_str:
            body_str = EMPTY_PAYLOAD_BODY_PLACEHOLDER
            message_attributes[EMPTY_PAYLOAD_ATTRIBUTE] = {
                "DataType": "String",
                "StringValue": EMPTY_PAYLOAD_ATTRIBUTE_VALUE,
            }

        kwargs: dict[str, Any] = {
            "QueueUrl": queue_url,
            "MessageBody": body_str,
            "MessageAttributes": message_attributes,
        }

        if server_specific_parameters:
            kwargs.update(server_specific_parameters)

        await self._client.send_message(**kwargs)

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        logger.debug("channel.subscribe", extra={"channels": list(channels_to_callbacks.keys())})

        if self._client is None:
            raise RuntimeError("SQS client is not connected.")

        sub = SqsSubscriber(
            server=self,
            channels_to_callbacks=channels_to_callbacks,
            concurrency_limit=concurrency_limit,
        )
        self._active_subscribers.add(sub)
        sub.task.add_done_callback(lambda _: self._active_subscribers.discard(sub))
        return sub
