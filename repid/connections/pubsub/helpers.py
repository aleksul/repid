from __future__ import annotations

from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

from gcloud.aio.pubsub import PubsubMessage, SubscriberClient, SubscriberMessage

from repid.data import MessageData

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT

    from .message_broker import PubsubServer


class ChannelOverride(TypedDict, total=False):
    project: str
    topic: str
    subscription: str


@dataclass(slots=True, kw_only=True)
class _ChannelConfig:
    channel: str
    subscription_path: str
    callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]]


@dataclass(slots=True, kw_only=True)
class _QueuedDelivery:
    callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]]
    message: ReceivedMessageT


class _EncodablePubsubMessage(PubsubMessage):
    def __init__(
        self,
        *,
        payload: bytes,
        attributes: Mapping[str, str] | None,
        ordering_key: str | None,
    ) -> None:
        super().__init__(payload, ordering_key=ordering_key or "")
        self.attributes = dict(attributes) if attributes else {}


class PubsubReceivedMessage:
    def __init__(
        self,
        *,
        raw: SubscriberMessage,
        subscription_path: str,
        channel_name: str,
        subscriber_client: SubscriberClient,
        server: PubsubServer,
    ) -> None:
        self._raw = raw
        self._subscription_path = subscription_path
        self._channel_name = channel_name
        self._subscriber_client = subscriber_client
        self._server = server
        self._is_acted_on = False

    @property
    def payload(self) -> bytes:
        return self._raw.data or b""

    @property
    def headers(self) -> dict[str, str] | None:
        if self._raw.attributes:
            return {str(k): str(v) for k, v in self._raw.attributes.items()}
        return None

    @property
    def content_type(self) -> str | None:
        headers = self.headers
        return headers.get("content_type") if headers else None

    @property
    def channel(self) -> str:
        return self._channel_name

    @property
    def is_acted_on(self) -> bool:
        return self._is_acted_on

    @property
    def message_id(self) -> str | None:
        return self._raw.message_id

    async def ack(self) -> None:
        if self._is_acted_on:
            return
        await self._subscriber_client.acknowledge(
            self._subscription_path,
            [self._raw.ack_id],
        )
        self._is_acted_on = True

    async def nack(self) -> None:
        if self._is_acted_on:
            return
        await self._subscriber_client.modify_ack_deadline(
            self._subscription_path,
            [self._raw.ack_id],
            ack_deadline_seconds=0,
        )
        self._is_acted_on = True

    async def reject(self) -> None:
        if self._is_acted_on:
            return
        await self._subscriber_client.modify_ack_deadline(
            self._subscription_path,
            [self._raw.ack_id],
            ack_deadline_seconds=1,
        )
        self._is_acted_on = True

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
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
