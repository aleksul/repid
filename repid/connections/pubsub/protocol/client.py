from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import partial
from typing import Protocol, TypeVar

import grpc.aio

from .credentials import CredentialsProvider
from .proto import AcknowledgeRequest, ModifyAckDeadlineRequest, PublishRequest, PublishResponse
from .resilience import ResilienceState, with_retry

ResponseT = TypeVar("ResponseT")


class SerializableRequest(Protocol):
    def serialize(self) -> bytes: ...


PUBLISH_METHOD = "/google.pubsub.v1.Publisher/Publish"
ACKNOWLEDGE_METHOD = "/google.pubsub.v1.Subscriber/Acknowledge"
MODIFY_ACK_DEADLINE_METHOD = "/google.pubsub.v1.Subscriber/ModifyAckDeadline"


class PubsubProtocolClient:
    """Low-level Pub/Sub unary gRPC client."""

    def __init__(
        self,
        *,
        channel: grpc.aio.Channel,
        credentials_provider: CredentialsProvider,
        resilience_state: ResilienceState,
    ) -> None:
        self._channel = channel
        self._credentials_provider = credentials_provider
        self._resilience_state = resilience_state

    @staticmethod
    def _serialize_request(request: SerializableRequest) -> bytes:
        return request.serialize()

    @staticmethod
    def _deserialize_empty_response(response: bytes) -> bytes:
        return response

    async def _unary_call(
        self,
        *,
        method: str,
        request: SerializableRequest,
        response_deserializer: Callable[[bytes], ResponseT],
        timeout: int | None = None,
    ) -> ResponseT:
        await self._credentials_provider.ensure_valid()
        unary_call = self._channel.unary_unary(
            method,
            request_serializer=self._serialize_request,
            response_deserializer=response_deserializer,
        )
        if timeout is None:
            return await unary_call(request)
        return await unary_call(request, timeout=timeout)

    async def publish(self, request: PublishRequest, timeout: int | None = None) -> None:
        await with_retry(
            self._resilience_state,
            partial(
                self._unary_call,
                method=PUBLISH_METHOD,
                request=request,
                response_deserializer=PublishResponse.deserialize,
                timeout=timeout,
            ),
            operation_name="publish",
        )

    async def acknowledge(self, subscription_path: str, ack_ids: Sequence[str]) -> None:
        if not ack_ids:
            return

        request = AcknowledgeRequest(
            subscription=subscription_path,
            ack_ids=list(ack_ids),
        )

        await with_retry(
            self._resilience_state,
            partial(
                self._unary_call,
                method=ACKNOWLEDGE_METHOD,
                request=request,
                response_deserializer=self._deserialize_empty_response,
            ),
            operation_name="acknowledge",
        )

    async def modify_ack_deadline(
        self,
        subscription_path: str,
        ack_ids: Sequence[str],
        seconds: int,
    ) -> None:
        if not ack_ids:
            return

        seconds = max(0, min(600, seconds))
        request = ModifyAckDeadlineRequest(
            subscription=subscription_path,
            ack_ids=list(ack_ids),
            ack_deadline_seconds=seconds,
        )

        await with_retry(
            self._resilience_state,
            partial(
                self._unary_call,
                method=MODIFY_ACK_DEADLINE_METHOD,
                request=request,
                response_deserializer=self._deserialize_empty_response,
            ),
            operation_name="modify_ack_deadline",
        )
