from __future__ import annotations

import base64
import binascii
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import botocore.exceptions

from repid.connections.abc import MessageAction, ReceivedMessageT
from repid.connections.sqs.constants import (
    EMPTY_PAYLOAD_ATTRIBUTE,
    EMPTY_PAYLOAD_ATTRIBUTE_VALUE,
    EMPTY_PAYLOAD_BODY_PLACEHOLDER,
)

if TYPE_CHECKING:
    from repid.connections.sqs.message_broker import SqsServer

logger = logging.getLogger("repid.connections.sqs")


class SqsReceivedMessage(ReceivedMessageT):
    def __init__(
        self,
        server: SqsServer,
        channel: str,
        queue_url: str,
        msg: Mapping[str, Any],
        visibility_timeout: int = 30,
    ) -> None:
        self._server = server
        self._channel = channel
        self._queue_url = queue_url
        self._msg = msg
        self._action: MessageAction | None = None
        self._visibility_timeout = visibility_timeout
        self._keep_alive_interval: int = visibility_timeout // 3

        self._message_id = msg.get("MessageId")
        self._receipt_handle = msg.get("ReceiptHandle")

        self._headers: dict[str, str] = {}
        self._content_type: str | None = None
        self._is_empty_payload = False

        attributes = msg.get("MessageAttributes", {})
        for key, value in attributes.items():
            str_value = value.get("StringValue")
            if key == "content-type":
                if str_value is not None:
                    self._content_type = str_value
            elif key == EMPTY_PAYLOAD_ATTRIBUTE:
                self._is_empty_payload = str_value == EMPTY_PAYLOAD_ATTRIBUTE_VALUE
            elif str_value is not None:
                self._headers[key] = str_value

        body = msg.get("Body", "")
        if self._is_empty_payload and body == EMPTY_PAYLOAD_BODY_PLACEHOLDER:
            self._payload = b""
        else:
            try:
                self._payload = base64.b64decode(body, validate=True)
            except (binascii.Error, ValueError):
                self._payload = str(body).encode("utf-8", errors="replace")

    @property
    def payload(self) -> bytes:
        return self._payload

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
        return self._channel

    @property
    def action(self) -> MessageAction | None:
        return self._action

    @property
    def is_acted_on(self) -> bool:
        return self._action is not None

    @property
    def message_id(self) -> str | None:
        return self._message_id

    @property
    def keep_alive_interval(self) -> int:
        return self._keep_alive_interval

    async def keep_alive(self) -> None:
        if self._action is not None:
            return
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        if not self._receipt_handle:
            return
        try:
            await self._server._client.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=self._receipt_handle,
                VisibilityTimeout=self._visibility_timeout,
            )
        except botocore.exceptions.ClientError as exc:
            if (
                not (error_dict := exc.response.get("Error"))
                or not isinstance(error_dict, dict)
                or error_dict.get("Code") != "ReceiptHandleIsInvalid"
            ):
                raise

    async def ack(self) -> None:
        if self._action is not None:
            return
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        if not self._receipt_handle:
            return
        try:
            await self._server._client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=self._receipt_handle,
            )
        except botocore.exceptions.ClientError as exc:
            if (
                not (error_dict := exc.response.get("Error"))
                or not isinstance(error_dict, dict)
                or error_dict.get("Code") != "ReceiptHandleIsInvalid"
            ):
                raise

        self._action = MessageAction.acked

    async def nack(self) -> None:
        if self._action is not None:
            return
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        if not self._receipt_handle:
            return

        dlq_strategy = self._server._dlq_queue_strategy
        if dlq_strategy:
            dlq_channel = dlq_strategy(self._channel)
            dlq_queue_url = await self._server._get_queue_url(dlq_channel)

            message_attributes: dict[str, Any] = {}
            for k, v in self._headers.items():
                message_attributes[k] = {"DataType": "String", "StringValue": v}
            if self._content_type:
                message_attributes["content-type"] = {
                    "DataType": "String",
                    "StringValue": self._content_type,
                }

            await self._server._client.send_message(
                QueueUrl=dlq_queue_url,
                MessageBody=self._msg.get("Body", ""),
                MessageAttributes=message_attributes,
            )

        try:
            await self._server._client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=self._receipt_handle,
            )
        except botocore.exceptions.ClientError as exc:
            if (
                not (error_dict := exc.response.get("Error"))
                or not isinstance(error_dict, dict)
                or error_dict.get("Code") != "ReceiptHandleIsInvalid"
            ):
                raise

        self._action = MessageAction.nacked

    async def reject(self) -> None:
        if self._action is not None:
            return
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        if not self._receipt_handle:
            return
        try:
            await self._server._client.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=self._receipt_handle,
                VisibilityTimeout=0,
            )
        except botocore.exceptions.ClientError as exc:
            if (
                not (error_dict := exc.response.get("Error"))
                or not isinstance(error_dict, dict)
                or error_dict.get("Code") != "ReceiptHandleIsInvalid"
            ):
                raise

        self._action = MessageAction.rejected

    async def reply(
        self,
        *,
        payload: bytes,  # noqa: ARG002
        headers: dict[str, str] | None = None,  # noqa: ARG002
        content_type: str | None = None,  # noqa: ARG002
        channel: str | None = None,  # noqa: ARG002
        server_specific_parameters: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> None:
        if self._action is not None:
            return
        raise NotImplementedError("SQS does not support native replies.")
