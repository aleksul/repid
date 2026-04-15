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
    ) -> None:
        self._server = server
        self._channel = channel
        self._queue_url = queue_url
        self._msg = msg
        self._action: MessageAction | None = None

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

    async def ack(self) -> None:
        if self._action is not None:
            return

        if self._server._client is not None and self._receipt_handle:
            try:
                await self._server._client.delete_message(
                    QueueUrl=self._queue_url,
                    ReceiptHandle=self._receipt_handle,
                )
            except botocore.exceptions.ClientError as e:
                error_response = getattr(e, "response", {})
                err = error_response.get("Error", {}) if isinstance(error_response, dict) else {}
                if isinstance(err, dict) and err.get("Code") != "ReceiptHandleIsInvalid":
                    raise

        self._action = MessageAction.acked

    async def nack(self) -> None:
        if self._action is not None:
            return

        if self._server._client is not None and self._receipt_handle:
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
            except botocore.exceptions.ClientError as e:
                error_response = getattr(e, "response", {})
                err = error_response.get("Error", {}) if isinstance(error_response, dict) else {}
                if isinstance(err, dict) and err.get("Code") != "ReceiptHandleIsInvalid":
                    raise

        self._action = MessageAction.nacked

    async def reject(self) -> None:
        if self._action is not None:
            return

        if self._server._client is not None and self._receipt_handle:
            try:
                await self._server._client.change_message_visibility(
                    QueueUrl=self._queue_url,
                    ReceiptHandle=self._receipt_handle,
                    VisibilityTimeout=0,
                )
            except botocore.exceptions.ClientError as e:
                error_response = getattr(e, "response", {})
                err = error_response.get("Error", {}) if isinstance(error_response, dict) else {}
                if isinstance(err, dict) and err.get("Code") != "ReceiptHandleIsInvalid":
                    raise

        self._action = MessageAction.rejected

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

        if self._server._client is not None and self._receipt_handle:
            reply_channel = channel if channel is not None else self._channel
            reply_queue_url = await self._server._get_queue_url(reply_channel)

            body_str = base64.b64encode(payload).decode("ascii")

            message_attributes: dict[str, Any] = {}
            if headers:
                for k, v in headers.items():
                    message_attributes[k] = {"DataType": "String", "StringValue": v}
            if content_type:
                message_attributes["content-type"] = {
                    "DataType": "String",
                    "StringValue": content_type,
                }
            if not body_str:
                body_str = EMPTY_PAYLOAD_BODY_PLACEHOLDER
                message_attributes[EMPTY_PAYLOAD_ATTRIBUTE] = {
                    "DataType": "String",
                    "StringValue": EMPTY_PAYLOAD_ATTRIBUTE_VALUE,
                }

            kwargs: dict[str, Any] = {
                "QueueUrl": reply_queue_url,
                "MessageBody": body_str,
                "MessageAttributes": message_attributes,
            }
            if server_specific_parameters:
                kwargs.update(server_specific_parameters)

            await self._server._client.send_message(**kwargs)

            try:
                await self._server._client.delete_message(
                    QueueUrl=self._queue_url,
                    ReceiptHandle=self._receipt_handle,
                )
            except botocore.exceptions.ClientError as e:
                error_response = getattr(e, "response", {})
                err = error_response.get("Error", {}) if isinstance(error_response, dict) else {}
                if isinstance(err, dict) and err.get("Code") != "ReceiptHandleIsInvalid":
                    raise  # pragma: no cover

        self._action = MessageAction.replied
