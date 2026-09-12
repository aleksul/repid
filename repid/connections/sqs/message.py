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

    @staticmethod
    def _is_stale_receipt_error(exc: botocore.exceptions.ClientError) -> bool:
        error_dict = exc.response.get("Error")
        return (
            isinstance(error_dict, dict)
            and bool(error_dict)
            and error_dict.get("Code") == "ReceiptHandleIsInvalid"
        )

    async def keep_alive(self) -> None:
        # Settlements reserve _action before their first await, so a plain
        # check here is enough to keep a renewal from racing a settlement.
        if self._action is not None:
            return
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        if not self._receipt_handle:
            return
        await self._change_visibility(self._visibility_timeout)

    async def _change_visibility(self, visibility_timeout: int) -> None:
        # Callers validate client and receipt handle first; re-check here so
        # mypy narrowing survives the method boundary.
        client = self._server._client
        receipt_handle = self._receipt_handle
        if client is None or not receipt_handle:  # pragma: no cover
            raise ConnectionError("SQS client is not connected.")
        try:
            await client.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout,
            )
        except botocore.exceptions.ClientError as exc:
            if not self._is_stale_receipt_error(exc):
                raise

    async def _delete_message(self) -> None:
        client = self._server._client
        receipt_handle = self._receipt_handle
        if client is None or not receipt_handle:  # pragma: no cover
            raise ConnectionError("SQS client is not connected.")
        try:
            await client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle,
            )
        except botocore.exceptions.ClientError as exc:
            if not self._is_stale_receipt_error(exc):
                raise

    async def ack(self) -> None:
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        if not self._receipt_handle:
            return
        # Reserve the action before the RPC: in a single event loop the
        # check-and-set is atomic, so concurrent settlements are deduplicated,
        # and a settlement cancelled mid-RPC is never followed by a second one.
        if self._action is not None:
            return
        self._action = MessageAction.acked
        try:
            await self._delete_message()
        except Exception:
            # Cancellation is not caught, so a cancelled settlement stays
            # reserved even if the RPC may have reached the server.
            self._action = None
            raise

    async def nack(self) -> None:
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        if not self._receipt_handle:
            return
        if self._action is not None:
            return
        self._action = MessageAction.nacked
        try:
            dlq_strategy = self._server._dlq_queue_strategy
            if dlq_strategy:
                await self._publish_to_dlq(dlq_strategy(self._channel))
            await self._delete_message()
        except Exception:
            self._action = None
            raise

    async def _publish_to_dlq(self, dlq_channel: str) -> None:
        # Callers validate the client first; re-check here so mypy narrowing
        # survives the method boundary.
        client = self._server._client
        if client is None:  # pragma: no cover
            raise ConnectionError("SQS client is not connected.")
        dlq_queue_url = await self._server._get_queue_url(dlq_channel)

        message_attributes: dict[str, Any] = {}
        for k, v in self._headers.items():
            message_attributes[k] = {"DataType": "String", "StringValue": v}
        if self._content_type:
            message_attributes["content-type"] = {
                "DataType": "String",
                "StringValue": self._content_type,
            }
        if self._is_empty_payload:
            message_attributes[EMPTY_PAYLOAD_ATTRIBUTE] = {
                "DataType": "String",
                "StringValue": EMPTY_PAYLOAD_ATTRIBUTE_VALUE,
            }

        await client.send_message(
            QueueUrl=dlq_queue_url,
            MessageBody=self._msg.get("Body", ""),
            MessageAttributes=message_attributes,
        )

    async def reject(self) -> None:
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        if not self._receipt_handle:
            return
        if self._action is not None:
            return
        self._action = MessageAction.rejected
        try:
            await self._change_visibility(0)
        except Exception:
            # Cancellation is not caught, so a cancelled settlement stays
            # reserved even if the RPC may have reached the server.
            self._action = None
            raise

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
