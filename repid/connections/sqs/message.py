from __future__ import annotations

import asyncio
import base64
import binascii
import logging
from collections.abc import Callable, Coroutine, Mapping
from typing import TYPE_CHECKING, Any, TypeVar

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
_Result = TypeVar("_Result")


async def _finish_operation(operation: Coroutine[Any, Any, _Result]) -> _Result:
    task = asyncio.create_task(operation)
    try:
        # Wait without forwarding cancellation into the RPC. Python 3.14's
        # shield also reports late RPC failures to the event loop's handler.
        await asyncio.wait({task})
    except asyncio.CancelledError:
        await asyncio.gather(task, return_exceptions=True)
        raise
    return task.result()


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
        self._keep_alive_during_settlement = False
        self._lease_expired = False
        self._visibility_lock = asyncio.Lock()
        self._visibility_timeout = visibility_timeout
        self._keep_alive_interval: float | None = (
            visibility_timeout / 3 if visibility_timeout > 0 else None
        )

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
    def keep_alive_interval(self) -> float | None:
        return self._keep_alive_interval

    @staticmethod
    def _is_stale_receipt_error(exc: botocore.exceptions.ClientError) -> bool:
        error_dict = exc.response.get("Error")
        return (
            isinstance(error_dict, dict)
            and bool(error_dict)
            and error_dict.get("Code") in ("ReceiptHandleIsInvalid", "MessageNotInflight")
        )

    async def keep_alive(self) -> bool | None:
        async with self._visibility_lock:
            if self._visibility_timeout <= 0 or self._lease_expired:
                return False
            if self._action is not None and not self._keep_alive_during_settlement:
                return False
            if self._server._client is None:
                raise ConnectionError("SQS client is not connected.")
            if not self._receipt_handle:
                return False
            # Even if the timer is cancelled, a visibility extension must
            # finish before a waiting reject can set the timeout to zero.
            await _finish_operation(self._change_visibility(self._visibility_timeout))
            if self._lease_expired:
                return False
        return None

    async def _change_visibility(self, visibility_timeout: int) -> None:
        # Re-check the client and receipt handle after crossing the method boundary.
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
            self._lease_expired = True

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

    def _can_settle(self) -> bool:
        if self._server._client is None:
            raise ConnectionError("SQS client is not connected.")
        return self._action is None and bool(self._receipt_handle) and not self._lease_expired

    async def _settle(
        self,
        action: MessageAction,
        operation: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        # Reserve the action before I/O. Shutdown sees the reservation and
        # skips rejection, so carry the settlement through caller cancellation.
        self._action = action
        self._keep_alive_during_settlement = action is not MessageAction.rejected

        async def settle() -> None:
            try:
                await operation()
            except Exception:
                self._action = None
                raise
            finally:
                self._keep_alive_during_settlement = False

        await _finish_operation(settle())

    async def ack(self) -> None:
        if self._can_settle():
            await self._settle(MessageAction.acked, self._delete_message)

    async def nack(self) -> None:
        if self._can_settle():
            await self._settle(MessageAction.nacked, self._dead_letter)

    async def _dead_letter(self) -> None:
        dlq_strategy = self._server._dlq_queue_strategy
        if dlq_strategy:
            await self._publish_to_dlq(dlq_strategy(self._channel))
        await self._delete_message()

    async def _publish_to_dlq(self, dlq_channel: str) -> None:
        # Re-check the client after crossing the method boundary.
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
        if not self._can_settle():
            return
        async with self._visibility_lock:
            if self._can_settle():
                await self._settle(MessageAction.rejected, lambda: self._change_visibility(0))

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
