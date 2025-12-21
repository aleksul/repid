from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime
from types import TracebackType
from typing import TYPE_CHECKING, Any, overload
from uuid import uuid4

from typing_extensions import Self

from repid._runner import _actor_run
from repid.connections.abc import ServerT
from repid.data import MessageData
from repid.message_registry import MessageRegistry

if TYPE_CHECKING:
    from repid.main import Repid
    from repid.serializer import SerializerT


class _MockServer:
    """Mock server for test client that implements minimal ServerT protocol."""

    def __init__(self, test_client: TestClient) -> None:
        self._test_client = test_client

    async def publish(
        self,
        *,
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> None:
        """Publish a message to a channel by queuing it in the test client."""
        test_message = TestMessage(
            operation_id=None,
            payload=message.payload,
            headers=message.headers,
            content_type=message.content_type,
            channel=channel,
        )
        self._test_client._sent_messages.append(test_message)
        await self._test_client._message_queue.put((channel, test_message))


class TestMessage:
    """
    Test message that tracks acknowledgment state and processing results.

    This is used internally by TestClient to track message
    lifecycle and enable assertions on message handling.

    Implements the ReceivedMessageT protocol.
    """

    __test__ = False  # prevent pytest from collecting this class

    __slots__ = (
        "_channel",
        "_content_type",
        "_exception",
        "_headers",
        "_is_acked",
        "_is_nacked",
        "_is_rejected",
        "_message_id",
        "_operation_id",
        "_payload",
        "_processed",
        "_reply_messages",
        "_result",
        "_timestamp",
    )

    def __init__(
        self,
        operation_id: str | None,
        payload: bytes,
        headers: dict[str, str] | None,
        content_type: str | None,
        channel: str,
    ) -> None:
        self._operation_id = operation_id
        self._payload = payload
        self._headers = headers
        self._content_type = content_type
        self._channel = channel
        self._message_id = str(uuid4())
        self._timestamp = datetime.now()
        self._is_acked = False
        self._is_nacked = False
        self._is_rejected = False
        self._reply_messages: list[tuple[str, MessageData]] = []
        self._exception: Exception | None = None
        self._result: Any = None
        self._processed = False

    @property
    def operation_id(self) -> str | None:
        """Operation ID for this message."""
        return self._operation_id

    @property
    def payload(self) -> bytes:
        """Message payload."""
        return self._payload

    @property
    def headers(self) -> dict[str, str] | None:
        """Message headers."""
        return self._headers

    @property
    def content_type(self) -> str | None:
        """Content type of the payload."""
        return self._content_type

    @property
    def channel(self) -> str:
        """Channel the message was received on."""
        return self._channel

    @property
    def message_id(self) -> str:
        """Unique identifier for this message."""
        return self._message_id

    @property
    def timestamp(self) -> datetime:
        """When the message was created/sent."""
        return self._timestamp

    @property
    def acked(self) -> bool:
        """Whether the message was acknowledged."""
        return self._is_acked

    @property
    def nacked(self) -> bool:
        """Whether the message was negatively acknowledged."""
        return self._is_nacked

    @property
    def rejected(self) -> bool:
        """Whether the message was rejected."""
        return self._is_rejected

    @property
    def success(self) -> bool:
        """Whether processing succeeded (no exception)."""
        return self._exception is None and self._processed

    @property
    def exception(self) -> Exception | None:
        """Exception raised during processing, if any."""
        return self._exception

    @property
    def result(self) -> Any:
        """Result returned by the actor."""
        return self._result

    @property
    def is_acted_on(self) -> bool:
        """Check if message has been acknowledged, nacked, or rejected."""
        return self._is_acked or self._is_nacked or self._is_rejected

    async def ack(self) -> None:
        """Acknowledge the message."""
        self._is_acked = True

    async def nack(self) -> None:
        """Negative acknowledge the message."""
        self._is_nacked = True

    async def reject(self) -> None:
        """Reject the message."""
        self._is_rejected = True

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> None:
        """Reply to the message with a new message."""
        await self.ack()
        reply_channel = channel if channel is not None else self._channel
        message = MessageData(
            payload=payload,
            headers=headers,
            content_type=content_type,
        )
        self._reply_messages.append((reply_channel, message))


class TestClient:
    """
    Test client for Repid applications.

    Mimics the Repid app interface (send_message, send_message_json, messages property)
    making it a drop-in replacement for testing.
    """

    __test__ = False  # prevent pytest from collecting this class

    def __init__(
        self,
        app: Repid,
        *,
        auto_process: bool = True,
        raise_on_actor_not_found: bool = True,
        raise_on_actor_error: bool = True,
        raise_on_operation_not_found: bool = True,
    ) -> None:
        """Initialize the test client.

        Args:
            app (Repid): The Repid application instance to test
            auto_process (bool, optional): Whether to automatically process messages when sent. Defaults to True.
            raise_on_actor_not_found (bool, optional): Whether to raise error when no actor found for message. Defaults to True.
            raise_on_actor_error (bool, optional): Whether to raise exceptions from actor execution. Defaults to True.
            raise_on_operation_not_found (bool, optional): Whether to raise error when operation ID is not found. Defaults to True.
        """
        self.app = app
        self.auto_process = auto_process
        self.raise_on_actor_not_found = raise_on_actor_not_found
        self.raise_on_actor_error = raise_on_actor_error
        self.raise_on_operation_not_found = raise_on_operation_not_found

        self._sent_messages: list[TestMessage] = []
        self._processed_messages: list[TestMessage] = []
        self._message_queue: asyncio.Queue[tuple[str, TestMessage]] = asyncio.Queue()
        self._mock_server: ServerT = _MockServer(self)  # type: ignore[assignment]
        self._producer_middleware_pipeline = app._producer_middleware_pipeline

    @property
    def messages(self) -> MessageRegistry:
        """Access to the message registry (same as Repid app)."""
        return self.app.messages

    async def _send_message(
        self,
        *,
        channel: str | None = None,
        operation_id: str | None = None,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,  # noqa: ARG002
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Internal method to send a message."""
        if channel is None and operation_id is None:
            raise ValueError("Either 'channel' or 'operation_id' must be specified.")
        if channel is not None and operation_id is not None:
            raise ValueError("Specify either 'channel' or 'operation_id', not both.")

        # Get channel from operation_id if needed
        if operation_id is not None:
            operation = self.app.messages.get_operation(operation_id)
            if operation is None:
                if self.raise_on_operation_not_found:
                    raise ValueError(f"Operation '{operation_id}' not found.")
                return
            channel = operation.channel.address

        async def _publish(
            channel: str,
            message: MessageData,
            server_specific_parameters: dict[str, Any] | None,  # noqa: ARG001
        ) -> None:
            # Create and track message
            test_message = TestMessage(
                operation_id=operation_id,
                payload=message.payload,
                headers=message.headers,
                content_type=message.content_type,
                channel=channel,
            )
            self._sent_messages.append(test_message)

            if self.auto_process:
                await self._process_message(test_message)
            else:
                await self._message_queue.put((channel, test_message))

        await self._producer_middleware_pipeline(_publish)(
            channel,  # type: ignore[arg-type]
            MessageData(
                payload=payload,
                headers=headers,
                content_type=content_type,
            ),
            server_specific_parameters,
        )

    @overload
    async def send_message(
        self,
        *,
        operation_id: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    @overload
    async def send_message(
        self,
        *,
        channel: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    async def send_message(
        self,
        *,
        channel: str | None = None,
        operation_id: str | None = None,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Send a raw message (mimics Repid.send_message).

        Messages are queued for processing instead of being sent to a real server.

        Args:
            channel (str | None, optional): The channel to send to. Mutually exclusive with operation_id.
            operation_id (str | None, optional): The operation ID to send to. Mutually exclusive with channel.
            payload (bytes): Message payload
            headers (dict[str, str] | None, optional): Optional message headers. Defaults to None.
            content_type (str | None, optional): Content type of the payload. Defaults to None.
            server_name (str | None, optional): Has no effect in test client, included for interface compatibility.
            server_specific_parameters (dict[str, Any] | None, optional): Has no effect in test client, included for interface compatibility.

        Raises:
            ValueError: If neither or both channel and operation_id are specified, or if the operation ID is not found.
        """
        await self._send_message(
            channel=channel,
            operation_id=operation_id,
            payload=payload,
            headers=headers,
            content_type=content_type,
            server_name=server_name,
            server_specific_parameters=server_specific_parameters,
        )

    @overload
    async def send_message_json(
        self,
        *,
        operation_id: str,
        payload: Any,
        headers: dict[str, str] | None = None,
        serializer: SerializerT | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    @overload
    async def send_message_json(
        self,
        *,
        channel: str,
        payload: Any,
        headers: dict[str, str] | None = None,
        serializer: SerializerT | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    async def send_message_json(
        self,
        *,
        channel: str | None = None,
        operation_id: str | None = None,
        payload: Any,
        headers: dict[str, str] | None = None,
        serializer: SerializerT | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Send a JSON message (mimics Repid.send_message_json).

        Messages are queued for processing instead of being sent to a real server.

        Args:
            channel (str | None, optional): The channel to send to. Mutually exclusive with operation_id.
            operation_id (str | None, optional): The operation ID to send to. Mutually exclusive with channel.
            payload (Any): JSON message payload
            headers (dict[str, str] | None, optional): Optional message headers. Defaults to None.
            serializer (SerializerT | None, optional): Serializer to use for the payload. Defaults to None, which will use the app's default serializer.
            server_name (str | None, optional): Has no effect in test client, included for interface compatibility.
            server_specific_parameters (dict[str, Any] | None, optional): Has no effect in test client, included for interface compatibility.

        Raises:
            ValueError: If neither or both channel and operation_id are specified, or if the operation ID is not found.
        """
        serializer = serializer if serializer is not None else self.app.default_serializer
        await self._send_message(
            channel=channel,
            operation_id=operation_id,
            payload=serializer(payload),
            headers=headers,
            content_type="application/json",
            server_name=server_name,
            server_specific_parameters=server_specific_parameters,
        )

    async def _process_message(self, test_message: TestMessage) -> TestMessage:
        # Find actors for this channel
        actors = self.app._centralized_router._actors_per_channel_address.get(
            test_message.channel,
            [],
        )
        actor = next(filter(lambda actor: actor.routing_strategy(test_message), actors), None)
        if actor is None:
            if self.raise_on_actor_not_found:
                raise ValueError(f"No actor found for channel '{test_message.channel}'")
            return test_message

        # _actor_run returns either the result or the exception
        actor_result = await _actor_run(
            actor,
            test_message,
            self._mock_server,
            self.app.default_serializer,
        )

        if isinstance(actor_result, Exception):
            test_message._exception = actor_result
        else:
            test_message._result = actor_result

        test_message._processed = True

        # Queue any reply messages for processing
        for reply_channel, reply_message in test_message._reply_messages:
            # Find operation ID for the reply channel
            reply_operation_id = None
            for op_id, operation in self.app.messages._operations.items():
                if operation.channel.address == reply_channel:
                    reply_operation_id = op_id
                    break

            # Create and track reply message
            reply_test_message = TestMessage(
                operation_id=reply_operation_id,
                payload=reply_message.payload,
                headers=reply_message.headers,
                content_type=reply_message.content_type,
                channel=reply_channel,
            )
            self._sent_messages.append(reply_test_message)
            await self._message_queue.put((reply_channel, reply_test_message))

        # Add to processed messages
        self._processed_messages.append(test_message)

        # Raise exception after recording if configured to do so
        if test_message._exception is not None and self.raise_on_actor_error:
            raise test_message._exception

        return test_message

    async def process_next(self) -> TestMessage | None:
        """Process the next message from the queue.

        Returns:
            TestMessage | None: The processed message or None if no message was processed.
        """
        try:
            _, message = self._message_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

        return await self._process_message(message)

    async def process_all(self) -> list[TestMessage]:
        """Process all messages in the queue.

        Returns:
            list[TestMessage]: The list of processed messages.
        """
        messages = []
        with suppress(asyncio.QueueEmpty):
            while not self._message_queue.empty():
                _, message = self._message_queue.get_nowait()
                messages.append(await self._process_message(message))
        return messages

    @overload
    def get_sent_messages(self, *, operation_id: str) -> list[TestMessage]: ...

    @overload
    def get_sent_messages(self, *, channel: str) -> list[TestMessage]: ...

    @overload
    def get_sent_messages(self) -> list[TestMessage]: ...

    def get_sent_messages(
        self,
        *,
        operation_id: str | None = None,
        channel: str | None = None,
    ) -> list[TestMessage]:
        """Get all messages sent during testing.

        Args:
            operation_id (str | None, optional): Filter by operation ID. Defaults to None.
            channel (str | None, optional): Filter by channel. Defaults to None.

        Returns:
            list[TestMessage]: The list of sent messages.
        """
        if operation_id is not None:
            return [m for m in self._sent_messages if m.operation_id == operation_id]
        if channel is not None:
            return [m for m in self._sent_messages if m.channel == channel]
        return self._sent_messages.copy()

    @overload
    def get_processed_messages(self, *, operation_id: str) -> list[TestMessage]: ...

    @overload
    def get_processed_messages(self, *, channel: str) -> list[TestMessage]: ...

    @overload
    def get_processed_messages(self) -> list[TestMessage]: ...

    def get_processed_messages(
        self,
        *,
        operation_id: str | None = None,
        channel: str | None = None,
    ) -> list[TestMessage]:
        """Get all messages that have been processed.

        Args:
            operation_id (str | None, optional): Filter by operation ID. Defaults to None.
            channel (str | None, optional): Filter by channel. Defaults to None.

        Returns:
            list[TestMessage]: The list of processed messages.
        """
        if operation_id is not None:
            return [m for m in self._processed_messages if m.operation_id == operation_id]
        if channel is not None:
            return [m for m in self._processed_messages if m.channel == channel]
        return self._processed_messages.copy()

    def clear(self) -> None:
        """Clear state of the test client (sent and processed messages)"""
        self._sent_messages.clear()
        self._processed_messages.clear()
        with suppress(asyncio.QueueEmpty):
            while not self._message_queue.empty():  # Clear the queue
                self._message_queue.get_nowait()

    async def __aenter__(self) -> Self:
        """Start the test client."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop the test client and cleanup."""
        self.clear()
