from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from redis.asyncio import Redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ResponseError
from redis.retry import Retry

from repid.connections.abc import (
    CapabilitiesT,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
    SubscriberT,
)
from repid.logger import logger

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag


# Default naming strategies
def _default_stream_name_strategy(channel: str) -> str:
    return f"repid:{channel}"


def _default_consumer_group_strategy(channel: str) -> str:
    return f"repid:{channel}:group"


def _default_dlq_stream_strategy(channel: str) -> str:
    return f"repid:{channel}:dlq"


def _build_message_fields(
    payload: bytes,
    headers: dict[str, str] | None,
    content_type: str | None,
) -> dict[Any, Any]:
    """Build Redis stream message fields from payload, headers, and content type."""
    fields: dict[Any, Any] = {b"payload": payload}
    if headers:
        fields[b"headers"] = json.dumps(headers)
    if content_type:
        fields[b"content_type"] = content_type
    return fields


def _parse_message_fields(
    fields: dict[Any, Any],
) -> tuple[bytes, dict[str, str] | None, str | None]:
    """Parse Redis stream message fields into payload, headers, and content type."""
    payload = fields.get(b"payload", b"")
    if isinstance(payload, str):
        payload = payload.encode()

    headers: dict[str, str] | None = None
    headers_raw = fields.get(b"headers")
    if headers_raw:
        if isinstance(headers_raw, bytes):
            headers_raw = headers_raw.decode()
        headers = json.loads(headers_raw)

    content_type: str | None = None
    content_type_raw = fields.get(b"content_type")
    if content_type_raw:
        content_type = (
            content_type_raw.decode() if isinstance(content_type_raw, bytes) else content_type_raw
        )

    return payload, headers, content_type


class RedisSentMessage(SentMessageT):
    """A message to be sent to Redis Streams."""

    __slots__ = (
        "_content_type",
        "_headers",
        "_payload",
    )

    def __init__(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._content_type = content_type

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def headers(self) -> dict[str, str] | None:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._content_type


class RedisReceivedMessage(ReceivedMessageT):
    """A message received from Redis Streams."""

    __slots__ = (
        "_channel",
        "_consumer_group",
        "_content_type",
        "_dlq_stream",
        "_headers",
        "_is_acted_on",
        "_message_id",
        "_payload",
        "_redis",
        "_server",
        "_stream_name",
    )

    def __init__(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None,
        content_type: str | None,
        message_id: str,
        channel: str,
        stream_name: str,
        consumer_group: str,
        redis_client: Redis,
        dlq_stream: str | None,
        server: RedisServer,
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._content_type = content_type
        self._message_id = message_id
        self._channel = channel
        self._stream_name = stream_name
        self._consumer_group = consumer_group
        self._redis = redis_client
        self._dlq_stream = dlq_stream
        self._server = server
        self._is_acted_on = False

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
    def message_id(self) -> str | None:
        return self._message_id

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def is_acted_on(self) -> bool:
        return self._is_acted_on

    async def ack(self) -> None:
        """Acknowledge the message - removes it from the pending entries list."""
        if self._is_acted_on:
            return
        self._is_acted_on = True
        await self._redis.xack(self._stream_name, self._consumer_group, self._message_id)

    async def nack(self) -> None:
        """Negative acknowledge - move to DLQ if configured, otherwise just ack and discard."""
        if self._is_acted_on:
            return
        self._is_acted_on = True

        async with self._redis.pipeline(transaction=True) as pipe:
            if self._dlq_stream is not None:
                # Move message to DLQ stream with original metadata
                fields = _build_message_fields(self._payload, self._headers, self._content_type)
                fields[b"original_stream"] = self._stream_name
                fields[b"original_id"] = self._message_id
                pipe.xadd(self._dlq_stream, fields)

            # Acknowledge to remove from pending
            pipe.xack(self._stream_name, self._consumer_group, self._message_id)
            await pipe.execute()

    async def reject(self) -> None:
        """Reject the message - re-add it to the stream for reprocessing."""
        if self._is_acted_on:
            return
        self._is_acted_on = True

        async with self._redis.pipeline(transaction=True) as pipe:
            # Re-add message to stream (at the end, will be processed again)
            fields = _build_message_fields(self._payload, self._headers, self._content_type)
            pipe.xadd(self._stream_name, fields)
            # Then acknowledge original to remove from pending
            pipe.xack(self._stream_name, self._consumer_group, self._message_id)
            await pipe.execute()

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> None:
        """Acknowledge and reply to the message atomically."""
        if self._is_acted_on:
            return
        self._is_acted_on = True

        target_channel = channel or self._channel

        # Use pipeline for atomicity
        async with self._redis.pipeline(transaction=True) as pipe:
            # Ack original message
            pipe.xack(self._stream_name, self._consumer_group, self._message_id)

            # Publish reply
            reply_stream = self._server._stream_name_strategy(target_channel)
            reply_fields = _build_message_fields(payload, headers, content_type)
            pipe.xadd(reply_stream, reply_fields)
            await pipe.execute()


class RedisSubscriber(SubscriberT):
    """Subscriber for Redis Streams using consumer groups."""

    def __init__(
        self,
        *,
        redis_client: Redis,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        channels_to_streams: dict[str, str],
        channels_to_groups: dict[str, str],
        channels_to_dlq: dict[str, str | None],
        consumer_name: str,
        concurrency_limit: int | None,
        server: RedisServer,
        block_ms: int = 5000,
    ) -> None:
        self._redis = redis_client
        self._channels_to_callbacks = channels_to_callbacks
        self._channels_to_streams = channels_to_streams
        self._channels_to_groups = channels_to_groups
        self._channels_to_dlq = channels_to_dlq
        self._consumer_name = consumer_name
        self._server = server
        self._block_ms = block_ms

        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()  # Start in resumed state

        self._semaphore: asyncio.Semaphore | None = (
            asyncio.Semaphore(concurrency_limit)
            if concurrency_limit and concurrency_limit > 0
            else None
        )
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._in_flight_messages: set[str] = set()

        self._task = asyncio.create_task(self._consume_loop())

    @property
    def is_active(self) -> bool:
        return not self._closed and not self._task.done()

    @property
    def task(self) -> asyncio.Task[None]:
        return self._task

    async def pause(self) -> None:
        """Pause message consumption."""
        self._paused_event.clear()

    async def resume(self) -> None:
        """Resume message consumption."""
        self._paused_event.set()

    async def close(self) -> None:
        """Close the subscriber and wait for in-flight messages."""
        if self._closed:
            return
        self._closed = True

        # Cancel the main consume task
        if not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

        # Wait for callback tasks to complete
        if self._callback_tasks:
            await asyncio.gather(*self._callback_tasks, return_exceptions=True)

    async def _consume_loop(self) -> None:
        """Main consumption loop using XREADGROUP."""
        # Build streams dict for XREADGROUP: {stream_name: ">"}
        # ">" means only new messages not yet delivered to this consumer
        streams_dict = dict.fromkeys(self._channels_to_streams.values(), ">")

        # Reverse mapping: stream_name -> channel
        stream_to_channel = {v: k for k, v in self._channels_to_streams.items()}

        while not self._closed:
            try:
                await self._consume_batch(streams_dict, stream_to_channel)
            except asyncio.CancelledError:
                break
            except ResponseError as exc:
                logger.exception("Redis error in consumer loop", exc_info=exc)
                await asyncio.sleep(1)

    async def _consume_batch(
        self,
        streams_dict: dict[str, str],
        stream_to_channel: dict[str, str],
    ) -> None:
        """Consume a single batch of messages."""
        # Wait if paused
        await self._paused_event.wait()

        if self._closed:
            return

        # Read from all streams
        result = await self._redis.xreadgroup(
            groupname=next(iter(self._channels_to_groups.values())),
            consumername=self._consumer_name,
            streams=streams_dict,  # type: ignore[arg-type]
            count=10,  # Batch size
            block=self._block_ms,
        )

        if not result:
            return

        for stream_data in result:
            stream_name_raw, messages = stream_data
            stream_name = (
                stream_name_raw.decode() if isinstance(stream_name_raw, bytes) else stream_name_raw
            )

            channel = stream_to_channel.get(stream_name)
            if channel is None:
                continue

            callback = self._channels_to_callbacks.get(channel)
            if callback is None:
                continue

            await self._process_stream_messages(
                messages,
                channel,
                stream_name,
                callback,
            )

    async def _process_stream_messages(
        self,
        messages: list[tuple[bytes, dict[bytes, bytes]]],
        channel: str,
        stream_name: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        """Process messages from a single stream."""
        for msg_id_raw, fields in messages:
            msg_id = msg_id_raw.decode() if isinstance(msg_id_raw, bytes) else msg_id_raw
            self._in_flight_messages.add(msg_id)

            payload, headers, content_type = _parse_message_fields(fields)

            received_msg = RedisReceivedMessage(
                payload=payload,
                headers=headers,
                content_type=content_type,
                message_id=msg_id,
                channel=channel,
                stream_name=stream_name,
                consumer_group=self._channels_to_groups[channel],
                redis_client=self._redis,
                dlq_stream=self._channels_to_dlq.get(channel),
                server=self._server,
            )

            # Schedule callback with optional concurrency limiting
            if self._semaphore is not None:
                await self._semaphore.acquire()

            task = asyncio.create_task(
                self._run_callback(callback, received_msg, msg_id),
            )
            self._callback_tasks.add(task)
            task.add_done_callback(self._callback_tasks.discard)

    async def _run_callback(
        self,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        message: RedisReceivedMessage,
        message_id: str,
    ) -> None:
        """Run a callback and handle cleanup."""
        try:
            await callback(message)
        except ResponseError as exc:
            logger.exception(
                "Error processing message {message_id}",
                extra={"message_id": message_id},
                exc_info=exc,
            )
            # Auto-nack on unhandled exception if not already acted on
            if not message.is_acted_on:
                await message.nack()
        finally:
            self._in_flight_messages.discard(message_id)
            if self._semaphore is not None:
                self._semaphore.release()


class RedisServer(ServerT):
    """Redis Streams message broker server.

    Uses Redis Streams with consumer groups for reliable, persistent messaging.
    Built-in retry logic is handled by redis-py library.

    Args:
        dsn: Redis connection URL (e.g., 'redis://localhost:6379/0')
        stream_name_strategy: Callable to generate stream name from channel name.
            Default: 'repid:{channel}'
        consumer_group_strategy: Callable to generate consumer group name from channel.
            Default: 'repid:{channel}:group'
        dlq_stream_strategy: Callable to generate DLQ stream name from channel.
            If None, DLQ is disabled and nack() will just discard messages.
            Default: 'repid:{channel}:dlq'
        retry_attempts: Number of retry attempts for Redis operations.
        title: AsyncAPI server title.
        summary: AsyncAPI server summary.
        description: AsyncAPI server description.
        variables: AsyncAPI server variables.
        security: AsyncAPI security requirements.
        tags: AsyncAPI server tags.
        external_docs: AsyncAPI external documentation link.
        bindings: AsyncAPI server bindings.
    """

    def __init__(
        self,
        dsn: str,
        *,
        stream_name_strategy: Callable[[str], str] | None = None,
        consumer_group_strategy: Callable[[str], str] | None = None,
        dlq_stream_strategy: Callable[[str], str] | None = _default_dlq_stream_strategy,
        retry_attempts: int = 3,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        variables: Mapping[str, ServerVariable] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: ServerBindingsObject | None = None,
    ) -> None:
        self._dsn = dsn
        self._stream_name_strategy = stream_name_strategy or _default_stream_name_strategy
        self._consumer_group_strategy = consumer_group_strategy or _default_consumer_group_strategy
        self._dlq_stream_strategy = dlq_stream_strategy
        self._retry_attempts = retry_attempts

        # AsyncAPI metadata
        self._title = title
        self._summary = summary
        self._description = description
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings

        # Parse DSN for server info
        parsed = urlparse(dsn)
        self._host = f"{parsed.hostname}:{parsed.port}" if parsed.port else str(parsed.hostname)
        self._pathname = parsed.path if parsed.path and parsed.path != "/" else None

        # Redis client (created on connect)
        self._redis: Redis | None = None

        # Active subscribers
        self._active_subscribers: list[RedisSubscriber] = []

    # ServerT properties

    @property
    def host(self) -> str:
        return self._host

    @property
    def protocol(self) -> str:
        return "redis"

    @property
    def pathname(self) -> str | None:
        return self._pathname

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
        return "6.2"  # Minimum Redis version supporting XAUTOCLAIM etc.

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
            "supports_reply": True,
            "supports_lightweight_pause": True,
        }

    # Connection lifecycle

    @property
    def is_connected(self) -> bool:
        return self._redis is not None

    async def connect(self) -> None:
        """Connect to Redis server."""
        if self._redis is not None:
            return

        logger.info("Connecting to Redis server at '{host}'.", extra={"host": self._host})

        # Create retry configuration
        retry = Retry(ExponentialBackoff(), self._retry_attempts)

        self._redis = Redis.from_url(
            self._dsn,
            retry=retry,
            retry_on_error=[ConnectionError, TimeoutError],
            decode_responses=False,  # We handle decoding ourselves
        )

        # Verify connection
        await self._redis.ping()  # type: ignore[misc]

    async def disconnect(self) -> None:
        """Disconnect from Redis server."""
        logger.info("Disconnecting from Redis server.")

        # Close all subscribers
        for subscriber in self._active_subscribers:
            try:
                await subscriber.close()
            except ResponseError as exc:
                logger.exception("Error closing subscriber", exc_info=exc)

        self._active_subscribers.clear()

        # Close Redis connection
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[RedisServer, None]:
        """Context manager for connection lifecycle."""
        await self.connect()
        try:
            yield self
        finally:
            await self.disconnect()

    # Message publishing
    async def publish(
        self,
        *,
        channel: str,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Publish a message to a Redis Stream.

        Args:
            channel: Target channel name (will be mapped to stream via strategy)
            message: Message to publish
            server_specific_parameters: Optional Redis-specific parameters:
                - maxlen: Maximum stream length (int)
                - approximate: Use ~ for MAXLEN (bool, default True)
                - nomkstream: Don't create stream if it doesn't exist (bool)
                - stream_id: Custom stream entry ID (str, default "*")
        """
        if self._redis is None:
            raise ConnectionError("Not connected to Redis server")

        stream_name = self._stream_name_strategy(channel)
        params = server_specific_parameters or {}

        # Build message fields
        fields = _build_message_fields(message.payload, message.headers, message.content_type)

        # XADD parameters
        xadd_kwargs: dict[str, Any] = {}
        if "maxlen" in params:
            xadd_kwargs["maxlen"] = params["maxlen"]
            xadd_kwargs["approximate"] = params.get("approximate", True)
        if params.get("nomkstream"):
            xadd_kwargs["nomkstream"] = True

        stream_id = params.get("stream_id", "*")

        logger.debug(
            "Publishing message to stream '{stream}'.",
            extra={"stream": stream_name, "channel": channel},
        )

        await self._redis.xadd(stream_name, fields, id=stream_id, **xadd_kwargs)

    # Message subscription

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        """Subscribe to channels using Redis Streams consumer groups.

        Args:
            channels_to_callbacks: Mapping of channel names to callback coroutines
            concurrency_limit: Maximum concurrent message processing (None = unlimited)

        Returns:
            A RedisSubscriber instance
        """
        if self._redis is None:
            raise ConnectionError("Not connected to Redis server")

        logger.debug(
            "Subscribing to channels '{channels}'.",
            extra={"channels": list(channels_to_callbacks.keys())},
        )

        # Map channels to stream/group names
        channels_to_streams: dict[str, str] = {}
        channels_to_groups: dict[str, str] = {}
        channels_to_dlq: dict[str, str | None] = {}

        for channel in channels_to_callbacks:
            stream_name = self._stream_name_strategy(channel)
            group_name = self._consumer_group_strategy(channel)

            channels_to_streams[channel] = stream_name
            channels_to_groups[channel] = group_name

            if self._dlq_stream_strategy is not None:
                channels_to_dlq[channel] = self._dlq_stream_strategy(channel)
            else:
                channels_to_dlq[channel] = None

            # Create consumer group if it doesn't exist
            await self._ensure_consumer_group(stream_name, group_name)

        # Generate unique consumer name
        consumer_name = f"repid-{uuid.uuid4().hex[:8]}"

        subscriber = RedisSubscriber(
            redis_client=self._redis,
            channels_to_callbacks=channels_to_callbacks,
            channels_to_streams=channels_to_streams,
            channels_to_groups=channels_to_groups,
            channels_to_dlq=channels_to_dlq,
            consumer_name=consumer_name,
            concurrency_limit=concurrency_limit,
            server=self,
        )

        self._active_subscribers.append(subscriber)

        # Remove from list when task finishes
        def _on_done(_task: asyncio.Task[None]) -> None:
            if subscriber in self._active_subscribers:
                self._active_subscribers.remove(subscriber)

        subscriber.task.add_done_callback(_on_done)

        return subscriber

    async def _ensure_consumer_group(self, stream_name: str, group_name: str) -> None:
        """Create consumer group if it doesn't exist."""
        if self._redis is None:
            return

        try:
            await self._redis.xgroup_create(
                stream_name,
                group_name,
                id="0",  # Start from beginning
                mkstream=True,  # Create stream if doesn't exist
            )
            logger.debug(
                "Created consumer group '{group}' for stream '{stream}'.",
                extra={"group": group_name, "stream": stream_name},
            )
        except ResponseError as e:
            # BUSYGROUP means group already exists - that's fine
            if "BUSYGROUP" not in str(e):
                raise
