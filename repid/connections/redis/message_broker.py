from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from itertools import groupby
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from redis.asyncio import Redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ResponseError
from redis.retry import Retry

from repid.connections.abc import (
    CapabilitiesT,
    MessageAction,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
    SubscriberT,
)

logger = logging.getLogger("repid.connections.redis")

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag


def _default_stream_name_strategy(channel: str) -> str:
    return f"repid:{channel}"


def _default_consumer_group_strategy(channel: str) -> str:
    return f"repid:{channel}:group"


def _default_dlq_stream_strategy(channel: str) -> str:
    return f"repid:{channel}:dlq"


@dataclass(frozen=True)
class ChannelConfig:
    """Routing metadata for a single channel."""

    stream: str
    group: str
    dlq: str | None
    dlq_maxlen: int | None = None


def _build_message_fields(
    payload: bytes,
    headers: dict[str, str] | None,
    content_type: str | None,
    reply_to: str | None,
) -> dict[bytes, bytes | str]:
    """Build Redis stream message fields from payload, headers, and content type."""
    fields: dict[bytes, bytes | str] = {b"payload": payload}
    if headers:
        fields[b"headers"] = json.dumps(headers)
    if content_type:
        fields[b"content_type"] = content_type
    if reply_to:
        fields[b"reply_to"] = reply_to
    return fields


def _parse_message_fields(
    fields: dict[Any, Any],
) -> tuple[bytes, dict[str, str] | None, str | None, str | None]:
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

    reply_to: str | None = None
    reply_to_raw = fields.get(b"reply_to")
    if reply_to_raw:
        reply_to = reply_to_raw.decode() if isinstance(reply_to_raw, bytes) else reply_to_raw

    return payload, headers, content_type, reply_to


class RedisSentMessage(SentMessageT):
    """A message to be sent to Redis Streams."""

    __slots__ = (
        "_content_type",
        "_headers",
        "_payload",
        "_reply_to",
    )

    def __init__(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        reply_to: str | None = None,
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._content_type = content_type
        self._reply_to = reply_to

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
        return self._reply_to


class RedisReceivedMessage(ReceivedMessageT):
    """A message received from Redis Streams."""

    __slots__ = (
        "_action",
        "_channel",
        "_consumer_group",
        "_content_type",
        "_dlq_maxlen",
        "_dlq_stream",
        "_headers",
        "_message_id",
        "_payload",
        "_redis",
        "_reply_to",
        "_server",
        "_stream_name",
    )

    def __init__(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None,
        content_type: str | None,
        reply_to: str | None,
        message_id: str,
        channel: str,
        stream_name: str,
        consumer_group: str,
        redis_client: Redis,
        dlq_stream: str | None,
        dlq_maxlen: int | None = None,
        server: RedisServer,
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._content_type = content_type
        self._reply_to = reply_to
        self._message_id = message_id
        self._channel = channel
        self._stream_name = stream_name
        self._consumer_group = consumer_group
        self._redis = redis_client
        self._dlq_stream = dlq_stream
        self._dlq_maxlen = dlq_maxlen
        self._server = server
        self._action: MessageAction | None = None

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
        return self._reply_to

    @property
    def message_id(self) -> str | None:
        return self._message_id

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def is_acted_on(self) -> bool:
        return self._action is not None

    @property
    def action(self) -> MessageAction | None:
        return self._action

    async def ack(self) -> None:
        """Acknowledge the message - removes it from the pending entries list."""
        if self._action is not None:
            return
        self._action = MessageAction.acked
        await self._redis.xack(self._stream_name, self._consumer_group, self._message_id)

    async def nack(self) -> None:
        """Negative acknowledge - move to DLQ if configured, otherwise just ack and discard."""
        if self._action is not None:
            return
        self._action = MessageAction.nacked

        async with self._redis.pipeline(transaction=True) as pipe:
            if self._dlq_stream is not None:
                fields = _build_message_fields(
                    self._payload,
                    self._headers,
                    self._content_type,
                    self._reply_to,
                )
                fields[b"original_stream"] = self._stream_name
                fields[b"original_id"] = self._message_id
                xadd_kwargs: dict[str, Any] = {}
                if self._dlq_maxlen is not None:
                    xadd_kwargs["maxlen"] = self._dlq_maxlen
                    xadd_kwargs["approximate"] = True
                pipe.xadd(self._dlq_stream, fields, **xadd_kwargs)  # type: ignore[arg-type]

            pipe.xack(self._stream_name, self._consumer_group, self._message_id)
            await pipe.execute()

    async def reject(self) -> None:
        """Reject the message — re-add it to the stream for reprocessing.

        Note: The re-added message receives a new stream ID and is appended to
        the end of the stream. Delivery order relative to other messages is
        not preserved.
        """
        if self._action is not None:
            return
        self._action = MessageAction.rejected

        async with self._redis.pipeline(transaction=True) as pipe:
            fields = _build_message_fields(
                self._payload,
                self._headers,
                self._content_type,
                self._reply_to,
            )
            pipe.xadd(self._stream_name, fields)  # type: ignore[arg-type]
            pipe.xack(self._stream_name, self._consumer_group, self._message_id)
            await pipe.execute()

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
        _ = (payload, headers, content_type, channel, server_specific_parameters)
        raise NotImplementedError("Redis does not support native replies.")


class RedisSubscriber(SubscriberT):
    """Subscriber for Redis Streams using consumer groups."""

    def __init__(
        self,
        *,
        redis_client: Redis,
        channels: dict[str, ChannelConfig],
        callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        consumer_name: str,
        concurrency_limit: int | None,
        server: RedisServer,
        block_ms: int = 5000,
        batch_size: int = 10,
        retry_delay: float = 1.0,
        claim_interval: float = 0.0,
        min_idle_ms: int = 60_000,
    ) -> None:
        self._redis = redis_client
        self._channels = channels
        self._callbacks = callbacks
        self._consumer_name = consumer_name
        self._server = server
        self._block_ms = block_ms
        self._batch_size = batch_size
        self._retry_delay = retry_delay
        self._claim_interval = claim_interval
        self._min_idle_ms = min_idle_ms

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

        self._task: asyncio.Task[None] | None = None
        self._claim_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Schedule the consume loop. Must be called inside a running event loop."""
        self._task = asyncio.create_task(self._consume_loop())
        if self._claim_interval > 0:
            self._claim_task = asyncio.create_task(self._claim_loop())

    @property
    def is_active(self) -> bool:
        return not self._closed and self._task is not None and not self._task.done()

    @property
    def task(self) -> asyncio.Task[None]:
        if self._task is None:
            raise RuntimeError(  # pragma: no cover
                "RedisSubscriber has not been started; call start() first.",
            )
        return self._task

    @property
    def in_flight_count(self) -> int:
        """Number of messages currently being processed by callbacks."""
        return len(self._in_flight_messages)

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

        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

        if self._claim_task is not None and not self._claim_task.done():
            self._claim_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._claim_task

        if self._callback_tasks:
            await asyncio.gather(*self._callback_tasks, return_exceptions=True)

    async def _consume_loop(self) -> None:
        """Main consumption loop — one concurrent task per unique consumer group."""
        sorted_channels = sorted(self._channels.items(), key=lambda item: item[1].group)
        groups: dict[str, dict[str, ChannelConfig]] = {}
        for group_name, items in groupby(sorted_channels, key=lambda item: item[1].group):
            groups[group_name] = dict(items)

        await asyncio.gather(
            *(self._consume_group_loop(gn, gc) for gn, gc in groups.items()),
            return_exceptions=True,
        )

    async def _consume_group_loop(
        self,
        group_name: str,
        group_channels: dict[str, ChannelConfig],
    ) -> None:
        """Per-group consumption loop, runs concurrently alongside other groups."""
        while not self._closed:
            try:
                await self._paused_event.wait()
                await self._consume_batch(group_name, group_channels)
            except asyncio.CancelledError:
                break
            except (ConnectionError, TimeoutError, ResponseError) as exc:
                if self._closed:  # pragma: no cover
                    break
                logger.exception("consumer.error.redis", exc_info=exc)
                await asyncio.sleep(self._retry_delay)
            except Exception as exc:
                if self._closed:  # pragma: no cover
                    break
                logger.exception("consumer.error.unexpected", exc_info=exc)
                await asyncio.sleep(self._retry_delay)

    async def _consume_batch(
        self,
        group_name: str,
        group_channels: dict[str, ChannelConfig],
    ) -> None:
        """Consume a single batch of messages for one consumer group."""
        if self._closed:
            return

        streams_dict = {cfg.stream: ">" for cfg in group_channels.values()}
        stream_to_channel = {cfg.stream: ch for ch, cfg in group_channels.items()}

        result = await self._redis.xreadgroup(
            groupname=group_name,
            consumername=self._consumer_name,
            streams=streams_dict,  # type: ignore[arg-type]
            count=self._batch_size,
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

            callback = self._callbacks.get(channel)
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
        messages: list[tuple[bytes | str, dict[bytes | str, bytes | str]]],
        channel: str,
        stream_name: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        """Process messages from a single stream."""
        cfg = self._channels[channel]
        for msg_id_raw, fields in messages:
            msg_id = msg_id_raw.decode() if isinstance(msg_id_raw, bytes) else msg_id_raw
            self._in_flight_messages.add(msg_id)

            payload, headers, content_type, reply_to = _parse_message_fields(fields)

            received_msg = RedisReceivedMessage(
                payload=payload,
                headers=headers,
                content_type=content_type,
                reply_to=reply_to,
                message_id=msg_id,
                channel=channel,
                stream_name=stream_name,
                consumer_group=cfg.group,
                redis_client=self._redis,
                dlq_stream=cfg.dlq,
                dlq_maxlen=cfg.dlq_maxlen,
                server=self._server,
            )

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
        except Exception as exc:
            logger.exception(
                "message.callback.error",
                extra={"message_id": message_id},
                exc_info=exc,
            )
            if not message.is_acted_on:
                await message.nack()
        finally:
            self._in_flight_messages.discard(message_id)
            if self._semaphore is not None:
                self._semaphore.release()

    async def _claim_loop(self) -> None:
        """Periodically reclaim stale pending messages using XAUTOCLAIM."""
        while not self._closed:
            await asyncio.sleep(self._claim_interval)
            if self._closed:
                break
            for channel, cfg in self._channels.items():
                callback = self._callbacks.get(channel)
                if callback is None:
                    continue
                try:
                    await self._reclaim_pending(cfg, channel, callback)
                except (ConnectionError, TimeoutError, ResponseError) as exc:
                    logger.exception(
                        "consumer.reclaim.error",
                        exc_info=exc,
                    )

    async def _reclaim_pending(
        self,
        cfg: ChannelConfig,
        channel: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        """Reclaim stale pending messages for one channel using XAUTOCLAIM.

        Iterates through the entire PEL (Pending Entries List) in batches,
        transferring ownership of idle messages to this consumer and dispatching
        them to the callback for reprocessing.
        """
        start_id = "0-0"
        while not self._closed:
            result = await self._redis.xautoclaim(
                cfg.stream,
                cfg.group,
                self._consumer_name,
                min_idle_time=self._min_idle_ms,
                start_id=start_id,
                count=self._batch_size,
            )
            next_id = result[0]
            messages = result[1]
            if messages:
                await self._process_stream_messages(messages, channel, cfg.stream, callback)
            next_id_str = next_id.decode() if isinstance(next_id, bytes) else next_id
            if next_id_str == "0-0":
                break
            start_id = next_id_str


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
        block_ms: Milliseconds to block on XREADGROUP when no messages are available.
        batch_size: Maximum number of messages to fetch per XREADGROUP call.
        retry_delay: Seconds to wait after a recoverable error before retrying.
        consumer_group_start_id: Stream ID from which new consumer groups begin reading.
            Use '0' to replay all history (default) or '$' to consume only new messages.
        dlq_maxlen: Maximum number of entries kept in each DLQ stream (approximate trim).
            None (default) disables the limit.
        claim_interval: Seconds between XAUTOCLAIM passes that recover pending messages
            from crashed consumers. Set to 0.0 (default) to disable automatic reclaim.
        min_idle_ms: Minimum milliseconds a pending entry must be idle before it is
            eligible for reclaim. Only used when claim_interval > 0. Default: 60_000 (1 min).
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
        consumer_group_start_id: str = "0",
        dlq_maxlen: int | None = None,
        claim_interval: float = 0.0,
        min_idle_ms: int = 60_000,
        retry_attempts: int = 3,
        block_ms: int = 5000,
        batch_size: int = 10,
        retry_delay: float = 1.0,
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
        self._consumer_group_start_id = consumer_group_start_id
        self._dlq_maxlen = dlq_maxlen
        self._claim_interval = claim_interval
        self._min_idle_ms = min_idle_ms
        self._retry_attempts = retry_attempts
        self._block_ms = block_ms
        self._batch_size = batch_size
        self._retry_delay = retry_delay

        self._title = title
        self._summary = summary
        self._description = description
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings

        parsed = urlparse(dsn)
        self._host = f"{parsed.hostname}:{parsed.port}" if parsed.port else str(parsed.hostname)
        self._pathname = parsed.path if parsed.path and parsed.path != "/" else None

        self._redis: Redis | None = None
        self._active_subscribers: list[RedisSubscriber] = []

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
        return "6.2"  # Minimum Redis version supporting Streams consumer groups

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
            "supports_lightweight_pause": True,
        }

    def stream_name_for(self, channel: str) -> str:
        """Return the Redis stream name for the given channel."""
        return self._stream_name_strategy(channel)

    @property
    def is_connected(self) -> bool:
        return self._redis is not None

    async def connect(self) -> None:
        """Connect to Redis server."""
        if self._redis is not None:
            return

        logger.info("server.connect", extra={"host": self._host})

        retry = Retry(ExponentialBackoff(), self._retry_attempts)

        self._redis = Redis.from_url(
            self._dsn,
            retry=retry,
            retry_on_error=[ConnectionError, TimeoutError],
            decode_responses=False,  # We handle decoding ourselves
        )

        await self._redis.ping()  # type: ignore[misc]

    async def disconnect(self) -> None:
        """Disconnect from Redis server."""
        logger.info("server.disconnect")

        for subscriber in self._active_subscribers:
            try:
                await subscriber.close()
            except Exception as exc:
                logger.exception("subscriber.close.error", exc_info=exc)

        self._active_subscribers.clear()

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

    async def publish(
        self,
        *,
        channel: str,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Publish a message to a Redis Stream.

        server_specific_parameters keys:
            maxlen (int): Maximum stream length.
            approximate (bool): Use ~ for MAXLEN (default True).
            nomkstream (bool): Don't create stream if it doesn't exist.
            stream_id (str): Custom stream entry ID (default "*").
        """
        if self._redis is None:
            raise ConnectionError("Not connected to Redis server")

        stream_name = self._stream_name_strategy(channel)
        params = server_specific_parameters or {}

        fields = _build_message_fields(
            message.payload,
            message.headers,
            message.content_type,
            message.reply_to,
        )

        xadd_kwargs: dict[str, Any] = {}
        if "maxlen" in params:
            xadd_kwargs["maxlen"] = params["maxlen"]
            xadd_kwargs["approximate"] = params.get("approximate", True)
        if params.get("nomkstream"):
            xadd_kwargs["nomkstream"] = True

        stream_id = params.get("stream_id", "*")

        logger.debug(
            "channel.publish",
            extra={"stream": stream_name, "channel": channel},
        )

        await self._redis.xadd(stream_name, fields, id=stream_id, **xadd_kwargs)  # type: ignore[arg-type]

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        """Subscribe to channels using Redis Streams consumer groups."""
        if self._redis is None:
            raise ConnectionError("Not connected to Redis server")

        logger.debug(
            "channel.subscribe",
            extra={"channels": list(channels_to_callbacks.keys())},
        )

        channels: dict[str, ChannelConfig] = {}
        for channel in channels_to_callbacks:
            stream_name = self._stream_name_strategy(channel)
            group_name = self._consumer_group_strategy(channel)
            dlq = self._dlq_stream_strategy(channel) if self._dlq_stream_strategy else None

            channels[channel] = ChannelConfig(
                stream=stream_name,
                group=group_name,
                dlq=dlq,
                dlq_maxlen=self._dlq_maxlen,
            )
            await self._ensure_consumer_group(stream_name, group_name)

        consumer_name = f"repid-{uuid.uuid4().hex[:8]}"

        subscriber = RedisSubscriber(
            redis_client=self._redis,
            channels=channels,
            callbacks=channels_to_callbacks,
            consumer_name=consumer_name,
            concurrency_limit=concurrency_limit,
            server=self,
            block_ms=self._block_ms,
            batch_size=self._batch_size,
            retry_delay=self._retry_delay,
            claim_interval=self._claim_interval,
            min_idle_ms=self._min_idle_ms,
        )
        subscriber.start()

        self._active_subscribers.append(subscriber)

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
                id=self._consumer_group_start_id,
                mkstream=True,
            )
            logger.debug(
                "consumer_group.create",
                extra={"group": group_name, "stream": stream_name},
            )
        except ResponseError as e:
            # BUSYGROUP means the group already exists — that's fine
            if "BUSYGROUP" not in str(e):
                raise
