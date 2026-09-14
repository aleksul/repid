from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from itertools import groupby
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from repid.connections._subscriber import (
    AdmittedTaskTracker,
    SubscriberDispatcher,
    run_supervised,
)
from repid.connections.abc import (
    CapabilitiesT,
    MessageAction,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
)

logger = logging.getLogger("repid.connections.redis")


def _resumed_event() -> asyncio.Event:
    event = asyncio.Event()
    event.set()
    return event


if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.connections.abc import SubscriberT
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
        "_consumer_name",
        "_content_type",
        "_dlq_maxlen",
        "_dlq_stream",
        "_headers",
        "_keep_alive_interval",
        "_message_id",
        "_payload",
        "_redis",
        "_reply_to",
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
        consumer_name: str = "",
        min_idle_ms: int = 0,
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
        self._action: MessageAction | None = None
        self._consumer_name = consumer_name
        self._keep_alive_interval: int | None = min_idle_ms // 3000 if min_idle_ms > 0 else None

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

    @property
    def keep_alive_interval(self) -> int | None:
        return self._keep_alive_interval

    async def keep_alive(self) -> None:
        if self._action is not None:
            return
        await self._redis.xclaim(
            self._stream_name,
            self._consumer_group,
            self._consumer_name,
            min_idle_time=0,
            message_ids=[self._message_id],
        )

    async def ack(self) -> None:
        """Acknowledge the message - removes it from the pending entries list."""
        # Reserve the action before the RPC: in a single event loop the
        # check-and-set is atomic, so concurrent settlements are deduplicated,
        # and a settlement cancelled mid-RPC is never followed by a second one.
        if self._action is not None:
            return
        self._action = MessageAction.acked
        try:
            await self._redis.xack(self._stream_name, self._consumer_group, self._message_id)
        except Exception:
            # Cancellation is not caught, so a cancelled settlement stays
            # reserved even if the RPC may have reached the server.
            self._action = None
            raise

    async def nack(self) -> None:
        """Negative acknowledge - move to DLQ if configured, otherwise just ack and discard."""
        if self._action is not None:
            return
        self._action = MessageAction.nacked
        try:
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
        except Exception:
            self._action = None
            raise

    async def reject(self) -> None:
        """Reject the message — re-add it to the stream for reprocessing.

        Note: The re-added message receives a new stream ID and is appended to
        the end of the stream. Delivery order relative to other messages is
        not preserved.
        """
        if self._action is not None:
            return
        self._action = MessageAction.rejected
        try:
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
        except Exception:
            self._action = None
            raise

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


class RedisSubscriber:
    """Subscriber for Redis Streams using consumer groups."""

    def __init__(
        self,
        *,
        redis_client: Redis,
        channels: dict[str, ChannelConfig],
        callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        consumer_name: str,
        dispatcher: SubscriberDispatcher,
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
        self._block_ms = block_ms
        self._batch_size = batch_size
        self._retry_delay = retry_delay
        self._claim_interval = claim_interval
        self._min_idle_ms = min_idle_ms
        self._dispatcher = dispatcher

        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()  # Start in resumed state
        self._channel_paused_events = {channel: _resumed_event() for channel in channels}

        self._in_flight_messages: set[tuple[str, str]] = set()
        self._admitted_tasks = AdmittedTaskTracker()

        self._task: asyncio.Task[None] | None = None
        self._on_close: Callable[[], None] | None = None
        self._close_completion: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Schedule supervised consumption. Must be called inside a running event loop."""
        self._task = asyncio.create_task(self._supervised_loop())

    async def _supervised_loop(self) -> None:
        loops = [self._consume_loop()]
        if self._claim_interval > 0:
            loops.append(self._claim_loop())
        await run_supervised(*loops)

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
        for event in self._channel_paused_events.values():
            event.clear()

    async def resume(self) -> None:
        """Resume message consumption."""
        self._paused_event.set()
        for event in self._channel_paused_events.values():
            event.set()

    async def pause_channel(self, channel: str) -> None:
        """Stop reading one channel's stream; other channels keep flowing."""
        event = self._channel_paused_events.get(channel)
        if event is not None:
            event.clear()

    async def resume_channel(self, channel: str) -> None:
        event = self._channel_paused_events.get(channel)
        if event is not None:
            event.set()

    async def stop(self) -> None:
        if not self._closed:
            self._closed = True
            if self._task is not None and not self._task.done():
                self._task.cancel()

    async def finish(self) -> None:
        await self.stop()
        # Cancel remaining work before any suspension so callbacks cannot slip in.
        await self._admitted_tasks.cancel_and_drain(drain=False)
        if self._close_completion is None:
            self._close_completion = asyncio.create_task(self._finish_close())
        await asyncio.shield(self._close_completion)

    async def _finish_close(self) -> None:
        try:
            # Cancel whatever is still running (exactly once), then join intake
            # and drain retained callback cleanup.
            await self._admitted_tasks.cancel_and_drain()
            if self._task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await self._task
        finally:
            if self._on_close is not None:
                self._on_close()

    async def _consume_loop(self) -> None:
        """Main consumption loop — one concurrent task per unique consumer group."""
        sorted_channels = sorted(self._channels.items(), key=lambda item: item[1].group)
        groups: dict[str, dict[str, ChannelConfig]] = {}
        for group_name, items in groupby(sorted_channels, key=lambda item: item[1].group):
            groups[group_name] = dict(items)

        await run_supervised(
            *(
                self._consume_group_loop(group_name, channels)
                for group_name, channels in groups.items()
            ),
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
            except (
                ConnectionError,
                TimeoutError,
                RedisConnectionError,
                RedisTimeoutError,
                ResponseError,
            ) as exc:
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

        streams_dict = {
            cfg.stream: ">"
            for ch, cfg in group_channels.items()
            if self._channel_paused_events[ch].is_set()
        }
        if not streams_dict:
            # Every channel in this group is paused; idle briefly instead of
            # issuing an empty XREADGROUP.
            await asyncio.sleep(0.1)
            return
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

        handled_result = [
            (stream_name, messages)
            for stream_name, messages in result
            if (
                (decoded := stream_name.decode() if isinstance(stream_name, bytes) else stream_name)
                in stream_to_channel
                and stream_to_channel[decoded] in self._callbacks
            )
        ]
        prefetched = await self._prefetch_messages(handled_result, stream_to_channel)

        for stream_index, stream_data in enumerate(result):
            stream_name_raw, messages = stream_data
            stream_name = (
                stream_name_raw.decode() if isinstance(stream_name_raw, bytes) else stream_name_raw
            )
            channel = stream_to_channel.get(stream_name)
            callback = self._callbacks.get(channel) if channel is not None else None
            if channel is None or callback is None:
                continue
            try:
                await self._process_stream_messages(
                    messages,
                    channel,
                    stream_name,
                    callback,
                    prefetched,
                )
            except BaseException:
                for remaining_stream_name_raw, remaining_messages in result[stream_index + 1 :]:
                    remaining_stream_name = (
                        remaining_stream_name_raw.decode()
                        if isinstance(remaining_stream_name_raw, bytes)
                        else remaining_stream_name_raw
                    )
                    remaining_channel = stream_to_channel.get(remaining_stream_name)
                    if remaining_channel is not None:
                        await self._requeue_prefetched_messages(
                            remaining_messages,
                            remaining_channel,
                            prefetched,
                        )
                raise

    async def _prefetch_messages(
        self,
        result: list[Any],
        stream_to_channel: dict[str, str],
    ) -> dict[tuple[str, str], RedisReceivedMessage]:
        """Construct and renew every fetched delivery before intake may block."""
        prefetched: dict[tuple[str, str], RedisReceivedMessage] = {}
        malformed: list[tuple[str, str, str, dict[bytes | str, bytes | str]]] = []
        for stream_name_raw, messages in result:
            stream_name = (
                stream_name_raw.decode() if isinstance(stream_name_raw, bytes) else stream_name_raw
            )
            channel = stream_to_channel.get(stream_name)
            if channel is None:
                continue
            for msg_id_raw, fields in messages:
                msg_id = msg_id_raw.decode() if isinstance(msg_id_raw, bytes) else msg_id_raw
                key = (channel, msg_id)
                if key in self._in_flight_messages:
                    continue
                try:
                    message = self._create_received_message(channel, stream_name, msg_id, fields)
                except Exception as exc:
                    logger.exception(
                        "message.nack.parse_error",
                        extra={"message_id": msg_id, "channel": channel},
                        exc_info=exc,
                    )
                    malformed.append((channel, stream_name, msg_id, fields))
                    continue
                self._in_flight_messages.add(key)
                self._dispatcher.start_keep_alive(message)
                prefetched[key] = message

        try:
            for channel, stream_name, msg_id, fields in malformed:
                await self._nack_unparseable_message(channel, stream_name, msg_id, fields)
        except BaseException:
            for key, message in prefetched.items():
                await self._requeue_received_message(message, key)
            raise
        return prefetched

    async def _nack_unparseable_message(
        self,
        channel: str,
        stream_name: str,
        msg_id: str,
        fields: dict[bytes | str, bytes | str],
    ) -> None:
        """Dead-letter an unparseable delivery, or discard it when no DLQ exists."""
        cfg = self._channels[channel]
        async with self._redis.pipeline(transaction=True) as pipe:
            if cfg.dlq is not None:
                dlq_fields = dict(fields)
                dlq_fields[b"original_stream"] = stream_name
                dlq_fields[b"original_id"] = msg_id
                xadd_kwargs: dict[str, Any] = {}
                if cfg.dlq_maxlen is not None:
                    xadd_kwargs["maxlen"] = cfg.dlq_maxlen
                    xadd_kwargs["approximate"] = True
                pipe.xadd(cfg.dlq, dlq_fields, **xadd_kwargs)  # type: ignore[arg-type]
            pipe.xack(stream_name, cfg.group, msg_id)
            await pipe.execute()

    def _create_received_message(
        self,
        channel: str,
        stream_name: str,
        msg_id: str,
        fields: dict[bytes | str, bytes | str],
    ) -> RedisReceivedMessage:
        cfg = self._channels[channel]
        payload, headers, content_type, reply_to = _parse_message_fields(fields)
        return RedisReceivedMessage(
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
            consumer_name=self._consumer_name,
            min_idle_ms=self._min_idle_ms,
        )

    async def _requeue_received_message(
        self,
        message: RedisReceivedMessage,
        key: tuple[str, str],
    ) -> None:
        try:
            await self._dispatcher.stop_keep_alive(message)
            if not message.is_acted_on:
                await message.reject()
        except Exception as exc:
            logger.exception(
                "message.reject.error",
                extra={"message_id": message.message_id},
                exc_info=exc,
            )
        finally:
            self._in_flight_messages.discard(key)

    async def _requeue_prefetched_messages(
        self,
        messages: list[tuple[bytes | str, dict[bytes | str, bytes | str]]],
        channel: str,
        prefetched: dict[tuple[str, str], RedisReceivedMessage],
    ) -> None:
        for msg_id_raw, _ in messages:
            msg_id = msg_id_raw.decode() if isinstance(msg_id_raw, bytes) else msg_id_raw
            key = (channel, msg_id)
            if (message := prefetched.get(key)) is not None:
                await self._requeue_received_message(message, key)

    async def _process_stream_messages(
        self,
        messages: list[tuple[bytes | str, dict[bytes | str, bytes | str]]],
        channel: str,
        stream_name: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        prefetched: dict[tuple[str, str], RedisReceivedMessage] | None = None,
    ) -> None:
        """Process messages from a single stream."""
        if prefetched is None:
            prefetched = await self._prefetch_messages(
                [(stream_name, messages)],
                {stream_name: channel},
            )
        for index, (msg_id_raw, _) in enumerate(messages):
            msg_id = msg_id_raw.decode() if isinstance(msg_id_raw, bytes) else msg_id_raw
            key = (channel, msg_id)
            received_msg = prefetched.get(key)
            if received_msg is None:
                continue
            try:
                lease = await self._dispatcher.reserve(received_msg)
            except BaseException:
                await self._requeue_received_message(received_msg, key)
                await self._requeue_prefetched_messages(
                    messages[index + 1 :],
                    channel,
                    prefetched,
                )
                raise
            if lease is None:
                self._in_flight_messages.discard(key)
                continue
            self._admitted_tasks.start(
                self._dispatcher,
                lease,
                received_msg,
                partial(self._run_callback, callback, message_id=msg_id),
                on_cancel=partial(self._requeue_received_message, received_msg, key),
            )

    async def _run_callback(
        self,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        message: ReceivedMessageT,
        message_id: str,
    ) -> None:
        """Run a callback and clean up in-flight tracking."""
        try:
            await callback(message)
        except asyncio.CancelledError:
            if not message.is_acted_on:
                try:
                    await message.reject()
                except Exception as exc:
                    logger.exception(
                        "message.reject.error",
                        extra={"message_id": message_id},
                        exc_info=exc,
                    )
            raise
        except Exception as exc:
            logger.exception(
                "message.callback.error",
                extra={"message_id": message_id},
                exc_info=exc,
            )
            if not message.is_acted_on:
                await message.nack()
        finally:
            self._in_flight_messages.discard((message.channel, message_id))

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
                except (
                    ConnectionError,
                    TimeoutError,
                    RedisConnectionError,
                    RedisTimeoutError,
                    ResponseError,
                ) as exc:
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
            await self._paused_event.wait()
            if not self._channel_paused_events[channel].is_set():
                return
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
            "supports_native_reply": False,
            "supports_keep_alive": True,
            "supports_pause": True,
            "supports_pause_per_channel": True,
            "supports_native_message_flow_control": False,
            "supports_native_message_flow_control_per_channel": False,
            "supports_native_payload_flow_control": False,
            "supports_native_payload_flow_control_per_channel": False,
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

        redis = Redis.from_url(
            self._dsn,
            retry=retry,
            retry_on_error=[ConnectionError, TimeoutError],
            decode_responses=False,  # We handle decoding ourselves
        )
        try:
            await redis.ping()  # type: ignore[misc]
        except BaseException:
            try:
                await redis.aclose()
            except BaseException as cleanup_error:
                logger.exception("server.connect.cleanup_error", exc_info=cleanup_error)
            raise

        self._redis = redis

    async def disconnect(self) -> None:
        """Disconnect from Redis server."""
        logger.info("server.disconnect")

        subscribers = tuple(self._active_subscribers)
        self._active_subscribers.clear()

        for subscriber in subscribers:
            try:
                await subscriber.stop()
                await subscriber.finish()
            except Exception as exc:
                logger.exception("subscriber.close.error", exc_info=exc)

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
        dispatcher: SubscriberDispatcher,
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
            dispatcher=dispatcher,
            block_ms=self._block_ms,
            batch_size=self._batch_size,
            retry_delay=self._retry_delay,
            claim_interval=self._claim_interval,
            min_idle_ms=self._min_idle_ms,
        )
        subscriber.start()

        self._active_subscribers.append(subscriber)

        def _on_close() -> None:
            if subscriber in self._active_subscribers:
                self._active_subscribers.remove(subscriber)

        subscriber._on_close = _on_close

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
