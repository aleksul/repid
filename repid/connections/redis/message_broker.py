from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import uuid
from collections import Counter
from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from functools import partial
from itertools import groupby
from typing import TYPE_CHECKING, Any, TypedDict, TypeVar, cast
from urllib.parse import urlparse

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from repid._concurrency import Concurrency, _channel_hints
from repid._keep_alive import _keep_message_alive
from repid.connections.abc import (
    CapabilitiesT,
    MessageAction,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
    SubscriberT,
)

logger = logging.getLogger("repid.connections.redis")

_StreamEntriesT = list[tuple[bytes | str, dict[bytes | str, bytes | str] | None]]
_StreamReadResultT = list[tuple[bytes | str, _StreamEntriesT]]
_ReadResultT = TypeVar("_ReadResultT")


class _PendingEntryT(TypedDict):
    message_id: bytes | str


if TYPE_CHECKING:
    from collections.abc import Awaitable, Collection, Generator

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
        "_consumer_name",
        "_content_type",
        "_dlq_maxlen",
        "_dlq_stream",
        "_headers",
        "_keep_alive_interval",
        "_message_id",
        "_on_settled",
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
        consumer_name: str = "",
        min_idle_ms: int = 0,
        on_settled: Callable[[], None] | None = None,
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
        self._keep_alive_during_settlement = False
        self._on_settled = on_settled
        self._consumer_name = consumer_name
        self._keep_alive_interval: float | None = min_idle_ms / 3000 if min_idle_ms > 0 else None

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
    def keep_alive_interval(self) -> float | None:
        return self._keep_alive_interval

    async def keep_alive(self) -> bool | None:
        if self._action is not None and not self._keep_alive_during_settlement:
            return False
        await self._redis.xclaim(
            self._stream_name,
            self._consumer_group,
            self._consumer_name,
            min_idle_time=0,
            message_ids=[self._message_id],
        )
        return None

    async def _settle(
        self,
        action: MessageAction,
        operation: Callable[[], Awaitable[None]],
    ) -> None:
        # Reserve the action before the RPC: in a single event loop the
        # check-and-set is atomic, so concurrent settlements are deduplicated,
        # and a settlement cancelled mid-RPC is never followed by a second one.
        if self._action is not None:
            return
        self._action = action
        self._keep_alive_during_settlement = True
        try:
            await operation()
            if self._on_settled is not None:
                self._on_settled()
        except Exception:
            # Cancellation is not caught, so a cancelled settlement stays
            # reserved even if the RPC may have reached the server.
            self._action = None
            raise
        finally:
            self._keep_alive_during_settlement = False

    async def ack(self) -> None:
        """Acknowledge the message - removes it from the pending entries list."""
        await self._settle(MessageAction.acked, self._ack_pending_entry)

    async def _ack_pending_entry(self) -> None:
        await self._redis.xack(self._stream_name, self._consumer_group, self._message_id)

    async def nack(self) -> None:
        """Negative acknowledge - move to DLQ if configured, otherwise just ack and discard."""
        await self._settle(MessageAction.nacked, self._discard_or_dead_letter)

    async def _discard_or_dead_letter(self) -> None:
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
                pipe.xadd(self._dlq_stream, fields, **xadd_kwargs)  # pyrefly: ignore[bad-argument-type]

            pipe.xack(self._stream_name, self._consumer_group, self._message_id)
            await pipe.execute()

    async def reject(self) -> None:
        """Reject the message — re-add it to the stream for reprocessing.

        Note: The re-added message receives a new stream ID and is appended to
        the end of the stream. Delivery order relative to other messages is
        not preserved.
        """
        await self._settle(MessageAction.rejected, self._requeue)

    async def _requeue(self) -> None:
        async with self._redis.pipeline(transaction=True) as pipe:
            fields = _build_message_fields(
                self._payload,
                self._headers,
                self._content_type,
                self._reply_to,
            )
            pipe.xadd(self._stream_name, fields)  # pyrefly: ignore[bad-argument-type]
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
        channel_concurrency: Mapping[str, Concurrency] | None = None,
        server: RedisServer,
        block_ms: int = 5000,
        batch_size: int = 10,
        retry_delay: float = 1.0,
        claim_interval: float = 0.0,
        min_idle_ms: int = 60_000,
        read_timeout: float = 5.0,
    ) -> None:
        if read_timeout <= 0 or not math.isfinite(read_timeout):
            raise ValueError("read_timeout must be positive and finite")
        self._redis = redis_client
        self._channels = channels
        self._callbacks = callbacks
        self._consumer_name = consumer_name
        self._server = server
        self._block_ms = block_ms
        self._retry_delay = retry_delay
        self._claim_interval = claim_interval
        self._min_idle_ms = min_idle_ms
        self._read_timeout = read_timeout

        self._closed = False
        self._close_event = asyncio.Event()
        self._paused_event = asyncio.Event()
        self._paused_event.set()  # Start in resumed state

        hints = channel_concurrency or {}
        self._intake_limits = {
            channel: min(batch_size, hint.messages or batch_size)
            if (hint := hints.get(channel)) is not None
            else batch_size
            for channel in channels
        }
        self._reclaim_cursors: dict[str, str] = {}
        self._callback_counts: Counter[str] = Counter()
        self._intake_requests: set[str] = set()
        self._intake_changed = {cfg.group: asyncio.Event() for cfg in channels.values()}
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._in_flight_messages: set[tuple[str, str]] = set()
        self._delivered_messages: set[tuple[str, str]] = set()
        self._uncertain_groups: set[str] = set()
        self._pending_cursors: dict[str, dict[str, str]] = {}
        self._notification_tasks: set[asyncio.Task[None]] = set()

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
        """Close after the outstanding read and in-flight callbacks finish."""
        if self._closed:
            return
        self._closed = True
        self._close_event.set()
        for changed in self._intake_changed.values():
            changed.set()
        self._paused_event.set()
        for task in self._notification_tasks:
            task.cancel()

        # Both XREADGROUP and XAUTOCLAIM can transfer ownership before their
        # replies arrive. Wait for them to finish or hit their RPC deadline.
        await asyncio.gather(
            *(task for task in (self._task, self._claim_task) if task is not None),
            return_exceptions=True,
        )

        await self._drain_callbacks()

        # A read may time out while close is draining it. Return any deliveries
        # already assigned by Redis, without starting actors during shutdown.
        for group in self._uncertain_groups.copy():
            channels = {ch: cfg for ch, cfg in self._channels.items() if cfg.group == group}
            self._pending_cursors.pop(group, None)
            try:
                while await self._recover_pending(group, channels):
                    await self._drain_callbacks()
            except (ConnectionError, TimeoutError, RedisConnectionError, RedisTimeoutError):
                logger.exception("consumer.recovery.error")

    async def _drain_callbacks(self) -> None:
        # A direct subscription callback may close its own subscriber.
        tasks = self._callback_tasks - {asyncio.current_task()}
        await asyncio.gather(*tasks, return_exceptions=True)

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
                await self._wait_for_retry()
            except Exception as exc:
                if self._closed:  # pragma: no cover
                    break
                logger.exception("consumer.error.unexpected", exc_info=exc)
                await self._wait_for_retry()

    async def _wait_for_retry(self) -> None:
        await self._wait_for_close(self._retry_delay)

    async def _wait_for_close(self, delay: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._close_event.wait(), timeout=delay)

    async def _consume_batch(
        self,
        group_name: str,
        group_channels: dict[str, ChannelConfig],
    ) -> None:
        """Consume a single batch of messages for one consumer group."""
        if self._closed or not self._paused_event.is_set():
            return

        recovered = group_name in self._uncertain_groups and await self._recover_pending(
            group_name,
            group_channels,
        )
        # Give other streams a chance to read between pages of pending entries.
        if await self._read_group(group_name, group_channels) or recovered:
            return
        await self._wait_for_messages(group_name, group_channels)

    def _available_channels(self, channels: dict[str, ChannelConfig]) -> dict[str, ChannelConfig]:
        # A saturated stream should not prevent other streams from reaching admission.
        return {ch: cfg for ch, cfg in channels.items() if self._available_slots(ch)}

    async def _read_group(self, group_name: str, channels: dict[str, ChannelConfig]) -> bool:
        if self._closed or not self._paused_event.is_set():
            return False
        available = self._available_channels(channels)
        if not available:
            return False

        streams = {cfg.stream: ">" for cfg in available.values()}
        stream_to_channel = {cfg.stream: ch for ch, cfg in available.items()}
        # Redis uses one COUNT for the read, applied to each included stream.
        count = min(self._available_slots(ch) for ch in available)
        with self._reserve_intake(available):
            result = cast(
                "_StreamReadResultT",
                await self._read_assignments(
                    group_name,
                    self._redis.xreadgroup(
                        groupname=group_name,
                        consumername=self._consumer_name,
                        streams=streams,  # pyrefly: ignore[bad-argument-type]
                        count=count,
                    ),
                ),
            )
            await self._dispatch_read_result(result, stream_to_channel)
        return bool(result)

    async def _dispatch_read_result(
        self,
        result: _StreamReadResultT,
        stream_to_channel: dict[str, str],
    ) -> None:
        for stream_name_raw, messages in result:
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

    async def _wait_for_messages(
        self,
        group_name: str,
        group_channels: dict[str, ChannelConfig],
    ) -> None:
        """Wait without claiming messages, so shutdown can safely cancel the wait."""
        if self._closed or not self._paused_event.is_set():
            return
        available = self._available_channels(group_channels)
        if not available:
            await self._intake_changed[group_name].wait()
            return
        if group_name in self._uncertain_groups:
            # An abandoned command can reach Redis after the first recovery
            # scan. Keep checking our own PEL even without new stream activity.
            await self._wait_for_retry()
            return
        streams = [cfg.stream for cfg in available.values()]
        watch_intake = self._claim_interval > 0 or len(available) < len(group_channels)
        intake_changed = self._intake_changed[group_name] if watch_intake else None
        tails = await asyncio.wait_for(
            asyncio.gather(*(self._redis.xrevrange(stream, count=1) for stream in streams)),
            timeout=self._read_timeout,
        )
        cursors = {
            stream: tail[0][0] if tail else "0-0"
            for stream, tail in zip(streams, tails, strict=True)
        }
        if self._closed or not self._paused_event.is_set():
            return
        # Messages may arrive before the cursors are captured. Check the group
        # again so those messages cannot be hidden behind the notification read.
        if (
            await self._read_group(group_name, group_channels)
            or self._closed
            or not self._paused_event.is_set()
        ):
            return
        await self._wait_for_notification(cursors, intake_changed)

    async def _wait_for_notification(
        self,
        cursors: dict[str, bytes | str],
        intake_changed: asyncio.Event | None,
    ) -> None:
        notification = asyncio.create_task(self._wait_for_activity(cursors))
        self._notification_tasks.add(notification)
        changed = asyncio.create_task(intake_changed.wait()) if intake_changed is not None else None
        try:
            if changed is not None:
                await asyncio.wait({notification, changed}, return_when=asyncio.FIRST_COMPLETED)
                if not notification.done():
                    return
            await notification
        except asyncio.CancelledError:
            if not self._closed:
                raise
        finally:
            self._notification_tasks.discard(notification)
            if changed is not None:
                notification.cancel()
                changed.cancel()
                await asyncio.gather(notification, changed, return_exceptions=True)

    async def _read_assignments(
        self,
        group_name: str,
        operation: Awaitable[_ReadResultT],
    ) -> _ReadResultT:
        # Bound the entire RPC, including redis-py's connection/retry handling.
        try:
            return await asyncio.wait_for(operation, timeout=self._read_timeout)
        except (
            asyncio.CancelledError,
            ConnectionError,
            TimeoutError,
            RedisConnectionError,
            RedisTimeoutError,
        ):
            self._uncertain_groups.add(group_name)
            self._pending_cursors.pop(group_name, None)
            raise

    async def _recover_pending(
        self,
        group_name: str,
        channels: dict[str, ChannelConfig],
    ) -> bool:
        cursors = self._pending_cursors.setdefault(
            group_name,
            dict.fromkeys((cfg.stream for cfg in channels.values()), "0-0"),
        )
        found = False
        scanned = 0
        for channel, cfg in channels.items():
            count = self._available_slots(channel)
            if not count:
                continue
            with self._reserve_intake((channel,)):
                last_id = await self._recover_stream(channel, cfg, cursors[cfg.stream], count)
            scanned += 1
            if last_id is not None:
                found = True
                cursors[cfg.stream] = last_id
        if not found and scanned == len(channels):
            self._pending_cursors.pop(group_name, None)
        return found

    async def _recover_stream(
        self,
        channel: str,
        cfg: ChannelConfig,
        cursor: str,
        count: int,
    ) -> str | None:
        # A settlement can complete before the metadata reply comes back.
        delivered = self._delivered_messages.copy()
        pending = cast(
            "list[_PendingEntryT]",
            await asyncio.wait_for(
                # XPENDING only inspects metadata. History XREADGROUP would refresh
                # every returned entry's idle time, including work we skip below.
                self._redis.xpending_range(
                    cfg.stream,
                    cfg.group,
                    min=f"({cursor}",
                    max="+",
                    count=count,
                    consumername=self._consumer_name,
                ),
                timeout=self._read_timeout,
            ),
        )
        if not pending:
            return None
        messages: _StreamEntriesT = []
        for entry in pending:
            message_id = entry["message_id"]
            msg_id = message_id.decode() if isinstance(message_id, bytes) else message_id
            if (channel, msg_id) in delivered:
                continue
            payload = cast(
                "_StreamEntriesT",
                await asyncio.wait_for(
                    self._redis.xrange(cfg.stream, min=message_id, max=message_id, count=1),
                    timeout=self._read_timeout,
                ),
            )
            messages.append((message_id, payload[0][1] if payload else None))
        await self._process_stream_messages(messages, channel, cfg.stream, self._callbacks[channel])
        last_id = pending[-1]["message_id"]
        return last_id.decode() if isinstance(last_id, bytes) else last_id

    async def _wait_for_activity(self, cursors: dict[str, bytes | str]) -> None:
        await self._redis.xread(streams=cursors, count=1, block=self._block_ms)  # pyrefly: ignore[bad-argument-type]

    async def _process_stream_messages(
        self,
        messages: list[tuple[bytes | str, dict[bytes | str, bytes | str] | None]],
        channel: str,
        stream_name: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        """Process messages from a single stream."""
        cfg = self._channels[channel]
        for msg_id_raw, fields in messages:
            msg_id = msg_id_raw.decode() if isinstance(msg_id_raw, bytes) else msg_id_raw
            key = (channel, msg_id)
            if key in self._in_flight_messages:
                continue
            if fields is None:
                # History can contain entries deleted while still pending.
                await asyncio.wait_for(
                    self._redis.xack(stream_name, cfg.group, msg_id),
                    timeout=self._read_timeout,
                )
                continue
            self._in_flight_messages.add(key)
            self._delivered_messages.add(key)
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
                consumer_name=self._consumer_name,
                min_idle_ms=self._min_idle_ms,
                on_settled=partial(self._delivered_messages.discard, key),
            )

            task = asyncio.create_task(
                self._run_callback(callback, received_msg, msg_id),
            )
            self._callback_tasks.add(task)
            self._callback_counts[channel] += 1
            task.add_done_callback(partial(self._intake_finished, channel))
            task.add_done_callback(self._callback_tasks.discard)

    def _available_slots(self, channel: str) -> int:
        if channel in self._intake_requests:
            return 0
        if self._closed:
            # Shutdown recovery only rejects deliveries. It must also work
            # when close() is being awaited by this stream's current callback.
            return self._intake_limits[channel]
        return max(0, self._intake_limits[channel] - self._callback_counts[channel])

    @contextmanager
    def _reserve_intake(
        self,
        channels: Collection[str],
        *,
        wake_readers: str | None = None,
    ) -> Generator[None, None, None]:
        # Each stream has one intake request at a time. Hold the reservation
        # until the response has been turned into tracked callback tasks.
        self._intake_requests.update(channels)
        try:
            yield
        finally:
            self._intake_requests.difference_update(channels)
            if wake_readers is not None:
                self._signal_intake_changed(wake_readers)

    def _signal_intake_changed(self, group: str) -> None:
        changed = self._intake_changed[group]
        self._intake_changed[group] = asyncio.Event()
        changed.set()

    def _intake_finished(self, channel: str, _task: asyncio.Task[None]) -> None:
        self._callback_counts[channel] -= 1
        if self._callback_counts[channel] == self._intake_limits[channel] - 1:
            self._signal_intake_changed(self._channels[channel].group)

    async def _run_callback(
        self,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        message: RedisReceivedMessage,
        message_id: str,
    ) -> None:
        """Run a callback and handle cleanup."""
        try:
            # A read can finish after pause(). Keep its lease alive while the
            # subscriber owns the delivery and the worker has not seen it yet.
            while self._closed or not self._paused_event.is_set():
                async with _keep_message_alive(
                    message,
                    message.keep_alive_interval,
                    self._server,
                ):
                    while not self._paused_event.is_set():
                        await self._paused_event.wait()
                    if self._closed:
                        await message.reject()
                        return
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
            self._in_flight_messages.discard((message.channel, message_id))

    async def _claim_loop(self) -> None:
        """Periodically reclaim stale pending messages using XAUTOCLAIM."""
        while not self._closed:
            await self._wait_for_close(self._claim_interval)
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
        start_id = self._reclaim_cursors.get(channel, "0-0")
        while (
            not self._closed
            and self._paused_event.is_set()
            and (count := self._available_slots(channel))
        ):
            with self._reserve_intake((channel,), wake_readers=cfg.group):
                result = await self._read_assignments(
                    cfg.group,
                    self._redis.xautoclaim(
                        cfg.stream,
                        cfg.group,
                        self._consumer_name,
                        min_idle_time=self._min_idle_ms,
                        start_id=start_id,
                        count=count,
                    ),
                )
                next_id, messages = result[:2]
                if messages:
                    await self._process_stream_messages(messages, channel, cfg.stream, callback)
            start_id = next_id.decode() if isinstance(next_id, bytes) else next_id
            self._reclaim_cursors[channel] = start_id
            if start_id == "0-0":
                break


class RedisServer(ServerT):
    """Redis Streams message broker server.

    Uses Redis Streams with consumer groups for reliable, persistent messaging.
    Built-in retry logic is handled by redis-py library.
    After an uncertain group read, the subscriber also polls its own pending
    deliveries on retry_delay, independently of optional stale-message reclaim.

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
        block_ms: Milliseconds to block on notification reads (XREAD) when no messages are available.
            Zero waits indefinitely until activity or close.
        read_timeout: Maximum seconds for each nonblocking read, reclaim, or pending-recovery request,
            including connection setup and retries. Must be positive and finite. Default: 5.0.
            This bounds reads drained on shutdown; notification waits still use block_ms.
        batch_size: Maximum number of messages to fetch per stream in a read or reclaim.
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
        read_timeout: float = 5.0,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        variables: Mapping[str, ServerVariable] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: ServerBindingsObject | None = None,
    ) -> None:
        if read_timeout <= 0 or not math.isfinite(read_timeout):
            raise ValueError("read_timeout must be positive and finite")
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
        self._read_timeout = read_timeout

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
            "supports_lightweight_pause": True,
            "supports_keep_alive": True,
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
            await redis.ping()  # pyrefly: ignore[not-async]
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
                await subscriber.close()
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

        await self._redis.xadd(stream_name, fields, id=stream_id, **xadd_kwargs)  # pyrefly: ignore[bad-argument-type]

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency: Concurrency,
        channel_concurrency: dict[str, Concurrency],
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
            channel_concurrency=_channel_hints(
                channels_to_callbacks,
                concurrency,
                channel_concurrency,
            ),
            server=self,
            block_ms=self._block_ms,
            batch_size=self._batch_size,
            retry_delay=self._retry_delay,
            claim_interval=self._claim_interval,
            min_idle_ms=self._min_idle_ms,
            read_timeout=self._read_timeout,
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
