from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import nats
from nats.js.api import AckPolicy, ConsumerConfig

from repid.connections._buffer import SubmissionBuffer, stop_task
from repid.connections.abc import (
    CapabilitiesT,
    MessageAction,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
    SubscriberT,
    broker_capabilities,
    validate_native_flow,
)
from repid.limits import UNLIMITED_NATIVE_FLOW, NativeFlow

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.aio.msg import Msg
    from nats.js.client import JetStreamContext

    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag

logger = logging.getLogger("repid.connections.nats")


class NatsReceivedMessage(ReceivedMessageT):
    def __init__(
        self,
        msg: Msg,
        server: NatsServer,
        channel: str,
        ack_wait: float | None = None,
    ) -> None:
        self._msg = msg
        self._server = server
        self._channel = channel
        self._action: MessageAction | None = None
        self._keep_alive_interval: float | None = (
            ack_wait / 3 if ack_wait is not None and ack_wait > 0 else None
        )

    @property
    def payload(self) -> bytes:
        return self._msg.data

    @property
    def headers(self) -> dict[str, str] | None:
        if self._msg.headers:
            return dict(self._msg.headers)
        return None

    @property
    def content_type(self) -> str | None:
        if self._msg.headers:
            return self._msg.headers.get("content-type")
        return None

    @property
    def reply_to(self) -> str | None:
        reply = self._msg.reply
        if isinstance(reply, str) and reply and not reply.startswith("$JS.ACK"):
            return reply
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
        with suppress(Exception):
            return f"{self._msg.metadata.stream}:{self._msg.metadata.sequence.stream}"
        return None

    @property
    def keep_alive_interval(self) -> float | None:
        return self._keep_alive_interval

    async def keep_alive(self) -> None:
        # Settlements reserve _action before their first await, so a plain
        # check here is enough to keep a renewal from racing a settlement.
        if self._action is not None:
            return
        await self._msg.in_progress()

    async def ack(self) -> None:
        # Reserve the action before the RPC: in a single event loop the
        # check-and-set is atomic, so concurrent settlements are deduplicated,
        # and a settlement cancelled mid-RPC is never followed by a second one.
        if self._action is not None:
            return
        self._action = MessageAction.acked
        try:
            await self._msg.ack()
        except Exception:
            # Cancellation is not caught, so a cancelled settlement stays
            # reserved even if the RPC may have reached the server.
            self._action = None
            raise
        logger.debug("message.ack", extra={"channel": self._channel})

    async def nack(self) -> None:
        if self._action is not None:
            return
        self._action = MessageAction.nacked
        try:
            dlq = (
                self._server._dlq_topic_strategy(self._channel)
                if self._server._dlq_topic_strategy
                else None
            )

            if dlq is None:
                await self._msg.term()
            else:
                headers = self.headers or {}
                headers["x-repid-original-channel"] = self._channel
                if self._server._js is not None:
                    await self._server._js.publish(dlq, self.payload, headers=headers)
                    await self._msg.ack()
                elif self._server._nc is not None:
                    await self._server._nc.publish(dlq, self.payload, headers=headers)
                    await self._msg.ack()
                else:
                    await self._msg.nak()
                    raise ConnectionError(
                        "NATS connection is not initialized. Cannot publish to DLQ.",
                    )
        except Exception:
            self._action = None
            raise
        logger.debug("message.nack", extra={"channel": self._channel})

    async def reject(self) -> None:
        if self._action is not None:
            return
        self._action = MessageAction.rejected
        try:
            await self._msg.nak()
        except Exception:
            self._action = None
            raise
        logger.debug("message.reject", extra={"channel": self._channel})

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> None:
        if self._action is not None:
            return

        reply_channel = channel or self.reply_to
        if reply_channel is None:
            raise ValueError(
                "Reply channel is not set. Provide `channel` or publish with `reply_to`.",
            )

        reply_headers = dict(headers) if headers else {}
        if content_type:
            reply_headers["content-type"] = content_type

        self._action = MessageAction.replied
        try:
            # Reply then ack as one reserved unit: another settlement cannot
            # interleave, since _action is already reserved.
            if self._server._js is not None:
                await self._server._js.publish(reply_channel, payload, headers=reply_headers)
                await self._msg.ack()
            elif self._server._nc is not None:
                await self._server._nc.publish(reply_channel, payload, headers=reply_headers)
                await self._msg.ack()
            else:
                await self._msg.nak()
                raise ConnectionError("NATS connection is not initialized. Cannot send reply.")
        except Exception:
            self._action = None
            raise
        logger.debug("message.reply", extra={"channel": self._channel})


class NatsSubscriber(SubscriberT):
    """Push intake with a shared server window and bounded, renewed local buffers."""

    def __init__(
        self,
        server: NatsServer,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
    ) -> None:
        self._server = server
        self._native_flow = native_flow
        self._requested_flow = native_flow
        self._channels_to_callbacks = channels_to_callbacks
        self._subs: dict[str, JetStreamContext.PushSubscription] = {}
        self._buffers: dict[str, SubmissionBuffer] = {}
        self._queues: dict[str, asyncio.Queue[NatsReceivedMessage]] = {}
        self._configs: dict[str, tuple[str, ConsumerConfig]] = {}
        self._closed = False
        self._finished = False
        self._active = True
        self._paused_event = asyncio.Event()
        self._ready = asyncio.Event()
        self._control_lock = asyncio.Lock()
        self._failure: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._callback_error: BaseException | None = None
        self._channel_tasks: list[asyncio.Task] = []
        self._task = asyncio.create_task(self._start())

    @property
    def native_flow(self) -> NativeFlow:
        return self._native_flow

    @property
    def is_active(self) -> bool:
        return self._active and not self._closed and not self._task.done()

    @property
    def task(self) -> asyncio.Task:
        return self._task

    def _check_window(self, channel: str, config: ConsumerConfig) -> None:
        if not config.deliver_subject or config.deliver_group != f"{channel}_group":
            raise ValueError(
                "Existing NATS consumer must be a push consumer with the matching queue group",
            )
        window = self._requested_flow.channels.get(channel)
        requested = window.max_messages if window is not None else None
        actual = config.max_ack_pending
        ack_required = config.ack_policy in (AckPolicy.EXPLICIT, AckPolicy.ALL)
        if requested is not None and (actual != requested or not ack_required):
            if window is not None and window.messages_automatic:
                compatible = ack_required and actual is not None and 0 < actual <= requested
                self._native_flow = replace(
                    self._native_flow,
                    channels={
                        **self._native_flow.channels,
                        channel: replace(window, max_messages=actual if compatible else None),
                    },
                )
                if compatible:
                    return
                logger.info(
                    "worker.intake.resolved",
                    extra={
                        "scope": "channel",
                        "channel": channel,
                        "native_cap": None,
                        "consumer_max_ack_pending": actual,
                        "consumer_ack_policy": config.ack_policy,
                        "requested_native_cap": requested,
                        "fallback_reason": "existing shared NATS consumer window; local control retained",
                    },
                )
            else:
                raise ValueError(
                    f"Explicit native messages window for {channel!r} conflicts with shared "
                    f"NATS consumer MaxAckPending={actual}, ack_policy={config.ack_policy}; "
                    "configure the consumer before subscribing",
                )

    async def _start(self) -> None:
        try:
            js = self._server._js
            if self._channels_to_callbacks and js is None:
                raise ConnectionError("JetStream is not initialized")
            # Resolve every existing consumer before opening any delivery subscription.
            for channel in self._channels_to_callbacks:
                js = cast("JetStreamContext", js)
                stream = await js.find_stream_name_by_subject(channel)
                window = self._native_flow.channels.get(channel)
                requested = window.max_messages if window is not None else None
                try:
                    info = await js.consumer_info(stream, f"{channel}_group")
                except nats.js.errors.NotFoundError:
                    config = ConsumerConfig(max_ack_pending=requested or 1000, ack_wait=30)
                else:
                    config = info.config
                    self._check_window(channel, config)
                self._configs[channel] = (stream, config)
                shared = config.max_ack_pending
                capacity = min(
                    requested or 1000,
                    shared if shared is not None and shared > 0 else 1000,
                )
                self._queues[channel] = asyncio.Queue(maxsize=capacity)
                self._buffers[channel] = SubmissionBuffer()
            async with self._control_lock:
                await self._attach()
                self._paused_event.set()
                self._ready.set()
            for channel in self._channels_to_callbacks:
                self._channel_tasks.append(asyncio.create_task(self._dispatch(channel)))
            await asyncio.gather(*self._channel_tasks, self._failure)
        finally:
            self._ready.set()
            self._active = False
            for task in self._channel_tasks:
                task.cancel()
            await asyncio.gather(*self._channel_tasks, return_exceptions=True)
            self._failure.cancel()
            if not self._failure.cancelled():
                self._failure.exception()

    def _report_failure(self, error: BaseException) -> None:
        if not self._failure.done():
            self._failure.set_exception(error)

    def _renewal_finished(self, task: asyncio.Task) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self._report_failure(error)

    async def _receive(self, channel: str, raw: Msg) -> None:
        _, config = self._configs[channel]
        ack_wait = config.backoff[0] if config.backoff else config.ack_wait
        message = NatsReceivedMessage(raw, self._server, channel, ack_wait=ack_wait)
        try:
            if self._closed:
                await message.reject()
                return
            queue = self._queues[channel]
            if queue.full():
                # Automatic fallback or early acknowledgment may exceed the local buffer.
                # Return credit without discarding work or blocking the SDK callback loop.
                await message.reject()
                return
            buffer = self._buffers[channel]
            buffer.track((message,))
            renewal = buffer.owned[id(message)][1]
            if renewal is not None:
                renewal.add_done_callback(self._renewal_finished)
            queue.put_nowait(message)
        except Exception as exc:  # noqa: BLE001
            # nats-py swallows callback errors; surface them through the subscriber task.
            self._callback_error = exc
            self._report_failure(exc)

    async def _attach(self) -> None:
        js = cast("JetStreamContext", self._server._js)
        for channel, (stream, config) in self._configs.items():

            async def receive(raw: Msg, address: str = channel) -> None:
                await self._receive(address, raw)

            subscription = await js.subscribe(
                channel,
                stream=stream,
                queue=f"{channel}_group",
                durable=f"{channel}_group",
                config=config,
                cb=receive,
                manual_ack=True,
                # A read can contain the entire shared server window before the SDK
                # schedules callbacks. Do not size its raw queue to a smaller local cap.
                pending_msgs_limit=(
                    config.max_ack_pending
                    if config.max_ack_pending is not None and config.max_ack_pending > 0
                    else 1000
                ),
                pending_bytes_limit=64 * 1024 * 1024,
            )
            self._subs[channel] = subscription
            # subscribe() can bind a durable created concurrently. Verify its actual settings
            # before dispatcher admission; never overwrite shared consumer configuration.
            info = await subscription.consumer_info()
            self._check_window(channel, info.config)
            self._configs[channel] = (stream, info.config)

    async def _detach(self) -> None:
        subscriptions = list(self._subs.items())

        async def detach(channel: str, subscription: JetStreamContext.PushSubscription) -> None:
            # Drain nats-py's raw callback queue before releasing the subscription. Settlement
            # and in-progress requests use the shared connection, which stays open until finish.
            await subscription._sub.drain()
            await subscription.unsubscribe()
            self._subs.pop(channel)

        results = await asyncio.gather(
            *(detach(channel, sub) for channel, sub in subscriptions),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        if self._callback_error is not None:
            raise self._callback_error

    async def _dispatch(self, channel: str) -> None:
        while not self._closed:
            await self._paused_event.wait()
            message = await self._queues[channel].get()
            await self._paused_event.wait()
            await self._buffers[channel].submit(message, self._channels_to_callbacks[channel])

    async def pause(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("NATS supports worker pause only")
        await self._ready.wait()
        async with self._control_lock:
            self._paused_event.clear()
            await self._detach()

    async def resume(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("NATS supports worker pause only")
        await self._ready.wait()
        async with self._control_lock:
            if not self._closed and not self._paused_event.is_set():
                await self._attach()
                self._paused_event.set()

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._active = False
        try:
            await stop_task(self._task)
        finally:
            async with self._control_lock:
                try:
                    await self._detach()
                finally:
                    results = await asyncio.gather(
                        *(buffer.dispose() for buffer in self._buffers.values()),
                        return_exceptions=True,
                    )
                    for result in results:
                        if isinstance(result, BaseException):
                            raise result

    async def finish(self) -> None:
        try:
            await self.stop()
        finally:
            if not self._finished:
                async with self._control_lock:
                    await self._detach()
                self._configs.clear()
                self._buffers.clear()
                self._queues.clear()
                self._finished = True


class NatsServer(ServerT):
    def __init__(
        self,
        dsn: str,
        *,
        dlq_topic_strategy: Callable[[str], str] | None = lambda channel: f"repid_{channel}_dlq",
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        variables: Mapping[str, ServerVariable] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: ServerBindingsObject | None = None,
    ) -> None:
        self.dsn = dsn
        self._dlq_topic_strategy = dlq_topic_strategy
        self._nc: Client | None = None
        self._js: JetStreamContext | None = None

        self._title = title
        self._summary = summary
        self._description = description
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings
        self._active_subscribers: set[NatsSubscriber] = set()

    @property
    def host(self) -> str:
        parsed = urlparse(self.dsn)
        return f"{parsed.hostname}:{parsed.port}" if parsed.port else str(parsed.hostname)

    @property
    def protocol(self) -> str:
        return "nats"

    @property
    def pathname(self) -> str | None:
        return None

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
        return None

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
        capabilities = broker_capabilities(native_reply=True, keep_alive=True, worker_pause=True)
        capabilities["supports_channel_native_messages"] = True
        return capabilities

    @property
    def is_connected(self) -> bool:
        return self._nc is not None and self._nc.is_connected

    async def connect(self) -> None:
        if self.is_connected:
            return

        # Drop a stale, no-longer-connected client before reconnecting
        if self._nc is not None:
            with suppress(Exception):
                await self._nc.close()
            self._nc = None
            self._js = None

        self._nc = await nats.connect(self.dsn, error_cb=self._intake_error)
        self._js = self._nc.jetstream()
        logger.info("server.connect", extra={"host": self.host})

    async def _intake_error(self, error: Exception) -> None:
        if isinstance(error, nats.errors.SlowConsumerError):
            for subscriber in self._active_subscribers:
                subscriber._report_failure(error)
        logger.error("server.intake.error", extra={"error_type": type(error).__name__})

    async def disconnect(self) -> None:
        for sub in list(self._active_subscribers):
            with suppress(Exception):  # pragma: no cover
                await sub.finish()
        self._active_subscribers.clear()

        if self._nc is not None:
            await self._nc.close()
            self._nc = None
            self._js = None
            logger.info("server.disconnect", extra={"host": self.host})

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[ServerT]:
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
        if not self.is_connected:
            raise ConnectionError("NATS connection is not initialized. Call connect() first.")

        headers = dict(message.headers) if message.headers else {}
        if message.content_type:
            headers["content-type"] = message.content_type
        publish_params = dict(server_specific_parameters or {})
        reply_candidate = publish_params.pop("reply", None)
        reply_to = (
            reply_candidate
            if isinstance(reply_candidate, str)
            else (message.reply_to if isinstance(message.reply_to, str) else None)
        )

        if self._js is not None:
            await self._js.publish(channel, message.payload, headers=headers)
            logger.debug("channel.publish", extra={"channel": channel})
        elif self._nc is not None:
            if reply_to is None:
                await self._nc.publish(channel, message.payload, headers=headers)
            else:
                await self._nc.publish(
                    channel,
                    message.payload,
                    reply=cast(str, reply_to),
                    headers=headers,
                )
            logger.debug("channel.publish.core", extra={"channel": channel})
        else:
            raise ConnectionError("NATS connection is not initialized. Cannot publish message.")

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
    ) -> SubscriberT:
        validate_native_flow(native_flow, self.capabilities)
        if not self.is_connected or self._js is None:
            raise ConnectionError("NATS connection is not initialized. Call connect() first.")

        subscriber = NatsSubscriber(
            server=self,
            channels_to_callbacks=channels_to_callbacks,
            native_flow=native_flow,
        )
        self._active_subscribers.add(subscriber)
        subscriber.task.add_done_callback(lambda _: self._active_subscribers.discard(subscriber))
        return subscriber
