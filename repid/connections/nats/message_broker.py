from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from functools import partial
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import nats
from nats.errors import NotJSMessageError
from nats.js.api import ConsumerConfig, ConsumerInfo
from nats.js.errors import APIError, NotFoundError
from nats.js.errors import Error as JetStreamError

from repid.connections.abc import (
    CapabilitiesT,
    MessageAction,
    MessageCallbackT,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
    SubscriberT,
)

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription
    from nats.js.client import JetStreamContext

    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag
    from repid.data.concurrency import Concurrency

logger = logging.getLogger("repid.connections.nats")

# JetStream reports these if another worker creates or binds a durable first.
_DURABLE_CREATE_CONFLICTS = {10013, 10105, 10148}


def _delivery_ack_wait(config: ConsumerConfig, msg: Msg) -> float | None:
    if config.backoff:
        with suppress(NotJSMessageError):
            retry = msg.metadata.num_delivered - 1
            return config.backoff[min(retry, len(config.backoff) - 1)]
    return config.ack_wait


async def _max_ack_pending(js: JetStreamContext, stream: str, limit: int | None) -> int | None:
    """Return the acknowledgment window for a new durable within the stream's limit."""
    if limit is None:
        return None
    info = await js.stream_info(stream)
    consumer_limits = info.config.consumer_limits
    stream_limit = consumer_limits.max_ack_pending if consumer_limits is not None else None
    if stream_limit is not None and stream_limit > 0:
        return min(limit, stream_limit)
    return limit


async def _get_push_consumer(
    js: JetStreamContext,
    nc: Client,
    stream: str,
    durable: str,
    channel: str,
    *,
    limit: int | None,
) -> ConsumerInfo:
    try:
        return cast(ConsumerInfo, await js.consumer_info(stream, durable))
    except NotFoundError:
        # The window is shared by every worker in the queue group. It is set only
        # when creating the durable, so an existing configuration is never replaced.
        config = ConsumerConfig(
            durable_name=durable,
            deliver_group=durable,
            deliver_subject=nc.new_inbox(),
            filter_subject=channel,
            max_ack_pending=await _max_ack_pending(js, stream, limit),
        )
        try:
            return await js.add_consumer(stream, config=config)
        except APIError as error:
            if error.err_code not in _DURABLE_CREATE_CONFLICTS:
                raise
            return cast(ConsumerInfo, await js.consumer_info(stream, durable))


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
        self._keep_alive_during_settlement = False
        # The action of a publication that succeeded before its ack failed. Any
        # later settlement only acks to finish it instead of publishing again.
        self._published: MessageAction | None = None
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

    async def keep_alive(self) -> bool | None:
        if self._action is not None and not self._keep_alive_during_settlement:
            return False
        await self._msg.in_progress()
        return None

    async def _settle(
        self,
        action: MessageAction,
        operation: Callable[[], Awaitable[None]],
        event: str,
    ) -> None:
        # Reserve the action before the RPC: in a single event loop the
        # check-and-set is atomic, so concurrent settlements are deduplicated,
        # and a settlement cancelled mid-RPC is never followed by a second one.
        if self._action is not None:
            return
        if self._published is not None:
            # An earlier settlement already published its copy. Returning or
            # republishing the delivery would repeat it, so finish that one.
            action, operation, event = self._published, self._msg.ack, "message.ack"
        self._action = action
        self._keep_alive_during_settlement = True
        try:
            await operation()
        except Exception:
            # Cancellation is not caught, so a cancelled settlement stays
            # reserved even if the RPC may have reached the server. A failure
            # that already returned the message keeps the action it recorded.
            if self._action is action:
                self._action = None
            raise
        finally:
            self._keep_alive_during_settlement = False
        logger.debug(event, extra={"channel": self._channel})

    async def ack(self) -> None:
        await self._settle(MessageAction.acked, self._msg.ack, "message.ack")

    async def nack(self) -> None:
        await self._settle(MessageAction.nacked, self._dead_letter, "message.nack")

    async def _dead_letter(self) -> None:
        dlq = (
            self._server._dlq_topic_strategy(self._channel)
            if self._server._dlq_topic_strategy
            else None
        )
        if dlq is None:
            await self._msg.term()
            return
        headers = self.headers or {}
        headers["x-repid-original-channel"] = self._channel
        await self._publish_then_ack(
            MessageAction.nacked,
            dlq,
            self.payload,
            headers,
            "Cannot publish to DLQ.",
        )

    async def reject(self) -> None:
        await self._settle(MessageAction.rejected, self._msg.nak, "message.reject")

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

        # Reply then ack as one reserved unit: another settlement cannot
        # interleave, since _action is already reserved.
        await self._settle(
            MessageAction.replied,
            partial(
                self._publish_then_ack,
                MessageAction.replied,
                reply_channel,
                payload,
                reply_headers,
                "Cannot send reply.",
            ),
            "message.reply",
        )

    async def _publish_then_ack(
        self,
        action: MessageAction,
        subject: str,
        payload: bytes,
        headers: dict[str, str],
        failure: str,
    ) -> None:
        if self._server._js is not None:
            await self._server._js.publish(subject, payload, headers=headers)
        elif self._server._nc is not None:
            await self._server._nc.publish(subject, payload, headers=headers)
        else:
            # Return the delivery now; it stays settled as a rejection.
            await self._msg.nak()
            self._action = MessageAction.rejected
            logger.warning(
                "message.reject.connection_lost",
                extra={"channel": self._channel},
            )
            raise ConnectionError(f"NATS connection is not initialized. {failure}")
        self._published = action
        await self._msg.ack()


class NatsSubscriber(SubscriberT):
    def __init__(
        self,
        server: NatsServer,
        channels_to_callbacks: dict[str, MessageCallbackT],
        channel_concurrency: Mapping[str, Concurrency] | None = None,
    ) -> None:
        self._server = server
        self._channels_to_callbacks = channels_to_callbacks
        self._channel_concurrency = channel_concurrency or {}
        self._subs: dict[str, Subscription] = {}
        self._bindings: set[asyncio.Task[Subscription | None]] = set()
        self._submissions: dict[asyncio.Task[None], NatsReceivedMessage] = {}
        self._closed = False
        self._generation: object | None = object()

        self._task = asyncio.create_task(self._start(self._generation))

    @property
    def is_active(self) -> bool:
        return self._generation is not None

    @property
    def task(self) -> asyncio.Task[None]:
        return self._task

    async def _start(self, generation: object) -> None:
        try:
            await self._subscribe_channels(generation)
            while not self._closed:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.close()

    async def _subscribe_channels(self, generation: object) -> None:
        for channel, callback in self._channels_to_callbacks.items():
            if self._generation is not generation:
                return
            try:
                await self._subscribe_channel(channel, callback, generation)
            except Exception:
                if self._generation is generation:
                    await self.pause()
                    raise
                return

    async def _subscribe_channel(
        self,
        channel: str,
        callback: MessageCallbackT,
        generation: object,
    ) -> None:
        js = self._server._js
        nc = self._server._nc
        if js is None or nc is None:  # pragma: no cover
            raise ConnectionError("JetStream context is not initialized. Call connect() first.")

        durable = f"{channel}_group"
        stream = await js.find_stream_name_by_subject(channel)
        hint = self._channel_concurrency.get(channel)
        limit = hint.messages if hint is not None else None
        info = await _get_push_consumer(js, nc, stream, durable, channel, limit=limit)

        # Push deliveries can arrive before the bind call returns, so the
        # configuration must already be known when installing the callback.
        while self._generation is generation:
            config = info.config
            if config.deliver_group != durable or config.deliver_subject is None:
                raise JetStreamError(
                    f"Consumer {durable!r} does not match the expected push queue.",
                )
            sub = await self._bind_channel(js, channel, callback, generation, stream, info=info)
            if sub is None or self._generation is not generation:
                return
            if info.push_bound:
                return
            # A concurrent first creation can replace an unbound durable's
            # inbox. Confirm we bound the inbox that the broker actually kept.
            info = cast(ConsumerInfo, await js.consumer_info(stream, durable))
            if info.config.deliver_subject == config.deliver_subject:
                return
            if self._subs.get(channel) is sub:
                del self._subs[channel]
                await sub.unsubscribe()

    async def _bind_channel(
        self,
        js: JetStreamContext,
        channel: str,
        callback: MessageCallbackT,
        generation: object,
        stream: str,
        *,
        info: ConsumerInfo,
    ) -> Subscription | None:
        async def message_handler(msg: Msg) -> None:
            message = NatsReceivedMessage(
                msg,
                self._server,
                channel,
                ack_wait=_delivery_ack_wait(info.config, msg),
            )
            if self._generation is not generation:
                await message.reject()
                return
            # nats-py invokes this callback serially. Admission can wait, so
            # run it separately and let the next delivery start lease renewal.
            task = asyncio.create_task(self._submit(callback, message, generation))
            self._submissions[task] = message
            task.add_done_callback(self._submission_done)

        async def bind() -> Subscription | None:
            if self._generation is not generation:
                return None
            # nats-py registers the local subscription before flushing SUB.
            # Keep the binding alive until we can install or unsubscribe its handle.
            sub = await js.subscribe_bind(
                stream=stream,
                config=info.config,
                consumer=info.name,
                cb=message_handler,
                manual_ack=True,
            )
            if self._generation is not generation:
                await sub.unsubscribe()
                return None
            self._subs[channel] = sub
            return sub

        binding = asyncio.create_task(bind())
        self._bindings.add(binding)
        binding.add_done_callback(self._binding_done)
        await asyncio.wait({binding})
        return binding.result()

    def _binding_done(self, task: asyncio.Task[Subscription | None]) -> None:
        self._bindings.discard(task)
        if not task.cancelled():
            task.exception()  # The binding's caller may have been cancelled.

    async def _submit(
        self,
        callback: MessageCallbackT,
        message: NatsReceivedMessage,
        generation: object,
    ) -> None:
        if self._generation is not generation:
            await message.reject()
            return
        try:
            await callback(message)
        except asyncio.CancelledError:
            if not message.is_acted_on:
                await message.reject()
            raise
        except Exception:
            logger.exception("consumer.error.unexpected", extra={"channel": message.channel})
            if not message.is_acted_on:
                await message.nack()

    def _submission_done(self, task: asyncio.Task[None]) -> None:
        del self._submissions[task]
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("consumer.error.settlement", exc_info=error)

    async def pause(self) -> None:
        if not self.is_active:
            return
        self._generation = None
        subs = tuple(self._subs.values())
        self._subs.clear()
        for sub in subs:
            with suppress(Exception):
                await sub.unsubscribe()
        logger.debug("subscriber.pause", extra={"channel": "all"})

    async def resume(self) -> None:
        if self._closed or self.is_active:
            return
        generation = self._generation = object()
        try:
            await self._subscribe_channels(generation)
        except asyncio.CancelledError:
            if self._generation is generation:
                await self.pause()
            raise
        logger.debug("subscriber.resume", extra={"channel": "all"})

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.pause()
        current = asyncio.current_task()
        submissions = {
            task: message for task, message in self._submissions.items() if task is not current
        }
        for task in submissions:
            if not task.cancelling():
                task.cancel()
        if not self._task.done() and current is not self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        if self._bindings:
            await asyncio.wait(tuple(self._bindings))
        if submissions:
            await asyncio.gather(*submissions, return_exceptions=True)
            for task, message in submissions.items():
                if task.cancelled() and not message.is_acted_on:
                    # A submission cancelled before its first step cannot reject itself.
                    with suppress(Exception):
                        await message.reject()
        logger.debug("subscriber.close", extra={"channel": "all"})


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
        return {
            "supports_native_reply": True,
            "supports_lightweight_pause": False,
            "supports_keep_alive": True,
        }

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

        self._nc = await nats.connect(self.dsn)
        self._js = self._nc.jetstream()
        logger.info("server.connect", extra={"host": self.host})

    async def disconnect(self) -> None:
        for sub in list(self._active_subscribers):
            with suppress(Exception):  # pragma: no cover
                await sub.close()
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
        channels_to_callbacks: dict[str, MessageCallbackT],
        channel_concurrency: dict[str, Concurrency],
    ) -> SubscriberT:
        # Message limits size the acknowledgment window of durables this worker
        # creates. The window is shared by every worker in the queue group.
        if not self.is_connected or self._js is None:
            raise ConnectionError("NATS connection is not initialized. Call connect() first.")

        subscriber = NatsSubscriber(
            server=self,
            channels_to_callbacks=channels_to_callbacks,
            channel_concurrency=channel_concurrency,
        )
        self._active_subscribers.add(subscriber)
        subscriber.task.add_done_callback(lambda _: self._active_subscribers.discard(subscriber))
        return subscriber
