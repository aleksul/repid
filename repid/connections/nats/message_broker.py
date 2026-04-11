from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import nats

from repid.connections.abc import (
    CapabilitiesT,
    MessageAction,
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

logger = logging.getLogger("repid.connections.nats")


class NatsReceivedMessage(ReceivedMessageT):
    def __init__(self, msg: Msg, server: NatsServer, channel: str) -> None:
        self._msg = msg
        self._server = server
        self._channel = channel
        self._action: MessageAction | None = None
        self._is_acted_on = False

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
    def channel(self) -> str:
        return self._channel

    @property
    def action(self) -> MessageAction | None:
        return self._action

    @property
    def is_acted_on(self) -> bool:
        return self._is_acted_on

    @property
    def message_id(self) -> str | None:
        with suppress(Exception):
            return f"{self._msg.metadata.stream}:{self._msg.metadata.sequence.stream}"
        return None

    async def ack(self) -> None:
        if self._is_acted_on:
            return
        await self._msg.ack()
        self._is_acted_on = True
        self._action = MessageAction.acked
        logger.debug("message.ack", extra={"channel": self._channel})

    async def nack(self) -> None:
        if self._is_acted_on:
            return

        dlq = (
            self._server._dlq_topic_strategy(self._channel)
            if self._server._dlq_topic_strategy
            else None
        )

        if dlq is None:
            if hasattr(self._msg, "term"):
                await self._msg.term()
            elif hasattr(self._msg, "nak"):
                await self._msg.nak()
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
                raise ConnectionError("NATS connection is not initialized. Cannot publish to DLQ.")

        self._is_acted_on = True
        self._action = MessageAction.nacked
        logger.debug("message.nack", extra={"channel": self._channel})

    async def reject(self) -> None:
        if self._is_acted_on:
            return
        if hasattr(self._msg, "nak"):
            await self._msg.nak()
        self._is_acted_on = True
        self._action = MessageAction.rejected
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
        if self._is_acted_on:
            return

        reply_headers = dict(headers) if headers else {}
        if content_type:
            reply_headers["content-type"] = content_type

        # Atomic reply: publish and then ack
        if self._server._js is not None:
            reply_channel = channel or self._channel
            await self._server._js.publish(reply_channel, payload, headers=reply_headers)
            await self._msg.ack()
        elif self._server._nc is not None:
            reply_channel = channel or self._channel
            await self._server._nc.publish(reply_channel, payload, headers=reply_headers)
            if hasattr(self._msg, "ack"):
                await self._msg.ack()
        else:
            await self._msg.nak()
            raise ConnectionError("NATS connection is not initialized. Cannot send reply.")

        self._is_acted_on = True
        self._action = MessageAction.replied
        logger.debug("message.reply", extra={"channel": self._channel})


class NatsSubscriber(SubscriberT):
    def __init__(
        self,
        server: NatsServer,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> None:
        self._server = server
        self._channels_to_callbacks = channels_to_callbacks
        self._concurrency_limit = concurrency_limit
        self._subs: dict[str, Subscription] = {}
        self._closed = False
        self._active = False

        self._semaphore = (
            asyncio.Semaphore(concurrency_limit)
            if concurrency_limit and concurrency_limit > 0
            else None
        )

        self._task = asyncio.create_task(self._start())

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def task(self) -> asyncio.Task:
        return self._task

    async def _start(self) -> None:
        self._active = True
        try:
            for channel, callback in self._channels_to_callbacks.items():
                await self._subscribe_channel(channel, callback)

            while not self._closed:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.close()

    async def _subscribe_channel(
        self,
        channel: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        async def message_handler(msg: Msg) -> None:
            wrapped = NatsReceivedMessage(msg, self._server, channel)
            if self._semaphore:
                async with self._semaphore:
                    try:
                        await callback(wrapped)
                    except Exception:
                        logger.exception("consumer.error.unexpected", extra={"channel": channel})
                        if not wrapped.is_acted_on:
                            await wrapped.nack()
            else:
                try:
                    await callback(wrapped)
                except Exception:
                    logger.exception("consumer.error.unexpected", extra={"channel": channel})
                    if not wrapped.is_acted_on:
                        await wrapped.nack()

        if self._server._js is not None:
            sub = await self._server._js.subscribe(
                channel,
                queue=f"{channel}_group",
                durable=f"{channel}_group",
                cb=message_handler,
                manual_ack=True,
            )
            self._subs[channel] = sub
        else:  # pragma: no cover
            # Shouldn't happen, as we check for jetstream context before creating subscriber
            raise ConnectionError("JetStream context is not initialized. Call connect() first.")

    async def pause(self) -> None:
        if not self._active:
            return
        self._active = False
        for _channel, sub in self._subs.items():
            with suppress(Exception):
                await sub.unsubscribe()
        self._subs.clear()
        logger.debug("subscriber.pause", extra={"channel": "all"})

    async def resume(self) -> None:
        if self._closed or self._active:
            return
        self._active = True
        for channel, callback in self._channels_to_callbacks.items():
            await self._subscribe_channel(channel, callback)
        logger.debug("subscriber.resume", extra={"channel": "all"})

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.pause()
        if not self._task.done() and asyncio.current_task() != self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
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
            "supports_acknowledgments": True,
            "supports_persistence": True,
            "supports_reply": True,
            "supports_lightweight_pause": False,
        }

    @property
    def is_connected(self) -> bool:
        return self._nc is not None and self._nc.is_connected

    async def connect(self) -> None:
        if self.is_connected:
            return

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
        server_specific_parameters: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> None:
        if not self.is_connected:
            raise ConnectionError("NATS connection is not initialized. Call connect() first.")

        headers = dict(message.headers) if message.headers else {}
        if message.content_type:
            headers["content-type"] = message.content_type

        if self._js is not None:
            await self._js.publish(channel, message.payload, headers=headers)
            logger.debug("channel.publish", extra={"channel": channel})
        elif self._nc is not None:
            await self._nc.publish(channel, message.payload, headers=headers)
            logger.debug("channel.publish.core", extra={"channel": channel})
        else:
            raise ConnectionError("NATS connection is not initialized. Cannot publish message.")

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        if not self.is_connected or self._js is None:
            raise ConnectionError("NATS connection is not initialized. Call connect() first.")

        subscriber = NatsSubscriber(
            server=self,
            channels_to_callbacks=channels_to_callbacks,
            concurrency_limit=concurrency_limit,
        )
        self._active_subscribers.add(subscriber)
        subscriber.task.add_done_callback(lambda _: self._active_subscribers.discard(subscriber))
        return subscriber
