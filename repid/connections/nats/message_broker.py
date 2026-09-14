from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import nats
from nats.errors import BadSubscriptionError
from nats.js.api import ConsumerConfig

from repid.connections._subscriber import (
    AdmittedTaskTracker,
    SubscriberDispatcher,
)
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
    from nats.js.manager import JetStreamManager

    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag

logger = logging.getLogger("repid.connections.nats")

# JetStream consumer pause is horizon-based: the server auto-resumes at
# `pause_until`.  Pause requests use this far-future horizon so an explicit
# `resume` is the only realistic un-pause.
_PAUSE_HORIZON = timedelta(days=365)
_PAUSE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


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
        self._keep_alive_interval: int | None = (
            int(ack_wait) // 3 if ack_wait is not None and ack_wait > 0 else None
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
    def keep_alive_interval(self) -> int | None:
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


class NatsSubscriber:
    def __init__(
        self,
        server: NatsServer,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        dispatcher: SubscriberDispatcher,
    ) -> None:
        self._server = server
        self._channels_to_callbacks = channels_to_callbacks
        self._dispatcher = dispatcher
        self._subs: dict[str, Subscription] = {}
        self._consumer_streams: dict[str, str] = {}
        self._admitted_tasks = AdmittedTaskTracker()
        self._subscription_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        # A monitor may end before non-draining cancellation cleanup has
        # scheduled its reject/lease work.  The server uses this event to keep
        # the subscriber as a connection owner until that hand-off is complete.
        self._cleanup_ready = asyncio.Event()
        self._callbacks_cancelled = False
        self._closed = False
        self._active = False

        self._task = asyncio.create_task(self._start())

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def task(self) -> asyncio.Task:
        return self._task

    async def _start(self) -> None:
        try:
            async with self._subscription_lock:
                if self._closed:
                    return
                for channel, callback in self._channels_to_callbacks.items():
                    await self._subscribe_channel(channel, callback)
                self._active = True

            # Handlers are dispatched by the NATS library itself; this task
            # keeps the subscription open until close() cancels it.
            await asyncio.Future()
        finally:
            if not self._closed:
                await self.stop()
                await self.finish()

    @staticmethod
    async def _run_callback(
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        channel: str,
        message: ReceivedMessageT,
    ) -> None:
        try:
            await callback(message)
        except asyncio.CancelledError:
            if not message.is_acted_on:
                await NatsSubscriber._reject_unacted_message(cast(NatsReceivedMessage, message))
            raise
        except Exception:
            logger.exception("consumer.error.unexpected", extra={"channel": channel})
            if not message.is_acted_on:
                await message.nack()

    @staticmethod
    async def _reject_unacted_message(message: NatsReceivedMessage) -> None:
        if not message.is_acted_on:
            try:
                await message.reject()
            except Exception as exc:
                logger.exception("message.reject.error", exc_info=exc)

    async def _handle_message(
        self,
        channel: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        ack_wait: float | None,
        message: Msg,
    ) -> None:
        received = NatsReceivedMessage(message, self._server, channel, ack_wait=ack_wait)
        if self._closed:
            await self._reject_unacted_message(received)
            return
        try:
            lease = await self._dispatcher.reserve(received)
        except BaseException:
            await self._reject_unacted_message(received)
            raise
        if lease is None:
            return
        if self._closed:
            try:
                await self._reject_unacted_message(received)
            finally:
                try:
                    await lease.release()
                except Exception as exc:
                    logger.exception("subscriber.lease.release.error", exc_info=exc)
            return
        self._admitted_tasks.start(
            self._dispatcher,
            lease,
            received,
            partial(self._run_callback, callback, channel),
            on_cancel=partial(self._reject_unacted_message, received),
        )

    async def _subscribe_channel(
        self,
        channel: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        if self._server._js is None:  # pragma: no cover
            raise ConnectionError("JetStream context is not initialized. Call connect() first.")

        ready = asyncio.Event()
        ack_wait: float | None = None
        consumer_name = f"{channel}_group"

        async def handle(message: Msg) -> None:
            await ready.wait()
            await self._handle_message(channel, callback, ack_wait, message)

        config = await self._consumer_config(channel, consumer_name)
        sub = await self._server._js.subscribe(
            channel,
            queue=consumer_name,
            durable=consumer_name,
            cb=handle,
            manual_ack=True,
            config=config,
        )
        self._subs[channel] = sub
        try:
            consumer_info = await sub.consumer_info()
            ack_wait = consumer_info.config.ack_wait
            self._consumer_streams[channel] = consumer_info.stream_name
        except Exception as exc:
            logger.warning(
                "subscriber.consumer_info.error",
                extra={"channel": channel},
                exc_info=exc,
            )
            # Resolve the stream now so pause/resume cannot fail later during
            # processing.
            js = self._server._js
            if js is not None:
                with suppress(Exception):
                    self._consumer_streams[channel] = await js.find_stream_name_by_subject(
                        channel,
                    )
        else:
            if consumer_info.paused is True:
                # Consumer pause is durable server-side state: a previous
                # subscriber may have left it paused after a crash or an
                # unclean exit.  A fresh subscription must receive deliveries.
                with suppress(Exception):
                    await self._resume_consumer(channel)
        finally:
            ready.set()

    async def _unsubscribe_all(self) -> Exception | None:
        self._active = False
        first_error: Exception | None = None
        for channel, sub in tuple(self._subs.items()):
            # Consumer pause is durable server-side state shared across the
            # queue group; leaving it paused would starve the next subscriber.
            with suppress(Exception):
                await self._resume_consumer(channel)
            try:
                await sub.unsubscribe()
            except BadSubscriptionError:
                self._subs.pop(channel, None)
            except Exception as exc:
                if first_error is None:
                    first_error = exc
                else:
                    logger.exception(
                        "subscriber.unsubscribe.error",
                        extra={"channel": channel},
                        exc_info=exc,
                    )
            else:
                self._subs.pop(channel, None)
        logger.debug("subscriber.unsubscribe", extra={"channel": "all"})
        return first_error

    def _require_jetstream_manager(self) -> JetStreamManager:
        """Return the manager captured at connect() time."""
        if self._server._jsm is None:
            raise ConnectionError("JetStream is not initialized. Call connect() first.")
        return self._server._jsm

    def _resolve_stream(self, channel: str) -> str:
        """Return the channel's stream, already resolved at subscribe time."""
        stream = self._consumer_streams.get(channel)
        if stream is None:
            raise ConnectionError(
                f"Stream for channel {channel!r} was not resolved during subscription.",
            )
        return stream

    async def _pause_consumer(self, channel: str) -> None:
        """Stop JetStream delivery for a channel without tearing down the subscription.

        The consumer stays attached; only the server-side delivery stops.
        Requires nats-server >= 2.11.
        """
        jsm = self._require_jetstream_manager()
        stream = self._resolve_stream(channel)
        pause_until = (datetime.now(timezone.utc) + _PAUSE_HORIZON).strftime(_PAUSE_TIME_FORMAT)
        try:
            await jsm.pause_consumer(stream, f"{channel}_group", pause_until)
        except Exception as exc:
            raise ConnectionError(
                "Consumer pause request failed; consumer pause requires nats-server >= 2.11.",
            ) from exc
        logger.debug("subscriber.pause", extra={"channel": channel})

    async def _resume_consumer(self, channel: str) -> None:
        """Resume server-side delivery for a paused consumer."""
        jsm = self._require_jetstream_manager()
        stream = self._resolve_stream(channel)
        try:
            await jsm.resume_consumer(stream, f"{channel}_group")
        except Exception as exc:
            raise ConnectionError(
                "Consumer resume request failed; consumer pause requires nats-server >= 2.11.",
            ) from exc
        logger.debug("subscriber.resume", extra={"channel": channel})

    async def pause(self) -> None:
        async with self._subscription_lock:
            if not self._active and not self._subs:
                return
            self._active = False
            first_error: Exception | None = None
            for channel in tuple(self._subs):
                try:
                    await self._pause_consumer(channel)
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
                    else:
                        logger.exception(
                            "subscriber.pause.error",
                            extra={"channel": channel},
                            exc_info=exc,
                        )
            if first_error is not None:
                raise first_error

    async def resume(self) -> None:
        async with self._subscription_lock:
            if self._closed or self._active:
                return
            first_error: Exception | None = None
            for channel, callback in self._channels_to_callbacks.items():
                if channel not in self._subs:
                    await self._subscribe_channel(channel, callback)
                    continue
                try:
                    await self._resume_consumer(channel)
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
                    else:
                        logger.exception(
                            "subscriber.resume.error",
                            extra={"channel": channel},
                            exc_info=exc,
                        )
            if first_error is not None:
                raise first_error
            self._active = True
            logger.debug("subscriber.resume", extra={"channel": "all"})

    async def pause_channel(self, channel: str) -> None:
        """Stop server-side delivery for a single channel's consumer."""
        async with self._subscription_lock:
            if channel not in self._subs:
                return
            await self._pause_consumer(channel)

    async def resume_channel(self, channel: str) -> None:
        """Resume server-side delivery for a single channel's consumer."""
        async with self._subscription_lock:
            if self._closed:
                return
            callback = self._channels_to_callbacks.get(channel)
            if callback is None:
                return
            if channel not in self._subs:
                await self._subscribe_channel(channel, callback)
                return
            await self._resume_consumer(channel)
            logger.debug("subscriber.resume", extra={"channel": channel})

    async def _consumer_config(
        self,
        channel: str,
        consumer_name: str,
    ) -> ConsumerConfig | None:
        """Build the consumer config carrying the per-channel intake window.

        The durable consumer's `max_ack_pending` bounds unacknowledged
        deliveries per channel: the credit stays consumed until the message
        is settled, which happens after admission.  An auto-created consumer
        uses this config directly; an existing one is updated server-side
        before binding (best effort).
        """
        native_limit = self._dispatcher.native_message_limit(channel)
        if native_limit is None:
            return None
        jsm = self._require_jetstream_manager()
        config = ConsumerConfig(max_ack_pending=native_limit)
        try:
            stream = await jsm.find_stream_name_by_subject(channel)
            existing = await jsm.consumer_info(stream, consumer_name)
        except Exception:  # noqa: BLE001
            # Auto-creation with `config` still applies the window.
            return config
        if existing.config.max_ack_pending == native_limit:
            return None
        try:
            await jsm.add_consumer(
                stream,
                config=replace(existing.config, max_ack_pending=native_limit),
            )
        except Exception as exc:
            logger.warning(
                "subscriber.consumer_config.update.error",
                extra={"channel": channel},
                exc_info=exc,
            )
        return None

    async def _wait_for_cleanup_ready(self) -> None:
        """Wait until a closing monitor has handed off cancellation cleanup."""
        await self._cleanup_ready.wait()

    async def _drain_admitted_work(self) -> None:
        """Wait for already-cancelled callbacks and their cleanup without re-cancelling."""
        await self._admitted_tasks.drain()

    async def _cancel_callbacks_once(self) -> None:
        if not self._callbacks_cancelled:
            self._callbacks_cancelled = True
            await self._admitted_tasks.cancel_and_drain(drain=False)

    async def stop(self) -> None:
        """Stop intake: reject incoming deliveries and cancel the monitor loop.

        Waits until the monitor task has actually processed the cancellation
        (including a startup blocked mid-subscribe) before returning, so that
        `finish()` cannot unsubscribe while startup is still running.
        """
        # Set this before waiting for the lock so incoming deliveries are
        # rejected while another concurrent closer is winding down.
        self._closed = True
        async with self._close_lock:
            if not self._task.done() and asyncio.current_task() is not self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    # The monitor was cancelled as requested; swallow that
                    # error only when it comes from the monitor task, not
                    # when the caller itself is being cancelled.
                    if not self._task.cancelled():
                        raise
                except Exception as exc:
                    # Startup failures surface via `task`; a concurrent stop()
                    # must not turn them into a surprising raise.
                    logger.exception(
                        "subscriber.stop.monitor.error",
                        exc_info=exc,
                    )
            self._cleanup_ready.set()

    async def finish(self) -> None:
        """Cancel remaining callbacks, drain their cleanup, unsubscribe."""
        await self.stop()
        async with self._close_lock:
            await self._cancel_callbacks_once()
            await self._drain_admitted_work()
            if (error := await self._unsubscribe_all()) is not None:
                raise error
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
        # JetStreamContext is itself the JetStream manager; captured at
        # connect() so pause/resume never fail looking it up mid-processing.
        self._jsm: JetStreamManager | None = None

        self._title = title
        self._summary = summary
        self._description = description
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings
        self._active_subscribers: set[NatsSubscriber] = set()
        self._subscriber_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._disconnect_lock = asyncio.Lock()

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
            "supports_keep_alive": True,
            "supports_pause": True,
            "supports_pause_per_channel": True,
            "supports_native_message_flow_control": False,
            "supports_native_message_flow_control_per_channel": True,
            "supports_native_payload_flow_control": False,
            "supports_native_payload_flow_control_per_channel": False,
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
            self._jsm = None

        self._nc = await nats.connect(self.dsn)
        self._js = self._nc.jetstream()
        self._jsm = self._js
        logger.info("server.connect", extra={"host": self.host})

    async def disconnect(self) -> None:
        async with self._disconnect_lock:
            for sub in list(self._active_subscribers):
                with suppress(Exception):  # pragma: no cover
                    await sub.stop()
                    await sub.finish()
            self._active_subscribers.clear()

            if self._nc is not None:
                await self._nc.close()
                self._nc = None
                self._js = None
                self._jsm = None
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
        dispatcher: SubscriberDispatcher,
    ) -> SubscriberT:
        if not self.is_connected or self._js is None:
            raise ConnectionError("NATS connection is not initialized. Call connect() first.")

        subscriber = NatsSubscriber(
            server=self,
            channels_to_callbacks=channels_to_callbacks,
            dispatcher=dispatcher,
        )
        self._active_subscribers.add(subscriber)

        def discard_when_cleanup_settles(_: asyncio.Task[Any]) -> None:
            task = asyncio.create_task(self._discard_subscriber_when_settled(subscriber))
            self._subscriber_cleanup_tasks.add(task)
            task.add_done_callback(self._subscriber_cleanup_tasks.discard)

        subscriber.task.add_done_callback(discard_when_cleanup_settles)
        return subscriber

    async def _discard_subscriber_when_settled(self, subscriber: NatsSubscriber) -> None:
        # Do not let a completed monitor relinquish server ownership while a
        # non-draining close is still rejecting messages or releasing leases.
        await subscriber._wait_for_cleanup_ready()
        await subscriber._drain_admitted_work()
        self._active_subscribers.discard(subscriber)
