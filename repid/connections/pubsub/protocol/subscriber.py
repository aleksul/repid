from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Coroutine
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING

import grpc
import grpc.aio

from repid.connections._subscriber import (
    AdmittedTaskTracker,
    SubscriberDispatcher,
    run_supervised,
)

from ._helpers import ChannelConfig, QueuedDelivery
from .proto import ReceivedMessage, StreamingPullRequest, StreamingPullResponse
from .received_message import PubsubReceivedMessage
from .resilience import ResilienceState

logger = logging.getLogger("repid.connections.pubsub.protocol")


def _resumed_event() -> asyncio.Event:
    event = asyncio.Event()
    event.set()
    return event


if TYPE_CHECKING:
    from repid.connections.pubsub.message_broker import PubsubServer

    from .credentials import CredentialsProvider


# gRPC method paths
STREAMING_PULL_METHOD = "/google.pubsub.v1.Subscriber/StreamingPull"


class PubsubSubscriber:
    """Pub/Sub subscriber using StreamingPull with resilience.

    Uses StreamingPull for efficient message delivery and flow control via
    max_outstanding_messages. Ack/nack/deadline operations use unary RPCs.
    """

    def __init__(
        self,
        *,
        channel: grpc.aio.Channel,
        channel_configs: list[ChannelConfig],
        credentials_provider: CredentialsProvider,
        resilience_state: ResilienceState,
        stream_ack_deadline_seconds: int,
        client_id: str,
        dispatcher: SubscriberDispatcher,
        server: PubsubServer,
        heartbeat_interval: float = 25.0,
        error_retry_delay: float = 1.0,
    ) -> None:
        self._channel = channel
        self._channel_configs = channel_configs
        self._credentials_provider = credentials_provider
        self._resilience_state = resilience_state
        self._stream_ack_deadline_seconds = stream_ack_deadline_seconds
        self._client_id = client_id
        self._dispatcher = dispatcher
        self._server = server
        self._heartbeat_interval = heartbeat_interval
        self._error_retry_delay = error_retry_delay

        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._channel_pause_events = {
            config.channel: _resumed_event() for config in channel_configs
        }
        self._shutdown_event = asyncio.Event()
        self._is_active = True
        self._is_closing = False
        self._close_setup_task: asyncio.Task[None] | None = None

        self._delivery_queue: asyncio.Queue[QueuedDelivery] = asyncio.Queue()
        self._admitted_tasks = AdmittedTaskTracker()
        self._close_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._task: asyncio.Task[None] | None = None

        # Track in-flight messages for nacking on close
        self._in_flight_messages: set[PubsubReceivedMessage] = set()

    @classmethod
    async def create(
        cls,
        *,
        channel: grpc.aio.Channel,
        channel_configs: list[ChannelConfig],
        credentials_provider: CredentialsProvider,
        resilience_state: ResilienceState,
        stream_ack_deadline_seconds: int,
        client_id: str,
        dispatcher: SubscriberDispatcher,
        server: PubsubServer,
    ) -> PubsubSubscriber:
        """Create and start a new subscriber."""
        subscriber = cls(
            channel=channel,
            channel_configs=channel_configs,
            credentials_provider=credentials_provider,
            resilience_state=resilience_state,
            stream_ack_deadline_seconds=stream_ack_deadline_seconds,
            client_id=client_id,
            dispatcher=dispatcher,
            server=server,
        )
        subscriber._start_background_tasks()
        return subscriber

    def _start_background_tasks(self) -> None:
        """Start background processing tasks."""
        if not self._channel_configs:
            self._is_active = False
            return
        self._task = asyncio.create_task(self._process_background())
        self._is_active = True

    async def _process_background(self) -> None:
        """Main background processing loop.

        One loop ending through a crash or shutdown ends them all.
        """
        await run_supervised(
            *(self._streaming_pull_loop(config) for config in self._channel_configs),
            self._dispatch_loop(),
        )

    async def _streaming_pull_loop(self, config: ChannelConfig) -> None:
        """StreamingPull loop for a single subscription with resilience."""
        channel_event = self._channel_pause_events.get(config.channel, self._pause_event)
        while not self._shutdown_event.is_set():
            await channel_event.wait()

            try:
                await self._run_streaming_pull(config)
            except grpc.aio.AioRpcError as e:
                if self._is_expected_stream_close(e):
                    logger.debug(
                        "streaming_pull.reconnect.expected_close",
                        extra={"subscription": config.subscription_path},
                    )
                    continue

                await self._resilience_state.record_failure()

                if not self._resilience_state.is_retryable(e):
                    logger.exception(
                        "streaming_pull.error.non_retryable",
                        extra={"subscription": config.subscription_path},
                        exc_info=e,
                    )
                    raise

                if not self._resilience_state.should_retry():
                    logger.error(
                        "streaming_pull.reconnect.exhausted",
                        extra={"subscription": config.subscription_path},
                    )
                    raise

                delay = self._resilience_state.calculate_delay()
                logger.warning(
                    "streaming_pull.reconnect.retry",
                    extra={"subscription": config.subscription_path, "delay": delay},
                    exc_info=e,
                )
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(
                    "streaming_pull.error.unexpected",
                    extra={"subscription": config.subscription_path},
                    exc_info=exc,
                )
                await asyncio.sleep(self._error_retry_delay)

        logger.debug(
            "streaming_pull.stop",
            extra={"subscription": config.subscription_path},
        )

    @staticmethod
    def _is_expected_stream_close(error: grpc.aio.AioRpcError) -> bool:
        """Check if a gRPC error indicates an expected stream closure."""
        return (
            error.code() == grpc.StatusCode.UNAVAILABLE
            and (details := error.details()) is not None
            and "The StreamingPull stream closed for an expected reason and should be recreated"
            in details
        )

    async def _request_iterator(
        self,
        config: ChannelConfig,
    ) -> AsyncIterator[StreamingPullRequest]:
        """Generate requests for the stream and keep it open after initialization."""
        # Send initial request with subscription info
        yield StreamingPullRequest(
            subscription=config.subscription_path,
            stream_ack_deadline_seconds=self._stream_ack_deadline_seconds,
            client_id=self._client_id,
            max_outstanding_messages=self._dispatcher.native_message_limit(config.channel) or 0,
            max_outstanding_bytes=self._dispatcher.native_payload_limit(config.channel) or 0,
        )

        while not self._shutdown_event.is_set():
            await asyncio.sleep(self._heartbeat_interval)
            if self._shutdown_event.is_set():
                break
            logger.debug("heartbeat.send")
            yield StreamingPullRequest(
                stream_ack_deadline_seconds=self._stream_ack_deadline_seconds,
            )

    def _create_received_message(
        self,
        received_msg: ReceivedMessage,
        config: ChannelConfig,
    ) -> PubsubReceivedMessage:
        """Create a PubsubReceivedMessage from a raw received message."""
        return PubsubReceivedMessage(
            raw_message=received_msg.message,  # type: ignore[arg-type]
            ack_id=received_msg.ack_id,
            delivery_attempt=received_msg.delivery_attempt,
            subscription_path=config.subscription_path,
            channel_name=config.channel,
            server=self._server,
            stream_ack_deadline_seconds=self._stream_ack_deadline_seconds,
        )

    async def _process_response(
        self,
        response: StreamingPullResponse,
        config: ChannelConfig,
    ) -> None:
        """Process a single StreamingPull response."""
        messages = [
            self._create_received_message(received_msg, config)
            for received_msg in response.received_messages
            if received_msg.message is not None
        ]
        self._in_flight_messages.update(messages)
        for message in messages:
            self._dispatcher.start_keep_alive(message)

        for index, message in enumerate(messages):
            try:
                channel_event = self._channel_pause_events.get(config.channel, self._pause_event)
                await self._pause_event.wait()
                await channel_event.wait()
                # Pub/Sub dispatches through an internal queue (see ``_dispatch_loop``),
                # so reserve at fetch time and carry the lease until the callback settles.
                lease = await self._dispatcher.reserve(message)
            except BaseException:
                if not self._is_closing:
                    for unadmitted_message in messages[index:]:
                        await self._cleanup_message(unadmitted_message)
                raise
            if lease is None:
                self._in_flight_messages.discard(message)
                continue
            if self._is_closing:
                # A close snapshot owns rejecting this message, so a late
                # admission releases only the intake lease instead of
                # enqueueing a delivery no dispatcher will ever consume.
                self._schedule_close_cleanup(lease.release())
                return
            await self._delivery_queue.put(
                QueuedDelivery(callback=config.callback, message=message, lease=lease),
            )

    async def _run_streaming_pull(
        self,
        config: ChannelConfig,
    ) -> None:
        """Run a single StreamingPull session."""
        # Ensure credentials are valid
        await self._credentials_provider.ensure_valid()

        # Create the bidirectional stream
        stream_method = self._channel.stream_stream(  # type: ignore[var-annotated]
            STREAMING_PULL_METHOD,
            request_serializer=lambda req: req.serialize(),
            response_deserializer=StreamingPullResponse.deserialize,
        )

        iterator = self._request_iterator(config)

        # Start the stream
        call = stream_method(iterator)

        # Process responses
        async for response in call:
            await self._resilience_state.record_success()

            if self._shutdown_event.is_set():
                break

            await self._process_response(response, config)

    async def _dispatch_loop(self) -> None:
        """Dispatch messages to callbacks."""
        try:
            while True:
                delivery = await self._delivery_queue.get()
                self._admitted_tasks.start_task(
                    partial(self._execute_callback, delivery),
                    on_cancel=partial(self._cleanup_delivery, delivery),
                    message=delivery.message,
                )
                self._delivery_queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            logger.debug("dispatcher.stop")

    async def _process_delivery(
        self,
        delivery: QueuedDelivery,
        message: PubsubReceivedMessage,
    ) -> None:
        try:
            await delivery.callback(message)
        except asyncio.CancelledError:
            if not message.is_acted_on:
                with suppress(Exception):
                    await message.reject()
            raise
        except Exception as exc:
            logger.exception("message.callback.error", exc_info=exc)
            if not message.is_acted_on:
                with suppress(Exception):
                    await message.nack()
        finally:
            self._in_flight_messages.discard(message)

    async def _cleanup_delivery(self, delivery: QueuedDelivery) -> None:
        """Reject and release a delivery removed during close."""
        try:
            if not delivery.message.is_acted_on:
                with suppress(Exception):
                    await delivery.message.reject()
        finally:
            self._in_flight_messages.discard(delivery.message)
            await delivery.lease.release()

    async def _cleanup_message(self, message: PubsubReceivedMessage) -> None:
        """Reject an in-flight message removed during close."""
        try:
            await self._dispatcher.stop_keep_alive(message)
            if not message.is_acted_on:
                with suppress(Exception):
                    await message.reject()
        finally:
            self._in_flight_messages.discard(message)

    def _schedule_close_cleanup(self, coro: Coroutine[None, None, None]) -> None:
        task = asyncio.create_task(coro)
        self._close_cleanup_tasks.add(task)
        task.add_done_callback(self._close_cleanup_done)

    def _close_cleanup_done(self, task: asyncio.Task[None]) -> None:
        self._close_cleanup_tasks.discard(task)
        if task.cancelled():
            return
        if (exc := task.exception()) is not None:
            logger.exception("subscriber.close.cleanup.error", exc_info=exc)

    async def _drain_close_cleanups(self) -> None:
        while self._close_cleanup_tasks:
            await asyncio.gather(*tuple(self._close_cleanup_tasks), return_exceptions=True)

    async def _execute_callback(self, delivery: QueuedDelivery) -> None:
        """Execute a queued callback while holding its intake lease."""
        await self._dispatcher.run_admitted(
            delivery.lease,
            delivery.message,
            partial(self._process_delivery, delivery),
        )

    @property
    def is_active(self) -> bool:
        """Check if the subscriber is active."""
        return self._is_active and not self._shutdown_event.is_set()

    @property
    def task(self) -> asyncio.Task[None]:
        """Get the main background task."""
        if self._task is None:
            raise RuntimeError("Subscriber has not been started.")
        return self._task

    async def pause(self) -> None:
        """Pause message processing."""
        if not self.is_active:
            return
        self._pause_event.clear()
        for event in self._channel_pause_events.values():
            event.clear()
        self._is_active = False

    async def resume(self) -> None:
        """Resume message processing."""
        if self._shutdown_event.is_set():
            return
        self._pause_event.set()
        for event in self._channel_pause_events.values():
            event.set()
        self._is_active = True

    async def pause_channel(self, channel: str) -> None:
        """Pause intake for a single channel."""
        event = self._channel_pause_events.get(channel)
        if event is not None:
            event.clear()

    async def resume_channel(self, channel: str) -> None:
        """Resume intake for a single channel."""
        event = self._channel_pause_events.get(channel)
        if event is not None:
            event.set()

    async def stop(self) -> None:
        """Stop intake: pause, signal shutdown, and cancel the background loops."""
        if self._is_closing:
            return
        self._is_closing = True
        # Pause to stop receiving new messages, then signal shutdown.
        self._pause_event.clear()
        self._shutdown_event.set()
        if self._task is not None and asyncio.current_task() is not self._task:
            self._task.cancel()

    async def finish(self) -> None:
        """Cancel remaining callbacks once, schedule their cleanup, and drain."""
        await self.stop()
        await self._cancel_and_schedule_close_work()
        await self._drain_close_work()

    async def _cancel_and_schedule_close_work(self) -> None:
        """Cancel work and schedule every message cleanup exactly once."""
        if self._close_setup_task is not None:
            await asyncio.shield(self._close_setup_task)
            return
        self._close_setup_task = asyncio.create_task(self._cancel_and_schedule_cleanup())
        await asyncio.shield(self._close_setup_task)

    async def _cancel_and_schedule_cleanup(self) -> None:
        """Cancel callbacks and schedule every message cleanup exactly once."""
        if self._admitted_tasks.tasks:
            logger.warning(
                "subscriber.close.tasks_pending",
                extra={"count": len(self._admitted_tasks.tasks)},
            )
        tracker_owned_messages = {id(message) for message in self._admitted_tasks.owned_messages}
        # Initiate cancellation without waiting so cleanup scheduling is completed
        # before any cancellable drain operation.
        await self._admitted_tasks.cancel_and_drain(drain=False)

        self._is_active = False

        # Tracker-owned messages are already being rejected and released there.
        # Schedule the rest so a non-draining close does not block on broker
        # confirmation while retaining cleanup.
        queued_message_ids: set[int] = set()
        while not self._delivery_queue.empty():
            delivery = self._delivery_queue.get_nowait()
            queued_message_ids.add(id(delivery.message))
            self._delivery_queue.task_done()
            self._schedule_close_cleanup(self._cleanup_delivery(delivery))

        skipped_message_ids = tracker_owned_messages | queued_message_ids
        for msg in list(self._in_flight_messages):
            if id(msg) not in skipped_message_ids:
                self._schedule_close_cleanup(self._cleanup_message(msg))

    async def _drain_close_work(self) -> None:
        """Wait for work left by an earlier non-draining close."""
        await self._admitted_tasks.cancel_and_drain(drain=True)
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
        await self._drain_close_cleanups()
