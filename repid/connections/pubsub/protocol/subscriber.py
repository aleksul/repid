from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import TYPE_CHECKING, cast

import grpc
import grpc.aio

from ._helpers import ChannelConfig, QueuedDelivery
from .proto import ReceivedMessage, StreamingPullRequest, StreamingPullResponse
from .received_message import PubsubReceivedMessage
from .resilience import ResilienceState

logger = logging.getLogger("repid.connections.pubsub.protocol")

if TYPE_CHECKING:
    from repid.connections.pubsub.message_broker import PubsubServer

    from .credentials import CredentialsProvider


# gRPC method paths
STREAMING_PULL_METHOD = "/google.pubsub.v1.Subscriber/StreamingPull"


class PubsubSubscriber:
    """Pub/Sub subscriber using StreamingPull with resilience.

    Uses bidirectional gRPC streaming for efficient message delivery
    and ack/nack via the stream. Implements flow control via
    max_outstanding_messages.
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
        concurrency_limit: int | None,
        server: PubsubServer,
        heartbeat_interval: float = 25.0,
        error_retry_delay: float = 1.0,
        poll_interval: float = 1.0,
    ) -> None:
        self._channel = channel
        self._channel_configs = channel_configs
        self._credentials_provider = credentials_provider
        self._resilience_state = resilience_state
        self._stream_ack_deadline_seconds = stream_ack_deadline_seconds
        self._client_id = client_id
        self._concurrency_limit = concurrency_limit
        self._server = server
        self._heartbeat_interval = heartbeat_interval
        self._error_retry_delay = error_retry_delay
        self._poll_interval = poll_interval

        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._shutdown_event = asyncio.Event()
        self._is_active = True
        self._is_closing = False

        self._delivery_queue: asyncio.Queue[QueuedDelivery] = asyncio.Queue()
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._task: asyncio.Task[None] | None = None

        # Per-subscription write queues for ack/nack
        self._write_queues: dict[str, asyncio.Queue[StreamingPullRequest]] = {}

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
        concurrency_limit: int | None,
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
            concurrency_limit=concurrency_limit,
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
        """Main background processing loop."""
        tasks = [
            *(self._streaming_pull_loop(config) for config in self._channel_configs),
            self._dispatch_loop(),
        ]
        await asyncio.gather(*tasks)

    async def _streaming_pull_loop(self, config: ChannelConfig) -> None:
        """StreamingPull loop for a single subscription with resilience."""
        subscription_path = config.subscription_path

        # Create write queue for this subscription
        write_queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()
        self._write_queues[subscription_path] = write_queue

        while not self._shutdown_event.is_set():
            await self._pause_event.wait()

            try:
                await self._run_streaming_pull(config, write_queue)
            except grpc.aio.AioRpcError as e:
                if self._is_expected_stream_close(e):
                    logger.debug(
                        "streaming_pull.reconnect.expected_close",
                        extra={"subscription": subscription_path},
                    )
                    continue

                await self._resilience_state.record_failure()

                if not self._resilience_state.is_retryable(e):
                    logger.exception(
                        "streaming_pull.error.non_retryable",
                        extra={"subscription": subscription_path},
                        exc_info=e,
                    )
                    raise

                if not self._resilience_state.should_retry():
                    logger.error(
                        "streaming_pull.reconnect.exhausted",
                        extra={"subscription": subscription_path},
                    )
                    raise

                delay = self._resilience_state.calculate_delay()
                logger.warning(
                    "streaming_pull.reconnect.retry",
                    extra={"subscription": subscription_path, "delay": delay},
                    exc_info=e,
                )
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(
                    "streaming_pull.error.unexpected",
                    extra={"subscription": subscription_path},
                    exc_info=exc,
                )
                await asyncio.sleep(self._error_retry_delay)

        logger.debug(
            "streaming_pull.stop",
            extra={"subscription": subscription_path},
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
        write_queue: asyncio.Queue[StreamingPullRequest],
    ) -> AsyncIterator[StreamingPullRequest]:
        """Generate requests for the stream.

        Yields the initial subscription request, then forwards
        ack/nack/heartbeat requests from the write queue.
        """
        # Send initial request with subscription info
        yield StreamingPullRequest(
            subscription=config.subscription_path,
            stream_ack_deadline_seconds=self._stream_ack_deadline_seconds,
            client_id=self._client_id,
            max_outstanding_messages=self._concurrency_limit or 0,
            max_outstanding_bytes=0,
        )

        # Forward requests from the queue (acks, nacks, heartbeats)
        while not self._shutdown_event.is_set():
            try:
                request = await asyncio.wait_for(
                    write_queue.get(),
                    timeout=self._poll_interval,
                )
                yield request
            except asyncio.TimeoutError:
                continue

    async def _heartbeat_loop(
        self,
        write_queue: asyncio.Queue[StreamingPullRequest],
    ) -> None:
        """Send periodic heartbeats to keep the stream alive."""
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self._heartbeat_interval)
            if self._shutdown_event.is_set():
                break
            logger.debug("heartbeat.send")
            write_queue.put_nowait(
                StreamingPullRequest(
                    stream_ack_deadline_seconds=self._stream_ack_deadline_seconds,
                ),
            )

    def _create_received_message(
        self,
        received_msg: ReceivedMessage,
        config: ChannelConfig,
        write_queue: asyncio.Queue[StreamingPullRequest],
    ) -> PubsubReceivedMessage:
        """Create a PubsubReceivedMessage from a raw received message."""
        return PubsubReceivedMessage(
            raw_message=received_msg.message,  # type: ignore[arg-type]
            ack_id=received_msg.ack_id,
            delivery_attempt=received_msg.delivery_attempt,
            subscription_path=config.subscription_path,
            channel_name=config.channel,
            write_queue=write_queue,
            server=self._server,
        )

    async def _process_response(
        self,
        response: StreamingPullResponse,
        config: ChannelConfig,
        write_queue: asyncio.Queue[StreamingPullRequest],
    ) -> None:
        """Process a single StreamingPull response."""
        for received_msg in response.received_messages:
            if received_msg.message is None:
                continue
            message = self._create_received_message(received_msg, config, write_queue)
            self._in_flight_messages.add(message)
            await self._delivery_queue.put(
                QueuedDelivery(callback=config.callback, message=message),
            )

    async def _run_streaming_pull(
        self,
        config: ChannelConfig,
        write_queue: asyncio.Queue[StreamingPullRequest],
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

        iterator = self._request_iterator(config, write_queue)
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(write_queue))

        try:
            # Start the stream
            call = stream_method(iterator)

            # Process responses
            async for response in call:
                await self._resilience_state.record_success()

                if self._shutdown_event.is_set():
                    break

                await self._process_response(response, config, write_queue)
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

    async def _dispatch_loop(self) -> None:
        """Dispatch messages to callbacks."""
        try:
            while True:
                delivery = await self._delivery_queue.get()
                task = asyncio.create_task(self._execute_callback(delivery))
                self._callback_tasks.add(task)
                task.add_done_callback(self._callback_tasks.discard)
                self._delivery_queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            logger.debug("dispatcher.stop")

    async def _execute_callback(self, delivery: QueuedDelivery) -> None:
        """Execute a single callback."""
        try:
            await delivery.callback(delivery.message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "message.callback.error",
                exc_info=exc,
            )
        finally:
            # Remove from in-flight tracking (message was either acked/nacked by callback)
            self._in_flight_messages.discard(cast(PubsubReceivedMessage, delivery.message))

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
        self._is_active = False

    async def resume(self) -> None:
        """Resume message processing."""
        if self._shutdown_event.is_set():
            return
        self._pause_event.set()
        self._is_active = True

    async def _drain_write_queues(self, timeout: float = 1.0) -> None:
        """Drain pending write queue items before shutdown."""
        if not self._write_queues:
            return
        tasks = [
            asyncio.create_task(write_queue.join()) for write_queue in self._write_queues.values()
        ]
        _, unfinished = await asyncio.wait(
            tasks,
            return_when=asyncio.ALL_COMPLETED,
            timeout=timeout,
        )
        for task in unfinished:
            logger.warning("write_queue.flush.timeout")
            task.cancel()

    def _cancel_callback_tasks(self) -> list[asyncio.Task[None]]:
        """Cancel all pending callback tasks and return them."""
        if not self._callback_tasks:
            return []
        logger.warning(
            "subscriber.close.tasks_pending",
            extra={"count": len(self._callback_tasks)},
        )
        callbacks = list(self._callback_tasks)
        self._callback_tasks.clear()
        for task in callbacks:
            task.cancel()
        return callbacks

    async def close(self) -> None:
        """Close the subscriber and clean up."""
        if self._is_closing:
            return
        self._is_closing = True

        # First, pause to stop receiving new messages
        self._pause_event.clear()

        # Give a brief moment for current requests to finish
        await self._drain_write_queues()

        # Now signal shutdown
        self._shutdown_event.set()

        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

        self._is_active = False

        # Clean up remaining callback tasks
        callbacks = self._cancel_callback_tasks()
        if callbacks:
            with suppress(asyncio.CancelledError):
                await asyncio.wait(callbacks)
