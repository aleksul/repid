from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import grpc
import grpc.aio

from repid.connections._buffer import SubmissionBuffer, stop_task
from repid.limits import UNLIMITED_NATIVE_FLOW, NativeFlow

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
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
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
        self._native_flow = native_flow
        self._buffer = SubmissionBuffer()
        self._finished = False
        self._server = server
        self._heartbeat_interval = heartbeat_interval
        self._error_retry_delay = error_retry_delay

        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._shutdown_event = asyncio.Event()
        self._is_active = True
        self._is_closing = False

        self._delivery_queue: asyncio.Queue[QueuedDelivery] = asyncio.Queue(maxsize=100)
        self._task: asyncio.Task[None] | None = None

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
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
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
            native_flow=native_flow,
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
        await self._buffer.run(self._process_intake)

    async def _process_intake(self) -> None:
        """Main background processing loop."""
        tasks = [
            *(self._streaming_pull_loop(config) for config in self._channel_configs),
            self._dispatch_loop(),
        ]
        background = [asyncio.create_task(task) for task in tasks]
        try:
            await asyncio.gather(*background)
        finally:
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)

    async def _streaming_pull_loop(self, config: ChannelConfig) -> None:
        """StreamingPull loop for a single subscription with resilience."""
        while not self._shutdown_event.is_set():
            await self._pause_event.wait()

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
            max_outstanding_messages=1000,
            max_outstanding_bytes=0,
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
        deliveries = [
            QueuedDelivery(
                callback=config.callback,
                message=self._create_received_message(raw, config),
            )
            for raw in response.received_messages
            if raw.message is not None
        ]
        self._buffer.track(delivery.message for delivery in deliveries)
        for delivery in deliveries:
            await self._pause_event.wait()
            await self._delivery_queue.put(delivery)

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
        while True:
            delivery = await self._delivery_queue.get()
            await self._pause_event.wait()
            try:
                await self._execute_callback(delivery)
            finally:
                self._delivery_queue.task_done()

    async def _execute_callback(self, delivery: QueuedDelivery) -> None:
        if id(delivery.message) not in self._buffer.owned:
            self._buffer.track((delivery.message,))
        await self._buffer.submit(delivery.message, delivery.callback)

    @property
    def native_flow(self) -> NativeFlow:
        return self._native_flow

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

    async def pause(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("Pub/Sub supports worker pause only")
        """Pause message processing."""
        if not self.is_active:
            return
        self._pause_event.clear()
        self._is_active = False

    async def resume(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("Pub/Sub supports worker pause only")
        """Resume message processing."""
        if self._shutdown_event.is_set():
            return
        self._pause_event.set()
        self._is_active = True

    async def stop(self) -> None:
        if self._is_closing:
            return
        self._is_closing = True
        self._shutdown_event.set()
        self._is_active = False
        try:
            if self._task is not None:
                await stop_task(self._task)
        finally:
            try:
                await self._buffer.dispose()
            finally:
                while not self._delivery_queue.empty():
                    self._delivery_queue.get_nowait()
                    self._delivery_queue.task_done()

    async def finish(self) -> None:
        await self.stop()
        self._finished = True
