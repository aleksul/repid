from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import TYPE_CHECKING, cast

import grpc.aio

from repid.logger import logger

from ._helpers import ChannelConfig, QueuedDelivery
from .proto import StreamingPullRequest, StreamingPullResponse
from .received_message import PubsubReceivedMessage
from .resilience import ResilienceState

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
    ) -> None:
        self._channel = channel
        self._channel_configs = channel_configs
        self._credentials_provider = credentials_provider
        self._resilience_state = resilience_state
        self._stream_ack_deadline_seconds = stream_ack_deadline_seconds
        self._client_id = client_id
        self._concurrency_limit = concurrency_limit
        self._server = server

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
        self._streams: dict[str, tuple[asyncio.Task[None], asyncio.Task[None]]] = {}

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
                await self._resilience_state.record_failure()

                if not self._resilience_state.is_retryable(e):
                    logger.exception(
                        "Non-retryable error in StreamingPull.",
                        extra={"subscription": subscription_path},
                        exc_info=e,
                    )
                    raise

                if not self._resilience_state.should_retry():
                    logger.error(
                        "Max reconnection attempts exhausted for StreamingPull.",
                        extra={"subscription": subscription_path},
                    )
                    raise

                delay = self._resilience_state.calculate_delay()
                logger.warning(
                    "StreamingPull error, reconnecting in {delay:.2f}s.",
                    extra={"subscription": subscription_path, "delay": delay},
                    exc_info=e,
                )
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Unexpected error in StreamingPull.",
                    extra={"subscription": subscription_path},
                    exc_info=exc,
                )
                await asyncio.sleep(1.0)

        logger.debug(
            "Stopping StreamingPull.",
            extra={"subscription": subscription_path},
        )

    async def _run_streaming_pull(
        self,
        config: ChannelConfig,
        write_queue: asyncio.Queue[StreamingPullRequest],
    ) -> None:
        """Run a single StreamingPull session."""
        subscription_path = config.subscription_path

        # Ensure credentials are valid
        await self._credentials_provider.ensure_valid()

        # Create the bidirectional stream
        stream_method = self._channel.stream_stream(  # type: ignore[var-annotated]
            STREAMING_PULL_METHOD,
            request_serializer=lambda req: req.serialize(),
            response_deserializer=StreamingPullResponse.deserialize,
        )

        async def request_iterator() -> AsyncIterator[StreamingPullRequest]:
            """Generate requests for the stream."""
            # Send initial request with subscription info
            initial_request = StreamingPullRequest(
                subscription=subscription_path,
                stream_ack_deadline_seconds=self._stream_ack_deadline_seconds,
                client_id=self._client_id,
                max_outstanding_messages=self._concurrency_limit or 0,
                max_outstanding_bytes=0,
            )
            yield initial_request

            # Then yield ack/nack requests from the queue
            while not self._shutdown_event.is_set():
                try:
                    request = await asyncio.wait_for(
                        write_queue.get(),
                        timeout=1.0,
                    )
                    yield request
                except asyncio.TimeoutError:
                    continue

        # Start the stream
        call = stream_method(request_iterator())

        # Process responses
        async for response in call:
            await self._resilience_state.record_success()

            if self._shutdown_event.is_set():
                break

            for received_msg in response.received_messages:
                if received_msg.message is None:
                    continue

                message = PubsubReceivedMessage(
                    raw_message=received_msg.message,
                    ack_id=received_msg.ack_id,
                    delivery_attempt=received_msg.delivery_attempt,
                    subscription_path=subscription_path,
                    channel_name=config.channel,
                    write_queue=write_queue,
                    server=self._server,
                )
                self._in_flight_messages.add(message)
                await self._delivery_queue.put(
                    QueuedDelivery(callback=config.callback, message=message),
                )

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
            logger.debug("Stopping Pub/Sub dispatcher.")

    async def _execute_callback(self, delivery: QueuedDelivery) -> None:
        """Execute a single callback."""
        try:
            await delivery.callback(delivery.message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Error inside Pub/Sub channel callback.",
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

    async def close(self) -> None:
        """Close the subscriber and clean up."""
        if self._is_closing:
            return
        self._is_closing = True

        # First, pause to stop receiving new messages
        self._pause_event.clear()

        # Give a brief moment for current requests to finish
        _, unfinished = await asyncio.wait(
            [
                asyncio.create_task(write_queue.join())
                for write_queue in self._write_queues.values()
            ],
            return_when=asyncio.ALL_COMPLETED,
            timeout=1.0,
        )
        for task in unfinished:
            logger.warning(
                "Write queue not fully flushed before subscriber shutdown.",
            )
            task.cancel()

        # Now signal shutdown
        self._shutdown_event.set()

        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

        self._is_active = False

        # Clean up remaining callback tasks
        if self._callback_tasks:
            logger.warning(
                "There are still %d pending Pub/Sub callback tasks during subscriber shutdown.",
                len(self._callback_tasks),
            )
            callbacks = self._callback_tasks.copy()
            self._callback_tasks.clear()
            for task in callbacks:
                task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.wait(callbacks)
