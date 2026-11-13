from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable
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
    from repid.connections.abc import ReceivedMessageT
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
        server: PubsubServer,
        client_id: str | None = None,
        concurrency_limit: int | None = None,
        heartbeat_interval: float = 25.0,
        error_retry_delay: float = 1.0,
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

        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._shutdown_event = asyncio.Event()
        self._is_active = True
        self._is_closing = False

        self._delivery_queue: asyncio.Queue[QueuedDelivery] = asyncio.Queue()
        self._streaming_calls: set[
            grpc.aio.StreamStreamCall[StreamingPullRequest, StreamingPullResponse]
        ] = set()
        self._callback_tasks: set[asyncio.Task[None]] = set()
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
        server: PubsubServer,
        client_id: str | None = None,
        concurrency_limit: int | None = None,
    ) -> PubsubSubscriber:
        """Create and start a new subscriber."""
        subscriber = cls(
            channel=channel,
            channel_configs=channel_configs,
            credentials_provider=credentials_provider,
            resilience_state=resilience_state,
            stream_ack_deadline_seconds=stream_ack_deadline_seconds,
            server=server,
            client_id=client_id,
            concurrency_limit=concurrency_limit,
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
            *(
                asyncio.create_task(self._streaming_pull_loop(config))
                for config in self._channel_configs
            ),
            asyncio.create_task(self._dispatch_loop()),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done() and not task.cancelling():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

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
        hint = config.concurrency
        count = hint.messages if hint is not None else self._concurrency_limit
        payload_bytes = hint.payload_bytes if hint is not None else None
        yield StreamingPullRequest(
            subscription=config.subscription_path,
            stream_ack_deadline_seconds=self._stream_ack_deadline_seconds,
            client_id=self._client_id if self._client_id is not None else config.client_id,
            max_outstanding_messages=min(count or 0, 2**63 - 1),
            max_outstanding_bytes=min(payload_bytes or 0, 2**63 - 1),
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
            raw_message=received_msg.message,  # pyrefly: ignore[bad-argument-type]
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
        for message in self._received_messages(response, config):
            self._in_flight_messages.add(message)
            await self._delivery_queue.put(
                QueuedDelivery(callback=config.callback, message=message),
            )

    def _received_messages(
        self,
        response: StreamingPullResponse,
        config: ChannelConfig,
    ) -> Iterable[PubsubReceivedMessage]:
        return (
            self._create_received_message(message, config)
            for message in response.received_messages
            if message.message is not None
        )

    async def _run_streaming_pull(
        self,
        config: ChannelConfig,
    ) -> None:
        """Run a single StreamingPull session."""
        # Ensure credentials are valid
        await self._credentials_provider.ensure_valid()
        if not self._pause_event.is_set() or self._shutdown_event.is_set():
            return

        # Create the bidirectional stream
        stream_method = self._channel.stream_stream(
            STREAMING_PULL_METHOD,
            request_serializer=StreamingPullRequest.serialize,
            response_deserializer=StreamingPullResponse.deserialize,
        )

        iterator = self._request_iterator(config)

        # Start the stream
        call = stream_method(iterator)

        self._streaming_calls.add(call)
        try:
            async for response in call:
                if self._shutdown_event.is_set() or call not in self._streaming_calls:
                    await self._reject_unacted(self._received_messages(response, config))
                    break
                await self._process_response(response, config)
                await self._resilience_state.record_success()
        except asyncio.CancelledError:
            # A locally cancelled call ends this session without stopping the
            # subscriber. Cancellation of the reader task still propagates.
            reader = asyncio.current_task()
            if call in self._streaming_calls or (reader is not None and reader.cancelling()):
                raise
        finally:
            if call in self._streaming_calls:
                self._streaming_calls.remove(call)
                call.cancel()

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
            if not delivery.message.is_acted_on:
                with suppress(Exception):
                    await delivery.message.reject()
            raise
        except Exception as exc:
            logger.exception(
                "message.callback.error",
                exc_info=exc,
            )
            if not delivery.message.is_acted_on:
                with suppress(Exception):
                    await delivery.message.nack()
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
        """Stop pulling new messages while allowing settlement of existing deliveries."""
        if not self.is_active:
            return
        self._pause_streams()
        self._is_active = False

    def _pause_streams(self) -> None:
        self._pause_event.clear()
        calls, self._streaming_calls = self._streaming_calls, set()
        for call in calls:
            call.cancel()

    async def resume(self) -> None:
        """Resume message processing."""
        if self._shutdown_event.is_set():
            return
        self._pause_event.set()
        self._is_active = True

    def _cancel_callback_tasks(
        self,
        current: asyncio.Task[None] | None = None,
    ) -> list[asyncio.Task[None]]:
        """Cancel all pending callback tasks and return them."""
        callbacks = list(self._callback_tasks - {current})
        if not callbacks:
            return []
        logger.warning(
            "subscriber.close.tasks_pending",
            extra={"count": len(callbacks)},
        )
        self._callback_tasks.difference_update(callbacks)
        for task in callbacks:
            task.cancel()
        return callbacks

    async def close(self) -> None:
        """Close the subscriber and clean up."""
        if self._is_closing:
            return
        self._is_closing = True

        # First, pause to stop receiving new messages
        self._pause_streams()

        # Now signal shutdown
        self._shutdown_event.set()

        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

        self._is_active = False

        # Combine undelivered queue entries and submissions that never started.
        queued: set[ReceivedMessageT] = set()
        while not self._delivery_queue.empty():
            delivery = self._delivery_queue.get_nowait()
            queued.add(delivery.message)
            self._delivery_queue.task_done()

        # Clean up remaining callback tasks
        callbacks = self._cancel_callback_tasks(asyncio.current_task())
        if callbacks:
            await asyncio.wait(callbacks)

        # Final safety pass after callbacks had a chance to ack/reject on cancellation.
        queued.update(self._in_flight_messages)
        self._in_flight_messages.clear()
        await self._reject_unacted(queued)

    @staticmethod
    async def _reject_unacted(messages: Iterable[ReceivedMessageT]) -> None:
        await asyncio.gather(
            *(message.reject() for message in messages if not message.is_acted_on),
            return_exceptions=True,
        )
