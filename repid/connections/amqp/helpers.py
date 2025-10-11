from __future__ import annotations

import asyncio
import time
from collections import UserDict
from collections.abc import Callable, Coroutine
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

from aiormq.abc import Basic as BasicAbc

from repid.logger import logger

if TYPE_CHECKING:
    import aiormq

    from repid.connections.abc import ReceivedMessageT, SentMessageT

    from .message_broker import AmqpServer


class AiormqConsumers(UserDict):
    """A special dictionary which informs Repid's RabbitMQ consumer
    that aiormq has removed consumer's callback from the channel."""

    __marker = object()

    def pop(self, key: str, default: Any = __marker) -> Any:
        try:
            value = self[key]
        except KeyError:  # pragma: no cover
            if default is self.__marker:
                raise
            return default
        else:
            cast("AiormqMessageHandler", value.__self__).on_cancel.set()
            del self[key]
            return value


class AiormqMessageHandler:
    def __init__(
        self,
        channel: str,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        server: AmqpServer,
        on_cancel: asyncio.Event,
    ) -> None:
        self.channel = channel
        self.callback = callback
        self.server = server
        self.on_cancel = on_cancel

    async def __call__(self, message: aiormq.abc.DeliveredMessage) -> None:
        await self.callback(
            AmqpReceivedMessage(
                message=message,
                channel_name=self.channel,
                server=self.server,
            ),
        )


class AmqpReceivedMessage:
    """Implementation of ReceivedMessageT for AmqpServer."""

    def __init__(
        self,
        *,
        message: aiormq.abc.DeliveredMessage,
        channel_name: str,
        server: AmqpServer,
    ) -> None:
        self._message = message
        self._channel_name = channel_name
        self._server = server
        self._is_acted_on = False

    @property
    def payload(self) -> bytes:
        return self._message.body

    @property
    def headers(self) -> dict[str, str] | None:
        if self._message.header.properties.headers:
            return {k: str(v) for k, v in self._message.header.properties.headers.items()}
        return None

    @property
    def content_type(self) -> str | None:
        return self._message.header.properties.content_type

    @property
    def channel(self) -> str:
        return self._channel_name

    @property
    def is_acted_on(self) -> bool:
        return self._is_acted_on

    async def ack(self) -> None:
        if self._is_acted_on:
            return
        if self._message.delivery_tag is not None:
            await self._message.channel.basic_ack(self._message.delivery_tag)
        self._is_acted_on = True

    async def nack(self) -> None:
        if self._is_acted_on:
            return
        if self._message.delivery_tag is not None:
            await self._message.channel.basic_nack(self._message.delivery_tag, requeue=False)
        self._is_acted_on = True

    async def reject(self) -> None:
        if self._is_acted_on:
            return
        if self._message.delivery_tag is not None:
            await self._message.channel.basic_reject(self._message.delivery_tag, requeue=True)
        self._is_acted_on = True

    async def reply(
        self,
        *,
        message: SentMessageT,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        reply_channel = channel or self._channel_name
        await self._server.publish(
            channel=reply_channel,
            message=message,
            server_specific_parameters=server_specific_parameters,
        )


class AmqpSubscriber:
    """Implementation of SubscriberT for AmqpServer."""

    def __init__(
        self,
        *,
        server: AmqpServer,
        consumer_channel: aiormq.abc.AbstractChannel,
        consumer_tags: dict[str, str],
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
        on_consumer_cancel: asyncio.Event,
    ) -> None:
        self._server = server
        self._consumer_channel = consumer_channel
        self._consumer_tags = consumer_tags
        self._queues_to_callbacks = queues_to_callbacks
        self._concurrency_limit = concurrency_limit
        self._is_active = True
        self._on_consumer_cancel = on_consumer_cancel
        self._is_closing = False
        self._recreate_lock = asyncio.Lock()
        self._last_recreate_time = 0.0
        self._recreate_backoff = 1.0
        self._max_backoff = 60.0

        # Start monitoring task
        self._task: asyncio.Task = asyncio.create_task(self._monitor_health())

    async def _monitor_health(self) -> None:
        """Monitor channel health and recreate if necessary"""
        channel_close_task: asyncio.Task | None = None
        consumer_cancel_task: asyncio.Task | None = None

        try:
            while not self._is_closing:
                try:
                    channel_close_task = asyncio.create_task(
                        self._consumer_channel._Channel__close_event.wait(),  # type: ignore[attr-defined]
                    )
                    consumer_cancel_task = asyncio.create_task(self._on_consumer_cancel.wait())

                    # Wait for either consumer cancel or channel closing
                    _, pending = await asyncio.wait(
                        [
                            consumer_cancel_task,
                            channel_close_task,
                        ],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    # Cancel pending tasks
                    for task in pending:
                        if isinstance(task, asyncio.Task):
                            task.cancel()
                            with suppress(asyncio.CancelledError):
                                await task

                    # Clear task references
                    channel_close_task = None
                    consumer_cancel_task = None

                    # If we're closing, exit
                    if self._is_closing:
                        break

                    # Check which event triggered
                    # Only recreate if the channel actually closed (not just consumer cancel)
                    if self._consumer_channel.is_closed:
                        logger.warning("Channel closed unexpectedly, recreating...")
                        await self._recreate_channel()
                    else:
                        # Consumer was cancelled by broker, recreate anyway
                        logger.warning("Consumer cancelled by broker, recreating channel...")
                        await self._recreate_channel()

                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    # Log error but continue monitoring
                    logger.exception(
                        "Error in subscriber health monitoring",
                        exc_info=exc,
                    )
                    await asyncio.sleep(1)
        finally:
            # Clean up any pending tasks on exit
            for cancellation_task in [channel_close_task, consumer_cancel_task]:
                if cancellation_task is not None and not cancellation_task.done():
                    cancellation_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await cancellation_task

    async def _setup_channel(
        self,
        channel: aiormq.abc.AbstractChannel,
        on_consumer_cancel: asyncio.Event,
    ) -> dict[str, str]:
        """Set up a channel with QoS, consumers, and return consumer tags.

        This method is used by both create() and _recreate_channel() to ensure
        consistent channel setup following DRY principles.
        """
        # Patch AiormqConsumers into the channel
        channel.consumers = AiormqConsumers()  # type: ignore[attr-defined]

        # Set QoS (prefetch count) for concurrency limit
        if self._concurrency_limit is not None:
            await channel.basic_qos(prefetch_count=self._concurrency_limit)

        # Set up consumers on the channel
        consumer_tags: dict[str, str] = {}
        for queue_name, callback in self._queues_to_callbacks.items():
            try:
                handler = AiormqMessageHandler(
                    queue_name,
                    callback,
                    self._server,
                    on_cancel=on_consumer_cancel,
                )
                result = await channel.basic_consume(
                    queue_name,
                    handler,
                    no_ack=False,
                )
            except Exception as exc:
                # Cleanup previously started consumers
                for tag in consumer_tags.values():
                    with suppress(Exception):
                        await channel.basic_cancel(tag)
                raise ConnectionError(
                    f"Failed to subscribe to queue '{queue_name}': {exc}",
                ) from exc

            if isinstance(result, BasicAbc.ConsumeOk) and result.consumer_tag:
                consumer_tags[queue_name] = result.consumer_tag
            else:
                # Cleanup previously started consumers
                for tag in consumer_tags.values():
                    with suppress(Exception):
                        await channel.basic_cancel(tag)
                raise ConnectionError(f"Failed to start consuming from queue {queue_name}.")

        return consumer_tags

    async def _recreate_channel(self) -> None:
        """Recreate the channel with exponential backoff"""
        async with self._recreate_lock:
            # Don't recreate if we're closing
            if self._is_closing:
                return

            # Check if server is still connected
            if not self._server.is_connected:
                logger.warning(
                    "Server connection is closed, not recreating channel",
                )
                return

            # Implement exponential backoff to prevent rapid reconnection
            current_time = time.monotonic()
            time_since_last_recreate = current_time - self._last_recreate_time

            if time_since_last_recreate < self._recreate_backoff:
                # Wait for backoff period
                wait_time = self._recreate_backoff - time_since_last_recreate
                logger.info(
                    "Backing off before recreating channel",
                    extra={"wait_time": wait_time},
                )
                await asyncio.sleep(wait_time)

                # Check again if we're closing after the sleep
                if self._is_closing:
                    return

                # Increase backoff for next time
                self._recreate_backoff = min(self._recreate_backoff * 2, self._max_backoff)
            else:
                # Reset backoff if enough time has passed
                self._recreate_backoff = 1.0

            self._last_recreate_time = time.monotonic()

            # Close old channel if it's still open
            if self._consumer_channel is not None and not self._consumer_channel.is_closed:
                with suppress(Exception):
                    await self._consumer_channel.close()

            # Create new channel
            try:
                # Server connection should not be None at this point due to is_connected check
                assert self._server._connection is not None  # noqa: S101
                channel = await self._server._connection.channel()

                # Create new shared event for consumer cancellation
                self._on_consumer_cancel = asyncio.Event()

                # Set up the channel (QoS, consumers, etc.) using shared method
                new_consumer_tags = await self._setup_channel(channel, self._on_consumer_cancel)

                # Successfully recreated, update state
                self._consumer_channel = channel
                self._consumer_tags = new_consumer_tags
                logger.info("Successfully recreated channel and consumers")

            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to recreate channel", exc_info=exc)
                # Will retry on next iteration

    @classmethod
    async def create(
        cls,
        *,
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
        server: AmqpServer,
    ) -> AmqpSubscriber:
        """Create a RabbitSubscriber by setting up consumers for all channels."""

        # Create a new channel for this subscriber
        if not server.is_connected or server._connection is None:
            raise ConnectionError("Server is not connected to RabbitMQ")

        consumer_channel = await server._connection.channel()

        # Create shared event for consumer cancellation
        on_consumer_cancel = asyncio.Event()

        # Create a temporary instance to use the shared setup method
        temp_instance = cls(
            server=server,
            consumer_channel=consumer_channel,
            consumer_tags={},  # Will be populated by _setup_channel
            queues_to_callbacks=queues_to_callbacks,
            concurrency_limit=concurrency_limit,
            on_consumer_cancel=on_consumer_cancel,
        )

        try:
            # Use the shared channel setup method
            consumer_tags = await temp_instance._setup_channel(consumer_channel, on_consumer_cancel)

            # Update the consumer tags
            temp_instance._consumer_tags = consumer_tags

            return temp_instance

        except Exception as exc:
            # Clean up on error
            if not temp_instance._task.done():
                temp_instance._task.cancel()
                with suppress(asyncio.CancelledError):
                    await temp_instance._task
            with suppress(Exception):
                await consumer_channel.close()
            raise ConnectionError(
                f"Failed to create RabbitMQ subscriber: {exc}",
            ) from exc

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def task(self) -> asyncio.Task:
        return self._task

    async def _set_consumer_qos(self, prefetch_count: int | None) -> None:
        """Set QoS for the consumer channel (used for pause/resume)"""
        if self._consumer_channel is not None and not self._consumer_channel.is_closed:
            # If prefetch_count is None, use 0 which means unlimited in RabbitMQ
            await self._consumer_channel.basic_qos(prefetch_count=prefetch_count or 0)

    async def _cancel_consumer(self, consumer_tag: str) -> None:
        """Cancel a consumer"""
        if self._consumer_channel is not None and not self._consumer_channel.is_closed:
            await self._consumer_channel.basic_cancel(consumer_tag)

    async def pause(self) -> None:
        """Pause message consumption by limiting concurrency to 1"""
        if not self._is_active or self._is_closing:
            return
        # Limit concurrency to 1 to effectively pause new message processing
        await self._set_consumer_qos(prefetch_count=1)
        self._is_active = False

    async def resume(self) -> None:
        """Resume message consumption by restoring original concurrency limit"""
        if self._is_active or self._is_closing:
            return
        # Restore original concurrency limit (or use default if None)
        await self._set_consumer_qos(prefetch_count=self._concurrency_limit)
        self._is_active = True

    async def close(self) -> None:
        """Close the subscriber and cancel all consumers"""
        # Signal that we're closing
        self._is_closing = True
        self._is_active = False

        # Cancel and wait for the monitoring task FIRST
        # This prevents it from interfering with channel cleanup
        if not self._task.done():
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

        # Clear consumer tags (no need to cancel them individually)
        # The channel close will clean them up
        self._consumer_tags.clear()

        # Close the channel (this will automatically cancel all consumers)
        if self._consumer_channel is not None:
            # Check if channel is not already closed/closing
            try:
                if not self._consumer_channel.is_closed:
                    with suppress(Exception):
                        await self._consumer_channel.close()
            except (AttributeError, RuntimeError):
                # Channel object may be in an invalid state, ignore
                pass
