from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from repid.connections.abc import (
    CapabilitiesT,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
    SubscriberT,
)
from repid.connections.in_memory.utils import DummyQueue
from repid.logger import logger

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag


class InMemorySentMessage(SentMessageT):
    def __init__(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        content_type: str | None = "text/plain",
        message_id: str | None = None,
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._correlation_id = correlation_id
        self._content_type = content_type
        self._message_id = message_id

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def headers(self) -> dict[str, str] | None:
        return self._headers

    @property
    def correlation_id(self) -> str | None:
        return self._correlation_id

    @property
    def content_type(self) -> str | None:
        return self._content_type

    @property
    def message_id(self) -> str | None:
        return self._message_id


class InMemoryReceivedMessage(ReceivedMessageT):
    def __init__(self, message: DummyQueue.Message, queue: DummyQueue, channel: str) -> None:
        self._message = message
        self._queue = queue
        self._channel = channel
        self._acted_on = False

    @property
    def payload(self) -> bytes:
        return self._message.payload

    @property
    def headers(self) -> dict[str, str] | None:
        return self._message.headers

    @property
    def content_type(self) -> str | None:
        return self._message.content_type

    @property
    def message_id(self) -> str | None:
        return self._message.message_id

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def is_acted_on(self) -> bool:
        return self._acted_on

    async def ack(self) -> None:
        if self._acted_on:
            return
        self._acted_on = True
        self._queue.processing.remove(self._message)

    async def nack(self) -> None:
        if self._acted_on:
            return
        self._acted_on = True
        self._queue.processing.remove(self._message)

    async def reject(self) -> None:
        if self._acted_on:
            return
        self._acted_on = True
        self._queue.processing.remove(self._message)
        self._queue.queue.put_nowait(self._message)

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> None:
        if self._acted_on:
            return
        self._acted_on = True
        # if channel specified, send there; otherwise to original channel
        target_channel = channel or self._channel
        self._queue.queue.put_nowait(
            DummyQueue.Message(
                payload=payload,
                headers=headers,
                content_type=content_type,
                message_id=str(uuid4()),
            ),
        )
        # if different channel requested and not existing create queue and enqueue
        if target_channel != self._channel:
            # naive cross-channel: just ensure a queue exists (cannot access server queues here)
            # so we fallback to current queue (document limitation)
            pass


class InMemorySubscriber(SubscriberT):
    """Represents a subscription over one or more channels.

    Provides pause/resume and close lifecycle controls. A single supervisor task
    is exposed via the ``task`` property that awaits all per-channel consumer tasks.
    """

    def __init__(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        queues: dict[str, DummyQueue],
        concurrency_limit: int | None,
    ) -> None:
        self._channels_to_callbacks = channels_to_callbacks
        self._queues = queues
        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()  # start in resumed state
        self._channel_tasks: dict[str, asyncio.Task] = {}
        self._semaphore: asyncio.Semaphore | None = (
            asyncio.Semaphore(concurrency_limit)
            if concurrency_limit and concurrency_limit > 0
            else None
        )
        self._callback_tasks: set[asyncio.Task] = set()
        self._supervisor_task = asyncio.create_task(self._supervisor())

    # --- SubscriberT protocol ---
    @property
    def is_active(self) -> bool:
        return not self._closed and any(not t.done() for t in self._channel_tasks.values())

    @property
    def task(self) -> asyncio.Task:
        return self._supervisor_task

    async def pause(self) -> None:
        self._paused_event.clear()

    async def resume(self) -> None:
        self._paused_event.set()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for t in self._channel_tasks.values():
            if not t.done():
                t.cancel()
        if not self._supervisor_task.done():
            self._supervisor_task.cancel()
        # Best-effort gather
        try:
            await asyncio.gather(*self._channel_tasks.values(), return_exceptions=True)
        finally:
            with contextlib.suppress(Exception):
                await asyncio.sleep(0)

    # --- internal helpers ---
    async def _supervisor(self) -> None:
        # Start channel consumer tasks lazily here so that _semaphore is ready
        for channel, callback in self._channels_to_callbacks.items():
            queue = self._queues[channel]
            self._channel_tasks[channel] = asyncio.create_task(
                self._channel_consumer(channel=channel, queue=queue, callback=callback),
            )
        try:
            await asyncio.gather(*self._channel_tasks.values())
        except asyncio.CancelledError:
            for t in self._channel_tasks.values():
                if not t.done():
                    t.cancel()
            raise

    async def _channel_consumer(
        self,
        *,
        channel: str,
        queue: DummyQueue,
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]],
    ) -> None:
        while True:
            await self._paused_event.wait()
            msg = await queue.queue.get()
            received_msg = InMemoryReceivedMessage(msg, queue, channel)
            queue.processing.add(msg)

            if self._semaphore is not None:
                await self._semaphore.acquire()

            async def _run_callback(rm: ReceivedMessageT = received_msg) -> None:
                try:
                    await callback(rm)
                finally:
                    if self._semaphore is not None:
                        self._semaphore.release()

            task = asyncio.create_task(_run_callback())
            self._callback_tasks.add(task)
            task.add_done_callback(self._callback_tasks.discard)


class InMemoryServer(ServerT):
    def __init__(
        self,
        *,
        title: str | None = "In-Memory Server",
        summary: str | None = "In-memory message broker for testing and development",
        description: str
        | None = "A simple in-memory message broker that implements the ServerT protocol",
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
    ) -> None:
        self._title = title
        self._summary = summary
        self._description = description
        self._tags = tags
        self._external_docs = external_docs
        self.queues: dict[str, DummyQueue] = {}
        self._connected = False
        # Track active subscribers to keep them alive
        self._subscribers: set[InMemorySubscriber] = set()

    @property
    def host(self) -> str:
        return "localhost"

    @property
    def protocol(self) -> str:
        return "in-memory"

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
        return "1.0.0"

    @property
    def variables(self) -> Mapping[str, ServerVariable] | None:
        return None

    @property
    def security(self) -> Sequence[Any] | None:
        return None

    @property
    def tags(self) -> Sequence[Tag] | None:
        return self._tags

    @property
    def external_docs(self) -> ExternalDocs | None:
        return self._external_docs

    @property
    def bindings(self) -> ServerBindingsObject | None:
        return None

    @property
    def capabilities(self) -> CapabilitiesT:
        return {
            "supports_acknowledgments": True,
            "supports_persistence": False,
            "supports_reply": True,
            "supports_lightweight_pause": True,
        }

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        logger.info("Connecting to in-memory server.")
        self._connected = True
        await asyncio.sleep(0)

    async def disconnect(self) -> None:
        logger.info("Disconnecting from in-memory server.")
        self._connected = False
        await asyncio.sleep(0)

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[ServerT, None]:
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
        if not self._connected:
            raise RuntimeError("Server is not connected")

        if channel not in self.queues:
            self.queues[channel] = DummyQueue()

        if (
            server_specific_parameters is not None
            and "message_id" in server_specific_parameters
            and isinstance(server_specific_parameters["message_id"], str)
            and server_specific_parameters["message_id"]
        ):
            message_id = server_specific_parameters["message_id"]
        else:
            message_id = str(uuid4())

        msg = DummyQueue.Message(
            payload=message.payload,
            headers=message.headers,
            content_type=message.content_type,
            message_id=message_id,
        )
        self.queues[channel].queue.put_nowait(msg)

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        if not self._connected:
            raise RuntimeError("Server is not connected")

        # Ensure queues exist for all channels
        for channel in channels_to_callbacks:
            if channel not in self.queues:
                self.queues[channel] = DummyQueue()

        subscriber = InMemorySubscriber(
            channels_to_callbacks=channels_to_callbacks,
            queues={ch: self.queues[ch] for ch in channels_to_callbacks},
            concurrency_limit=concurrency_limit,
        )
        self._subscribers.add(subscriber)

        # Remove from set when supervisor task finishes
        def _on_done(_task: asyncio.Task) -> None:
            self._subscribers.discard(subscriber)

        subscriber.task.add_done_callback(_on_done)
        return subscriber
