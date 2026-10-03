from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from repid.connections._buffer import SubmissionBuffer
from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.protocol import ManagedSession, ReceiverLink
from repid.limits import UNLIMITED_NATIVE_FLOW, NativeFlow

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT


class AmqpSubscriber:
    """
    Implementation of SubscriberT for AmqpServer.

    This subscriber uses ManagedSession's ReceiverPool, which automatically
    handles reconnection and link recreation.

    Resolved channel count windows use AMQP link credit. Controlled dispatchers
    transfer delivery ownership to the runner, which reserves local intake and execution.
    """

    def __init__(
        self,
        *,
        managed_session: ManagedSession,
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
        paused_event: asyncio.Event,
        naming_strategy: Callable[[str], str],
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self._managed_session = managed_session
        self._queues_to_callbacks = queues_to_callbacks
        self._native_flow = native_flow
        self._finished = False
        self._stopped = False
        self._is_active = True
        self._paused_event = paused_event
        self._naming_strategy = naming_strategy
        self._stop_event = stop_event if stop_event is not None else asyncio.Event()
        self._dispatch_tasks: list[asyncio.Task] = []
        self._buffers: list[SubmissionBuffer] = []

        # Start the background task that monitors the connection
        # Note: With ManagedSession, reconnection is handled automatically by ReceiverPool
        self._task = asyncio.create_task(self._run_forever())

    @property
    def native_flow(self) -> NativeFlow:
        return self._native_flow

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def task(self) -> asyncio.Task:
        return self._task

    async def pause(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("AMQP supports worker pause only")
        self._is_active = False
        self._paused_event.clear()
        await self._managed_session.receiver_pool.pause_intake(
            [self._naming_strategy(queue) for queue in self._queues_to_callbacks],
        )

    async def resume(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("AMQP supports worker pause only")
        self._is_active = True
        await self._managed_session.receiver_pool.resume_intake(
            [self._naming_strategy(queue) for queue in self._queues_to_callbacks],
        )
        self._paused_event.set()

    async def _run_forever(self) -> None:
        stopped = asyncio.create_task(self._stop_event.wait())
        try:
            done, _ = await asyncio.wait(
                {stopped, *self._dispatch_tasks},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done - {stopped}:
                error = None if task.cancelled() else task.exception()
                if error is not None:
                    raise error
                if not self._stop_event.is_set():
                    raise RuntimeError("AMQP delivery dispatcher stopped unexpectedly")
        finally:
            stopped.cancel()
            await asyncio.gather(stopped, return_exceptions=True)

    @classmethod
    async def create(
        cls,
        *,
        managed_session: ManagedSession,
        queues_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
        naming_strategy: Callable[[str], str],
        publish_fn: Callable[..., Coroutine[Any, Any, None]],
    ) -> AmqpSubscriber:
        """
        Create a new subscriber.

        Args:
            managed_session: The managed session to use
            queues_to_callbacks: Mapping of queue names to callback functions
            native_flow: Resolved native windows; channel counts set link credit.
            naming_strategy: Function to convert queue names to AMQP addresses

        Returns:
            A new AmqpSubscriber instance
        """
        dispatchers: list[asyncio.Task] = []
        buffers: list[SubmissionBuffer] = []
        receiver_pool = managed_session.receiver_pool

        paused_event = asyncio.Event()
        paused_event.set()
        stopped_event = asyncio.Event()
        addresses: list[str] = []

        try:
            for queue, callback in queues_to_callbacks.items():
                window = native_flow.channels.get(queue)
                prefetch = (
                    window.max_messages if window and window.max_messages is not None else 100
                )
                deliveries: asyncio.Queue = asyncio.Queue(maxsize=prefetch)
                buffer = SubmissionBuffer()
                buffers.append(buffer)

                async def dispatch(
                    deliveries: asyncio.Queue = deliveries,
                    callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]] = callback,
                    buffer: SubmissionBuffer = buffer,
                ) -> None:
                    while True:
                        message = await deliveries.get()
                        await paused_event.wait()
                        await buffer.submit(message, callback)

                dispatchers.append(asyncio.create_task(buffer.run(dispatch)))

                # Create wrapper callback that handles the message
                async def wrapped_callback(  # noqa: PLR0917
                    payload: bytes,
                    headers: dict[str, Any] | None,
                    delivery_id: int,
                    delivery_tag: bytes,
                    link_ref: ReceiverLink,
                    properties: Properties | None = None,
                    queue: str = queue,
                    managed_session: ManagedSession = managed_session,
                    deliveries: asyncio.Queue = deliveries,
                    buffer: SubmissionBuffer = buffer,
                ) -> None:
                    link_ref.defer_delivery_credit(delivery_id)

                    msg = AmqpReceivedMessage(
                        payload=payload,
                        headers=headers,
                        link=link_ref,
                        delivery_id=delivery_id,
                        delivery_tag=delivery_tag,
                        channel_name=queue,
                        managed_session=managed_session,
                        publish_fn=publish_fn,
                        properties=properties,
                    )
                    try:
                        if stopped_event.is_set() or deliveries.full():
                            # A replacement link has fresh credit while old deliveries can
                            # still fill this queue. Release overflow instead of stranding it.
                            await msg.reject()
                            return
                        buffer.track((msg,))
                        deliveries.put_nowait(msg)
                    except Exception as exc:  # noqa: BLE001
                        # ReceiverLink catches callback errors; report them to our supervisor.
                        buffer.fail(exc)

                # Subscribe using the receiver pool (handles reconnection automatically)
                address = naming_strategy(queue)
                addresses.append(address)
                await receiver_pool.subscribe(
                    address,
                    wrapped_callback,
                    f"receiver-{queue}",
                    prefetch=prefetch,
                )

        except BaseException:
            for dispatcher in dispatchers:
                dispatcher.cancel()
            await asyncio.gather(*dispatchers, return_exceptions=True)
            await asyncio.gather(*(buffer.dispose() for buffer in buffers), return_exceptions=True)
            await asyncio.gather(
                *(receiver_pool.unsubscribe(address) for address in addresses),
                return_exceptions=True,
            )
            raise

        subscriber = cls(
            managed_session=managed_session,
            queues_to_callbacks=queues_to_callbacks,
            native_flow=native_flow,
            paused_event=paused_event,
            naming_strategy=naming_strategy,
            stop_event=stopped_event,
        )
        subscriber._dispatch_tasks = dispatchers
        subscriber._buffers = buffers
        return subscriber

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._stop_event.set()
        errors: list[BaseException] = []
        try:
            await self.pause()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        self._task.cancel()
        for task in self._dispatch_tasks:
            task.cancel()
        results = await asyncio.gather(self._task, *self._dispatch_tasks, return_exceptions=True)
        results.extend(
            await asyncio.gather(
                *(buffer.dispose() for buffer in self._buffers),
                return_exceptions=True,
            ),
        )
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                errors.append(result)
        if errors:
            raise errors[0]

    async def finish(self) -> None:
        try:
            await self.stop()
        finally:
            if not self._finished:
                results = await asyncio.gather(
                    *(
                        self._managed_session.receiver_pool.unsubscribe(
                            self._naming_strategy(queue),
                        )
                        for queue in self._queues_to_callbacks
                    ),
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
                self._finished = True
