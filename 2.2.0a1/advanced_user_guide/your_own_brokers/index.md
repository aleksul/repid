# Your own brokers

Repid's architecture makes it easy to plug in your own message brokers. To create a custom broker, you need to implement a class that adheres to the `ServerT` protocol, defined in `repid.connections.abc`.

Compatibility

We try our best to preserve custom broker compatibility, but broker protocols may change between minor releases. Check the release notes when upgrading and keep custom broker implementations covered by integration tests.

## The `ServerT` Protocol

At its core, a server (broker implementation) must handle the connection lifecycle, message publishing, and message consumption (subscribing).

Here is a simplified overview of what you need to implement:

```
import asyncio
from typing import Mapping, Sequence, Callable, Coroutine, Any
from contextlib import AbstractAsyncContextManager
from repid.connections import SubscriberDispatcher
from repid.connections.abc import (
    CapabilitiesT,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
    SubscriberT,
)


class MyCustomServer:
    # 1. Server Metadata Properties for AsyncAPI
    @property
    def host(self) -> str:
        return "my-broker-host"

    @property
    def protocol(self) -> str:
        return "my-custom-protocol"

    # (other properties like title, summary, tags, variables, etc.
    # can return None or empty defaults)

    # 2. Capabilities
    @property
    def capabilities(self) -> CapabilitiesT:
        return {
            "supports_native_reply": False,
            "supports_keep_alive": False,
            # Whether the broker supports stopping intake
            # in such a way that it causes less burden than reconnection
            "supports_pause": False,
            "supports_pause_per_channel": False,
            # Whether the broker supports limiting the intake
            # of message count/payload size worker-wide and per channel
            "supports_native_message_flow_control": False,
            "supports_native_message_flow_control_per_channel": False,
            "supports_native_payload_flow_control": False,
            "supports_native_payload_flow_control_per_channel": False,
        }

    # 3. Connection Lifecycle
    @property
    def is_connected(self) -> bool:
        # Return True if the connection to the broker is active
        ...

    async def connect(self) -> None:
        # Establish the connection
        ...

    async def disconnect(self) -> None:
        # Teardown the connection
        ...

    def connection(self) -> AbstractAsyncContextManager[ServerT]:
        # Return an async context manager for the connection
        ...

    # 4. Message Publishing
    async def publish(
        self,
        *,
        channel: str,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        # Send the payload to the broker on the specified channel
        ...

    # 5. Message Consumption
    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        dispatcher: SubscriberDispatcher,
    ) -> SubscriberT:
        # Start consuming from the requested channels and map them to their callbacks
        # Return an object that implements `SubscriberT` (pause, resume, stop, and finish)
        ...
```

## Creating a Subscriber

The `subscribe` method returns an instance compatible with the `SubscriberT` protocol. It represents the active listening loop and must provide the following members:

```
class MyCustomSubscriber:
    @property
    def is_active(self) -> bool:
        # True if actively consuming
        ...

    @property
    def task(self) -> asyncio.Task:
        # The background asyncio Task running the consumer loop
        ...

    async def pause(self) -> None:
        # Pause consumption temporarily
        ...

    async def resume(self) -> None:
        # Resume consumption
        ...

    async def pause_channel(self, channel: str) -> None:
        # Pause consumption on a single channel temporarily
        ...

    async def resume_channel(self, channel: str) -> None:
        # Resume consumption on a single channel
        ...

    async def stop(self) -> None:
        # Stop intake: no new messages are fetched or delivered afterwards.
        # Does not clean up or cancel running tasks. Idempotent.
        ...

    async def finish(self) -> None:
        # Terminate remaining work and release broker resources.
        # Cancels any tasks still running, waits for them to finish,
        # then unsubscribes/releases broker resources.
        # Implies ``stop()``. Idempotent.
        ...
```

To support the `"worker_pause"` backpressure strategy, set `"supports_pause": True` in the server capabilities and implement `pause`/`resume` methods on the subscriber.

To support the `"channel_pause"` backpressure strategy, set `"supports_pause_per_channel": True` in the server capabilities and implement `pause_channel`/`resume_channel` methods on the subscriber.

Pause support means that delivery stops *without terminating the subscription*: in-flight messages may still complete, but the broker stops pushing new ones and a `resume` call re-enables delivery.

If your broker doesn't support worker-wide or per-channel pause, implement the respective methods to raise `NotImplementedError`.

## Applying Intake Limits

Repid passes a `SubscriberDispatcher` into `subscribe()`. It handles admission and oversized payloads.

```
class MyCustomServer:
    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[
            str,
            Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        ],
        dispatcher: SubscriberDispatcher,
    ): ...
```

### Native Intake Limits

If the broker supports propagating prefetch limits to the server, indicate this via the capabilities dict.

```
@property
def capabilities(self) -> CapabilitiesT:
    return {
        ...,
        "supports_native_message_flow_control": True,  # (1)
        "supports_native_message_flow_control_per_channel": True,  # (2)
        "supports_native_payload_flow_control": True,  # (3)
        "supports_native_payload_flow_control_per_channel": True,  # (4)
    }
```

1. Supports limiting the number of messages for the whole subscriber
1. Supports limiting the number of messages per individual channel
1. Supports limiting the total payload size for the whole subscriber
1. Supports limiting the total payload size per individual channel

To implement these capabilities, call the dispatcher's `native_message_limit()` and `native_payload_limit()` methods:

```
# subscriber-wide message/payload limits
subscriber_prefetch = dispatcher.native_message_limit()
subscriber_payload_budget = dispatcher.native_payload_limit()

# per-channel message/payload limits
for channel in channels_to_callbacks:
    channel_prefetch = dispatcher.native_message_limit(channel)
    channel_payload_budget = dispatcher.native_payload_limit(channel)
    ...
```

Based on the advertised capabilities, Repid allows using the `"native"` backpressure strategy.

### Working with Dispatcher

For each delivery, reserve capacity and pass the lease, message, and callback to `run_admitted()`.

A broker may schedule `run_admitted()` in its consume loop or carry the lease through to a separate delivery loop.

This approach is common for pull-based brokers.

```
from contextlib import suppress


class MyCustomSubscriber:
    def __init__(self, *, channels_to_callbacks, dispatcher):
        self._callbacks = channels_to_callbacks
        self._dispatcher = dispatcher
        self._callback_tasks: set[asyncio.Task] = set()
        self._task = asyncio.create_task(self._consume())

    @property
    def task(self) -> asyncio.Task:
        return self._task

    async def _consume(self) -> None:
        while True:
            message = await self._fetch()

            lease = await self._dispatcher.reserve(message)  # (1)
            if lease is None:
                continue

            # Run callbacks outside the intake loop so `stop()` can cancel
            # intake without touching in-flight callbacks.
            task = asyncio.create_task(
                self._dispatcher.run_admitted(
                    lease,
                    message,
                    self._callbacks[message.channel],
                ),
            )
            self._callback_tasks.add(task)
            task.add_done_callback(self._callback_tasks.discard)

    async def stop(self) -> None:
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task

    async def finish(self) -> None:
        # Cancel whatever is still running; `run_admitted` releases its lease
        # even when cancelled.
        for task in tuple(self._callback_tasks):
            task.cancel()
        await asyncio.gather(*self._callback_tasks, return_exceptions=True)
```

1. `reserve()` returns `None` when it has already disposed of the message (e.g. if it had oversized payload): there is no lease and nothing to run.

This is similar to the previous example, except the consume loop fetches a batch of messages at once.

When the broker fetches a whole batch that waits in a local buffer while earlier members are being admitted, `reserve()` only keeps alive the message it is called for. To prevent the broker from rescheduling other messages while they wait, control keep-alive explicitly.

```
class MyCustomSubscriber:
    # ...

    async def _consume(self) -> None:
        while True:
            batch = await self._fetch_batch()
            if not batch:
                continue

            # The batch waits in a local buffer, so keep every member
            # alive while it waits for its turn to be admitted.
            for message, callback in batch:
                self._dispatcher.start_keep_alive(message)

            for message, callback in batch:
                lease = await self._dispatcher.reserve(message)  # (1)
                if lease is None:
                    continue

                task = asyncio.create_task(
                    self._dispatcher.run_admitted(lease, message, callback),
                )
                self._callback_tasks.add(task)
                task.add_done_callback(self._callback_tasks.discard)

    async def _drop_batch(self, batch) -> None:
        # Reject members that will never be admitted
        # (e.g. intake has stopped with the buffer non-empty)
        # after stopping their keep-alive.
        for message, _callback in batch:
            await self._dispatcher.stop_keep_alive(message)
            await self._reject(message)
```

1. `reserve()` returns `None` when it has already disposed of the message (e.g. if it had oversized payload): there is no lease and nothing to run.

This approach is common for push-based brokers.

```
from contextlib import suppress


class MyCustomSubscriber:
    def __init__(self, *, channels_to_callbacks, dispatcher):
        self._callbacks = channels_to_callbacks
        self._dispatcher = dispatcher
        self._callback_tasks: set[asyncio.Task] = set()
        self._delivery_queue: asyncio.Queue[
            tuple[
                ReservationLeaseT,
                ReceivedMessageT,
                Callable[[ReceivedMessageT], Coroutine[Any, Any, None]],
            ],
        ] = asyncio.Queue()
        self._intake_active = True
        # The SDK calls `_handle_push` for every message it pushes;
        # the only loop we own is the delivery loop.
        self._sdk_consumer = MyBrokerSDKConsumer(on_message=self._handle_push)
        self._deliver_task = asyncio.create_task(self._deliver())

    @property
    def task(self) -> asyncio.Task:
        return self._deliver_task

    async def _handle_push(self, message: ReceivedMessageT) -> None:
        if not self._intake_active:
            return  # intake already stopped, drop the push

        lease = await self._dispatcher.reserve(message)  # (1)
        if lease is None:
            return

        await self._delivery_queue.put(
            (lease, message, self._callbacks[message.channel]),
        )

    async def _deliver(self) -> None:
        while True:
            lease, message, callback = await self._delivery_queue.get()
            task = asyncio.create_task(
                self._dispatcher.run_admitted(lease, message, callback),
            )
            self._callback_tasks.add(task)
            task.add_done_callback(self._callback_tasks.discard)

    async def stop(self) -> None:
        self._intake_active = False
        await self._sdk_consumer.close()  # SDK stops pushing new messages
        self._deliver_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._deliver_task

    async def finish(self) -> None:
        # Cancel whatever is still running; `run_admitted` releases its lease
        # even when cancelled.
        for task in tuple(self._callback_tasks):
            task.cancel()
        await asyncio.gather(*self._callback_tasks, return_exceptions=True)
```

1. `reserve()` returns `None` when it has already disposed of the message (e.g. if it had oversized payload): there is no lease and nothing to run.

## Received Messages

When invoking the callbacks provided to `subscribe`, you must provide instances compatible with the `ReceivedMessageT` protocol. These objects wrap the payload, headers, reply metadata (`reply_to`), and methods to act on a message (`ack`, `nack`, `reject`, `reply`).

If your broker does not provide native request/reply semantics, implement `reply(...)` to raise `NotImplementedError`.

By implementing these protocols, your custom broker will integrate natively with the rest of Repid's architecture, including routers, workers, and middlewares.
