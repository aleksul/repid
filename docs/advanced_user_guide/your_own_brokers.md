# Your own brokers

Repid's architecture allows you to easily plug in your own message brokers.
To create a custom broker, you need to implement a class that adheres to the `ServerT` protocol,
defined in `repid.connections.abc`.

!!! warning "Compatibility"
    We try our best to preserve custom broker compatibility, but broker protocols may
    change between minor releases. Check the release notes when upgrading and keep custom broker
    implementations covered by integration tests.

## The `ServerT` Protocol

At its core, a server (broker implementation) must handle the connection lifecycle,
message publishing, and message consumption (subscribing).

Here is a simplified overview of what you need to implement:

```python
import asyncio
from typing import Mapping, Sequence, Callable, Coroutine, Any
from collections.abc import AbstractAsyncContextManager
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

    # (other properties like title, summary, tags, variables, etc. can return
    # None or empty defaults)

    # 2. Capabilities
    @property
    def capabilities(self) -> CapabilitiesT:
        return {
            "supports_native_reply": False,
            "supports_lightweight_pause": False,
            "supports_channel_pause": False,
            "supports_keep_alive": False,
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
        # Return an object that implements `SubscriberT` (can manage pauses, resumes, closures).
        ...
```

## Creating a Subscriber

The `subscribe` method returns an instance compatible with the `SubscriberT` protocol.
It represents the active listening loop. It must have these properties:

```python
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
        # Pause one channel.
        ...

    async def resume_channel(self, channel: str) -> None:
        # Resume one channel.
        ...

    async def close(self) -> None:
        # Shut down the subscriber
        ...
```

## Applying Intake Limits

Repid passes a `SubscriberDispatcher` into `subscribe()`. It handles admission and oversized
payloads. `subscribe()` must return after setup rather than awaiting its delivery loop; Repid
activates dispatch only after validating the subscription's backpressure policy.

```python
class MyCustomServer:
    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        dispatcher: SubscriberDispatcher | None = None,
    ):
        return MyCustomSubscriber(
            channels_to_callbacks=channels_to_callbacks,
            dispatcher=dispatcher,
        )
```

If the broker supports numeric prefetch limits, indicate them upfront. The dispatcher exposes only
built-in numeric limits; custom `LimitPolicyT` instances are never translated to broker prefetch or
native flow control:

```python
# subscriber-wide message/payload limits
subscriber_prefetch = dispatcher.native_message_limit()
subscriber_payload_budget = dispatcher.native_payload_limit()

# per-channel message/payload limits
for channel in channels_to_callbacks:
    channel_prefetch = dispatcher.native_message_limit(channel)
    channel_payload_budget = dispatcher.native_payload_limit(channel)
    ...
```

A subscriber may opt into Repid's native backpressure strategy:

```python
from repid import BackpressureResource


class MyCustomSubscriber:
    def supports_native_flow_control(
        self,
        channel: str,
        resource: BackpressureResource,
    ) -> bool:
        ...
```

Return `True` only when the corresponding numeric dispatcher limit is enforced upstream for that
channel. The broker's credit must remain consumed until `run_admitted()` releases the intake lease,
and reconnects must restore the same window. If the method is absent or returns `False`, Repid tries
the next configured backpressure strategy. Batch-size limits and acknowledgement-based windows do
not qualify when they can release credit before the intake lease.

For each delivery, reserve capacity and pass the lease, message, and callback to
`run_admitted()`. It always releases the lease when the callback settles:

```python
from contextlib import suppress


class MyCustomSubscriber:
    def __init__(self, *, channels_to_callbacks, dispatcher):
        self._callbacks = channels_to_callbacks
        self._dispatcher = dispatcher
        self._task = asyncio.create_task(self._consume())

    @property
    def task(self) -> asyncio.Task:
        return self._task

    async def _consume(self) -> None:
        while True:
            message = await self._fetch()
            lease = await self._dispatcher.reserve(message)
            if lease is not None:
                await self._dispatcher.run_admitted(
                    lease,
                    message,
                    self._callbacks[message.channel],
                )

    async def close(self) -> None:
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
```

A broker may schedule `run_admitted()` in its existing callback task lifecycle or carry the lease
through an internal delivery queue before calling it. `reserve()` returns `None` when it has already
disposed of an oversized payload.

## Received Messages

When invoking the callbacks provided to `subscribe`, you must provide
instances compatible with `ReceivedMessageT` protocol. These objects wrap the payload, headers,
reply metadata (`reply_to`), and methods to act on a message (`ack`, `nack`, `reject`, `reply`).

If your broker does not provide native request/reply
semantics, implement `reply(...)` to raise `NotImplementedError`.

By implementing these protocols, your custom broker will integrate natively with the rest
of Repid's architecture, including routers, workers, and middlewares.
