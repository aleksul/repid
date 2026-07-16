# Your own brokers

Repid's architecture allows you to easily plug in your own message brokers. To create a custom broker, you need to implement a class that adheres to the `ServerT` protocol, defined in `repid.connections.abc`.

## The `ServerT` Protocol

At its core, a server (broker implementation) must handle the connection lifecycle, message publishing, and message consumption (subscribing).

Here is a simplified overview of what you need to implement:

```
import asyncio
from typing import Mapping, Sequence, Callable, Coroutine, Any
from collections.abc import AbstractAsyncContextManager
from repid.connections.abc import ServerT, SubscriberT, SentMessageT, ReceivedMessageT, CapabilitiesT

class MyCustomServer:
    # 1. Server Metadata Properties for AsyncAPI
    @property
    def host(self) -> str:
        return "my-broker-host"

    @property
    def protocol(self) -> str:
        return "my-custom-protocol"

    # (other properties like title, summary, tags, variables, etc. can return None or empty defaults)

    # 2. Capabilities
    @property
    def capabilities(self) -> CapabilitiesT:
        return {
            "supports_native_reply": False,
            "supports_lightweight_pause": False,
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
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        # Start consuming from the requested channels and map them to their callbacks
        # Return an object that implements `SubscriberT` (can manage pauses, resumes, closures)
        ...
```

## Creating a Subscriber

The `subscribe` method returns a instance, compatible with `SubscriberT` protocol. It represents the active listening loop. It must have these properties:

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

    async def close(self) -> None:
        # Shut down the subscriber
        ...
```

## Received Messages

When invoking the callbacks provided to `subscribe`, you must provide instances compatible with `ReceivedMessageT` protocol. These objects wrap the payload, headers, reply metadata (`reply_to`), and methods to act on a message (`ack`, `nack`, `reject`, `reply`).

If your broker does not provide native request/reply semantics, implement `reply(...)` to raise `NotImplementedError`.

By implementing these protocols, your custom broker will integrate natively with the rest of Repid's architecture, including routers, workers, and middlewares.
