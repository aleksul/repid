# Your own brokers

Repid's architecture allows you to easily plug in your own message brokers.
To create a custom broker, you need to implement a class that adheres to the `ServerT` protocol,
defined in `repid.connections.abc`.

## The `ServerT` Protocol

At its core, a server (broker implementation) must handle the connection lifecycle,
message publishing, and message consumption (subscribing).

Here is a simplified overview of what you need to implement:

```python
import asyncio
from contextlib import AbstractAsyncContextManager
from typing import Mapping, Sequence, Any
from repid.connections.abc import (
    ServerT,
    SubscriberT,
    SentMessageT,
    Intake,
    CapabilitiesT,
)


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
    async def subscribe(self, *, intake: Intake) -> SubscriberT:
        # Start consuming from the channels in `intake.channels`, handing each delivery
        # to its channel's intake. Return an object that implements `SubscriberT`
        # (can manage pauses, resumes, closures).
        ...
```

### Intake

`subscribe` receives the worker's `Intake`. It tells your subscriber how much work the worker
can take. `intake.channels` maps each channel to consume to a `ChannelIntake`.

Both the intake and each channel intake have three properties:

- `limits`: the most messages and payload bytes the worker runs at once. Copy it into native
  settings such as link credit or unacknowledged-message limits.
- `demand`: the free capacity right now, as `Demand(messages=..., payload_bytes=...)`. `None`
  means unlimited. Use it to size fetches.
- `ready`: a future that is done while there is room for another message.

A channel's values already include the worker's limit. A broker that fetches for all channels
at once can size that fetch from `intake` itself.

### Submitting messages

Hand each received message to its channel with `channel.submit(message)`. The call is
synchronous, and from then on the worker owns the message: it runs the actor and settles the
message. Submit only while `ready` is done.

A pull-based reader looks like this:

```python
async def read(channel: ChannelIntake) -> None:
    while True:
        ready = channel.ready
        if not ready.done():
            await ready
            continue
        room = channel.demand.messages
        for message in await fetch(max_messages=room or DEFAULT_BATCH):
            channel.submit(message)
```

A push-based broker sets its window from `channel.limits` and submits messages as they arrive.

### Prefetching (optional)

Your subscriber can fetch a few messages beyond `demand` and submit them as soon as `ready`
completes, so freed slots fill without a broker round trip. Make the amount a server constructor
argument such as `prefetch`. Until you submit them, held messages are your subscriber's: renew
their leases, and return them to the broker in `close()`.

### Keep-alive (optional)

If your broker uses leases, set `supports_keep_alive` and implement `keep_alive()`. Repid calls
it every `keep_alive_interval` seconds until it's done with the message. Return `False` once
renewal is pointless (the message is settled or its lease expired), otherwise `None`.

Repid starts renewing when you submit the message. To renew a message your subscriber holds
before that, use `repid.LeaseRenewal` and submit inside `handoff()`, so the worker takes over the
renewal:

```python
renewal = LeaseRenewal(message)
renewal.renew_at(message.keep_alive_interval)
...
with renewal.handoff():
    channel.submit(message)
renewal.stop_if_unclaimed()
```

## Creating a Subscriber

`subscribe` returns an object compatible with the `SubscriberT` protocol. It represents the
active listening loop and must have these members:

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

    async def close(self) -> None:
        # Shut down the subscriber
        ...
```

## Received Messages

The messages you submit must be
instances compatible with `ReceivedMessageT` protocol. These objects wrap the payload, headers,
reply metadata (`reply_to`), and methods to act on a message (`ack`, `nack`, `reject`, `reply`).

If your broker does not provide native request/reply
semantics, implement `reply(...)` to raise `NotImplementedError`.

By implementing these protocols, your custom broker will integrate natively with the rest
of Repid's architecture, including routers, workers, and middlewares.
