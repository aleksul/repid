# Your own brokers

Repid's architecture makes it easy to plug in your own message brokers.
To create a custom broker, you need to implement a class that adheres to the `ServerT` protocol,
defined in `repid.connections.abc`.

!!! warning "Compatibility"
    We try our best to preserve custom broker compatibility, but broker protocols may
    change between minor releases. Check the release notes when upgrading and keep custom broker
    implementations covered by integration tests.

## The `ServerT` Protocol

Implement metadata, connection lifecycle, publishing, and subscription as before. Subscription
now receives resolved native settings and message-only submission callbacks:

```python
from collections.abc import Callable, Coroutine
from repid.connections.abc import CapabilitiesT, ReceivedMessageT, SubscriberT, broker_capabilities
from repid.limits import UNLIMITED_NATIVE_FLOW, NativeFlow


class MyCustomServer:
    @property
    def capabilities(self) -> CapabilitiesT:
        return broker_capabilities(keep_alive=True, worker_pause=True)

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
    ) -> SubscriberT: ...
```

`broker_capabilities()` defaults unsupported guarantees to false. Advertise only guarantees your
adapter actually implements:

- `supports_worker_native_messages` / `supports_channel_native_messages`:
  Outstanding delivery count at the stated scope.
- `supports_worker_native_bytes` / `supports_channel_native_bytes`:
  Outstanding serialized payload bytes at the stated scope.
- `supports_worker_pause` / `supports_channel_pause`:
  Soft pause without losing existing settlement resources.
- `supports_worker_oversized_delivery` / `supports_channel_oversized_delivery`:
  A single message exceeding the byte window can reach Repid.
- `supports_worker_oversized_blocking` / `supports_channel_oversized_blocking`:
  Over-window messages are blocked before delivery.

Native byte delivery mode allows a singleton oversized delivery above the numeric window; ordinary
outstanding payloads remain bounded. Blocking mode supplies a hard byte boundary without promising
reject, nack, drop, or dead-letter behavior. Unknown behavior cannot be advertised as blocking.
Fetch batch sizes do not qualify as outstanding-delivery windows. Preserve the existing
`supports_native_reply` and `supports_keep_alive` flags.

## Native settings

```python
worker_count = native_flow.worker.max_messages
worker_bytes = native_flow.worker.max_payload_bytes
for channel, window in native_flow.channels.items():
    channel_count = window.max_messages
    channel_bytes = window.max_payload_bytes
    byte_mode = window.oversized_delivery
```

Absent numeric values request no native window. Every subscribed channel has an entry.
Worker windows bound aggregate outstanding deliveries; independent per-channel windows cannot
substitute for that guarantee. Channel values reflect channel limits and actor/router propagation,
not copied absolute worker numbers. The value includes the resolved byte delivery/blocking mode.

The runner resolves automatic windows, fallback, and explicit-window validation before intake.
Resolved windows retain `messages_automatic` so adapters can distinguish automatic count
requests from explicit requirements when discovering existing transport state. Expose the actual
settings through `subscriber.native_flow`; use an absent window on automatic fallback so the runner
retains local pause thresholds. Explicit count requests must match exactly.
Adapters must not silently ignore a supplied native requirement. Use
`validate_native_flow(native_flow, self.capabilities)` to validate the contract.
Native windows end on settlement, not submission callback return; early acknowledgment may free a
native slot before actor processing finishes.

## Submission and ownership

`await callback(message)` returns once the runner admits or disposes of the delivery. It does not
wait for actor completion and does not imply acknowledgment. Controlled delivery loops await
submissions; they do not create an unrestricted callback task per message.

The adapter owns fetched batches and queued deliveries until callback entry. Bound those buffers,
renew every waiting delivery, and settle deliveries that will not be submitted. At callback entry
the runner takes renewal and settlement ownership, including while admission waits. If submission
fails or is cancelled, the adapter must not settle the handed-off message again.

For a pull adapter, the ownership boundary is straightforward:

```python
async def consume(self) -> None:
    while not self.stopped:
        message = await self.fetch_one()
        # Do not catch submission errors and perform another settlement.
        await self.callbacks[message.channel](message)
```

A batched adapter must renew the entire fetched batch before awaiting its first submission.
Transfer each message's renewal ownership immediately before callback entry, and reject remaining
adapter-owned batch members during stop. Push adapters need a bounded, renewed queue and controlled
submission loops; messages pushed after stop still need an explicit disposal path.

## Subscriber lifecycle

```python
import asyncio


class MyCustomSubscriber:
    @property
    def is_active(self) -> bool: ...

    @property
    def task(self) -> asyncio.Task: ...

    async def pause(self, channel: str | None = None) -> None: ...

    async def resume(self, channel: str | None = None) -> None: ...

    async def stop(self) -> None: ...

    async def finish(self) -> None: ...
```

`channel=None` means worker scope. Unsupported scope operations raise; advertised operations must
work. Concurrent worker and channel pauses must remain independent so resuming one cannot clear
another. Pauses can have transport overshoot, but must reduce further intake without disconnecting
settlement resources.

`stop()` is idempotent and ends intake, cancels active submissions, and disposes of adapter-owned
buffers. Preserve connections, receiver links, offset tracking, and other resources required by
runner-owned admitted processing. Do not cancel actor tasks.

`finish()` implies stop, is idempotent, and releases subscriber resources after admitted processing
has drained or been cancelled. Cleanup failures must propagate. The runner stops intake first,
rejects work that has not started processing, gives started processing its grace period, and then
finishes the subscriber.

Opt-in runner resubscription stops the old intake and disposes of unadmitted deliveries, retains
its settlement resources while admitted work drains, and replaces it only after drain and pressure
recovery. Drain has no timeout; explicit shutdown interrupts it. Transport reconnects must likewise
avoid cancelling runner-owned tasks or reservations.

## Migration note

!!! warning "Interface migration"
    Custom brokers must replace the old `concurrency_limit` subscription argument with
    `native_flow`, migrate capability flags, add scoped pause/resume, expose the actual resolved
    `subscriber.native_flow`, and replace `close()` with `stop()` and `finish()`. There is no
    dispatcher, close-only lifecycle, or old-policy lease compatibility shim.
    Policies return fresh async context managers and use `on_capacity_exhausted`; see
    [Custom limit policies](../user_guide/workers/concurrency.md).

The NATS adapter retains push subscriptions and the durable queue group `<channel>_group`.
New consumers use the resolved channel count window as `MaxAckPending` (1000 when no window
is requested). This server-side budget is shared by every subscription bound to that consumer;
it is not a per-worker budget. Existing consumer configuration is preserved. A stricter existing
window is accepted for automatic requests; an existing window larger than an automatic request
falls back to local control with a resolution diagnostic. A conflicting explicit native window fails
before delivery subscriptions open. Configure shared consumer settings centrally before starting
workers when an exact group-wide budget is needed.

NATS worker aggregate counts, payload bytes, and processing reservations remain local. Early
acknowledgment releases server credit but retains local capacity through processing and middleware
cleanup. Adapter buffers are bounded and renew waiting messages. Pause and stop drain and detach
push delivery subscriptions while retaining the connection used for settlement and renewal;
`finish()` releases the remaining subscriber state. Existing push durables need no recreation.

## Received Messages

When invoking the callbacks provided to `subscribe`, you must provide
instances compatible with the `ReceivedMessageT` protocol. These objects wrap the payload, headers,
reply metadata (`reply_to`), and methods to act on a message (`ack`, `nack`, `reject`, `reply`).

If your broker does not provide native request/reply
semantics, implement `reply(...)` to raise `NotImplementedError`.

By implementing these protocols, your custom broker will integrate natively with the rest
of Repid's architecture, including routers, workers, and middlewares.
