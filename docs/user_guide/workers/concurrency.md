# Concurrency Limits

Concurrency limits keep a worker from taking more work than it can handle.
For example, you can use them to protect memory, database pools, external services, etc.

Configure message count and payload bytes with `Concurrency`:

```python
from repid import Concurrency

await app.run_worker(
    concurrency=Concurrency(
        messages=200,
        payload_bytes=64 * 1024**2,
    ),
)
```

## Default worker limit

A worker admits up to 1,000 messages by default:

```python
await app.run_worker()  # same as Concurrency(messages=1000)
```

To remove this limit, pass an empty `Concurrency` object:

```python
await app.run_worker(concurrency=Concurrency())
```

There is no default payload byte cap.

Each field accepts a positive integer or `None` for unlimited.

Routers and actors have no default limits.

??? Note "Why is the default 1,000 messages?"
    In tests, the processing overhead of receiving and scheduling about 1,000 to 2,000 no-op
    messages can saturate one CPU core. This number is based on Repid's
    message-consumption overhead, does not include any work done by your actors.

    When a worker takes more messages than its CPU can process, it can also take work away from
    less busy workers, thus the whole system becomes less efficient.
    The best value depends on your CPU, broker, and actors.
    Measure your workload and adjust the limit.

## Channel limits

Declare an additional budget on a channel:

```python
from repid import Channel, Concurrency, Router

images = Channel(
    address="images",
    concurrency=Concurrency(
        messages=8,
        payload_bytes=32 * 1024**2,
    ),
)
router = Router(channel=images)

await app.run_worker(
    concurrency=Concurrency(
        messages=100,
        payload_bytes=128 * 1024**2,
    ),
)
```

The worker limit applies to all channels combined. A message counts toward the worker limit and
any limit set for its channel. All channels draw from the same pool of worker capacity.

=== "With a channel limit"

    The worker waits until both budgets can admit the message.

    ```mermaid
    flowchart TD
        routed["Message routed to a channel"] --> worker{"Room in the shared worker budget?"}
        worker -->|Yes| channel{"Room in the images channel budget?"}
        worker -->|No| wait["Wait for capacity"]
        channel -->|No| wait
        wait -->|Retry| worker
        channel -->|Yes| reserve["Reserve worker and channel capacity together"]
        reserve --> process["Process the message"]
    ```

=== "Without a channel limit"

    The worker checks only its own budget.

    ```mermaid
    flowchart TD
        routed["Message routed to a channel"] --> capacity{"Room in the worker budget?"}
        capacity -->|Yes| process["Reserve worker capacity and process the message"]
        capacity -->|No| wait["Wait for capacity"]
        wait --> capacity
    ```

### Sharing a channel limit across actors

Both actors below use the same `images` address, so they share one 8-message limit.

```python
from repid import Channel, Concurrency, Router

router = Router()


@router.actor(
    channel=Channel(
        address="images",
        concurrency=Concurrency(messages=8),
    ),
)
async def resize_image() -> None: ...


@router.actor(
    channel=Channel(
        address="images",
        concurrency=Concurrency(messages=8),
    ),
)
async def make_thumbnail() -> None: ...
```

If 6 messages are running in `resize_image` and 2 in `make_thumbnail`, the next `images`
message waits for a slot.

If you explicitly set concurrency limits for the same channel address more than once,
the settings must match exactly. Repid will raise an error when it materializes the router
if the settings differ.

You can also declare the limit once and reference the channel by its address:

```python
from repid import Channel, Concurrency, Router

router = Router()


@router.actor(
    channel=Channel(
        address="images",
        concurrency=Concurrency(messages=8),
    ),
)
async def resize_image() -> None: ...


@router.actor(channel="images")  # (1)
async def make_thumbnail() -> None: ...
```

1. Implicitly references earlier declaration of the channel with Concurrency of 8 messages.

## What the budgets measure

Each call to `app.run_worker()` has its own budgets for the duration of the run.

- `messages` counts messages the worker has admitted for processing.
- `payload_bytes` adds up `len(message.payload)` for those messages. It measures serialized
  payloads only. Headers, decoded objects, and broker buffers are outside this budget.

A message starts counting after routing, just before processing. It holds that capacity until
processing finishes, including input conversion, actor and middlewares execution, and
any acknowledgment or rejection the worker awaits. The same timing applies even if
the actor acknowledges early or leaves the message unacknowledged.

A message larger than a byte budget waits for that budget to empty, then runs alone within it.
Deliveries waiting on the same channel enter in arrival order. A channel waiting on its own
budget leaves shared capacity available to other channels. When the next eligible channel
needs more shared capacity, the worker lets that budget drain enough for its delivery.

## Broker flow control

Repid also supplies intake hints where the adapter can use them. For each field, it uses the
tighter finite worker or channel limit. For example, a worker with 1,000 message slots and
`Channel(address="images", concurrency=Concurrency(messages=3))` gives the AMQP
receiver link for `images` credit for three outstanding deliveries.

| Adapter | Intake setting |
| --- | --- |
| AMQP 1.0 | Message credit per receiver link |
| Google Pub/Sub | Outstanding messages and bytes per stream |
| Kafka | Shared poll and fetch settings using the smallest subscribed channel hints |
| Redis Streams | Message count per stream read or reclaim |
| SQS | Receive batch and pending callback counts per queue |
| NATS JetStream | Acknowledgment window configured on the shared durable consumer |
| In-memory | Pending callback count per channel |

Broker's native flow control and Repid's concurrency settings can have slightly different lifetimes.
For example, batch sizes and fetch settings can leave additional data in client or protocol buffers.
