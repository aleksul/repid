# Concurrency Limits

Concurrency limits keep a worker from taking more work than it can handle. For example, you can use them to protect memory, database pools, external services, etc.

Configure message count and payload bytes with `Concurrency`:

```
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

```
await app.run_worker()  # same as Concurrency(messages=1000)
```

To remove this limit, pass an empty `Concurrency` object:

```
await app.run_worker(concurrency=Concurrency())
```

There is no default payload byte cap.

Any `Concurrency` you pass replaces the default as a whole, and a field you leave out is unlimited. A worker configured with only `payload_bytes` therefore has no message limit, and small or empty payloads can still start an unbounded number of actors. Set both fields to bound both:

```
await app.run_worker(
    concurrency=Concurrency(messages=1000, payload_bytes=64 * 1024**2),
)
```

The `tasks_limit` worker argument is deprecated. Use `concurrency=Concurrency(messages=...)` to set the same limit.

Each field accepts a positive integer or `None` for unlimited.

Routers and actors have no default limits.

> [!NOTE]
>
> **Why is the default 1,000 messages?**
>
> In tests, the processing overhead of receiving and scheduling about 1,000 to 2,000 no-op messages can saturate one CPU core. This number is based on Repid's message-consumption overhead, does not include any work done by your actors.
>
> When a worker takes more messages than its CPU can process, it can also take work away from less busy workers, thus the whole system becomes less efficient. The best value depends on your CPU, broker, and actors. Measure your workload and adjust the limit.

## Channel limits

Declare an additional budget on a channel:

```
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

The worker limit applies to all channels combined. A message counts toward the worker limit and any limit set for its channel. All channels draw from the same pool of worker capacity.

The worker waits until both budgets can admit the message.

```
flowchart TD
    routed["Message routed to a channel"] --> worker{"Room in the shared worker budget?"}
    worker -->|Yes| channel{"Room in the images channel budget?"}
    worker -->|No| wait["Wait for capacity"]
    channel -->|No| wait
    wait -->|Retry| worker
    channel -->|Yes| reserve["Reserve worker and channel capacity together"]
    reserve --> process["Process the message"]
```

The worker checks only its own budget.

```
flowchart TD
    routed["Message routed to a channel"] --> capacity{"Room in the worker budget?"}
    capacity -->|Yes| process["Reserve worker capacity and process the message"]
    capacity -->|No| wait["Wait for capacity"]
    wait --> capacity
```

### Sharing a channel limit across actors

Both actors below use the same `images` address, so they share one 8-message limit.

```
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

If 6 messages are running in `resize_image` and 2 in `make_thumbnail`, the next `images` message waits for a slot.

If you explicitly set concurrency limits for the same channel address more than once, the settings must match exactly. Repid will raise an error when it materializes the router if the settings differ. A limit declared on a router's default channel also applies when actors in other included routers use the same address.

You can also declare the limit once and reference the channel by its address:

```
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

## What counts toward a limit

- `messages` counts the messages the worker is processing.
- `payload_bytes` adds up the size of their serialized payloads.

A message counts from the moment it starts until the worker has finished with it, including the final acknowledgment. A message larger than a byte limit runs alone once that limit is free.

## Prefetch

Subscribers fetch only as many messages as the worker has room for. With `prefetch=N`, they fetch up to N extra messages per channel and keep them ready, so a slot freed by a finished actor fills without waiting for the broker.

```
from repid.connections import RedisServer

server = RedisServer("redis://localhost:6379/0", prefetch=10)
```

| Broker | `prefetch` default |
| --- | --- |
| Redis Streams | `batch_size`, up to the channel's message limit |
| SQS | `batch_size`, up to the channel's message limit |
| AMQP 1.0, Google Pub/Sub, Kafka, NATS JetStream | `0` |
| In-memory | not available |

Prefetched messages are reserved for this worker and don't count toward `payload_bytes`. Keep `prefetch` small when actors are slow or many workers share a queue, and raise it when actors are fast and broker round trips are the bottleneck.

### NATS JetStream

Repid sets the consumer's `max_ack_pending` to the channel's message limit plus `prefetch`, but only when it creates the consumer. All workers share that window, and Repid never changes an existing consumer. To change it later, update the consumer on the NATS server.
