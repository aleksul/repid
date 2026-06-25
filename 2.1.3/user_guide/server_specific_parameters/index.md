# Server Specific Parameters

Repid is designed to be highly unopinionated and framework-agnostic. However, different message brokers (like RabbitMQ, Redis, or Google Cloud Pub/Sub) often have unique features that don't map cleanly to a universal abstraction.

For these situations, Repid provides an "escape hatch" known as `server_specific_parameters`.

## Publishing with Parameters

When you use `send_message`, `send_message_json`, or the eagerly-returned `message.reply()` and `message.send_message()`, you can pass a `server_specific_parameters` dictionary.

```
await app.send_message_json(
    channel="tasks",
    payload={"task": "process_video"},
    headers={"topic": "video_worker"},
    server_specific_parameters={
        # Your broker-specific flags go here
    }
)
```

The keys and values inside this dictionary are passed directly to the underlying connection implementation (`ServerT.publish`). If a parameter is not recognized by the active server, it is usually safely ignored, though behavior depends on the specific connection library.

## Supported Parameters by Broker

Here is a list of parameters officially supported by Repid's built-in servers:

### RabbitMQ (AMQP)

When using the `AmqpServer`, you can customize how the internal AMQP 1.0 protocol publishes the message by providing instances of AMQP `AmqpMessageHeader` or `AmqpMessageProperties` objects.

```
from repid import AmqpServer
from repid.connections.amqp import AmqpMessageHeader, AmqpMessageProperties

await app.send_message_json(
    channel="tasks",
    payload={"msg": "hello"},
    server_specific_parameters={
        "header": AmqpMessageHeader(
            durable=True,
            priority=10,
            ttl=5000,  # Time to live in ms
            delivery_count=0
        ),
        "properties": AmqpMessageProperties(
            subject="task_subject",
            reply_to="reply_queue",
            group_id="group_1",
            absolute_expiry_time=1700000000
        ),
        "routing_key": "my.custom.routing.key"
    }
)
```

The supported keys include:

- `header` (`AmqpHeader`): Override the AMQP header. Includes properties like `durable`, `priority`, `ttl` (Time to live in ms), `first_acquirer`, and `delivery_count`.
- `properties` (`AmqpProperties`): Override standard AMQP properties. Includes properties like `message_id`, `user_id`, `to`, `subject`, `reply_to`, `correlation_id`, `content_type`, `content_encoding`, `absolute_expiry_time`, `creation_time`, `group_id`, `group_sequence`, and `reply_to_group_id`.
- `delivery_annotations` (`dict[str, Any]`): Add AMQP delivery annotations.
- `message_annotations` (`dict[str, Any]`): Add AMQP message annotations.
- `footers` (`dict[str, Any]`): Add AMQP footers.
- `to` (`str`): Explicitly override the AMQP `to` address, bypassing Repid's default naming strategy.
- `routing_key` (`str`): Specify a custom routing key to use in combination with Repid's exchange naming strategy.

### Redis Streams

When using the `RedisServer`, the parameters directly translate to `XADD` command options.

- `maxlen` (`int`): Maximum stream length. This forces the stream to drop older entries if it exceeds this length.
- `approximate` (`bool`): Used in conjunction with `maxlen`. If `True` (default), uses `~` for `MAXLEN` which is much more performant.
- `nomkstream` (`bool`): If `True`, the message will not be added and an error will be thrown if the stream doesn't already exist.
- `stream_id` (`str`): Custom stream entry ID. Defaults to `"*"` (auto-generate).

### Google Cloud Pub/Sub

When using the `PubsubServer`, you can customize the gRPC `PublishRequest`.

- `topic` (`str`): Override the target topic name (bypasses default prefixing).
- `project` (`str`): Override the Google Cloud project ID for this specific publish.
- `timeout` (`int | float | timedelta`): Override the timeout for the gRPC publish call.
- `ordering_key` (`str`): A string identifying related messages for which publish order should be respected.
- `attributes` (`dict[str, str]`): Extra metadata attributes to attach directly to the Pub/Sub message envelope.

### In-Memory

When using the `InMemoryServer` for testing or local development:

- `message_id` (`str`): Explicitly set the unique ID of the dummy message in the queue. If omitted, a random UUID is generated.
