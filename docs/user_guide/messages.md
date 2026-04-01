# Messages

Sending messages to a queue is the entry point for triggering background tasks in Repid.

## Sending raw data

To send a message, you call `send_message` on your initialized `Repid` application. The payload for
`send_message` must be raw `bytes`.

```python
import asyncio
from repid import Repid, InMemoryServer

app = Repid()
app.servers.register_server("default", InMemoryServer(), is_default=True)

async def main():
    # A connection must be active to publish!
    async with app.servers.default.connection():

        await app.send_message(
            channel="my_channel",
            payload=b"Hello, world!",
            content_type="text/plain",
            headers={"topic": "my_actor"}  # Routing topic
        )

if __name__ == "__main__":
    asyncio.run(main())
```

## Sending JSON

Since JSON is the standard for most applications, Repid provides a helper method
`send_message_json`. It takes any Python dictionary, list, or Pydantic model, automatically
serializes it to bytes, and sets the `content-type` to `application/json`.

```python
await app.send_message_json(
    channel="email_queue",
    payload={
        "to": "user@example.com",
        "subject": "Welcome!",
    },
    headers={"topic": "send_welcome_email"}
)
```

## Routing: Channels vs Operation IDs

When you send a message, you must specify **where** it goes so the correct worker can pick it up.
Repid supports two ways to route messages, directly mirroring the AsyncAPI specification:

### 1. Channels (Basic Routing)

The simplest way to route a message is by specifying the raw queue or topic name as the `channel`.

By default, Repid uses a topic-based routing strategy on the worker side. This means that even if a
worker is listening to `email_queue`, you usually need to supply a `"topic"` header matching the
name of the actor function you want to execute
(e.g. `headers={"topic": "send_welcome_email"}`).

```python
await app.send_message_json(
    channel="email_queue",
    payload={"msg": "hi"},
    headers={"topic": "send_welcome_email"}
)
```

### 2. Operation IDs (Advanced Routing)

In a complex system, you might have multiple distinct actions going over the same channel, or you
might want to decouple the logical "action" from the physical queue infrastructure.

You can register an `operation_id` to a specific channel in Repid's message registry.

```python
# During setup, tell Repid that the "send_welcome_email" operation
# happens on "email_queue"
app.messages.register_operation(
    operation_id="send_welcome_email",
    channel="email_queue"
)

# Later, send by operation_id instead of channel!
await app.send_message_json(
    operation_id="send_welcome_email",
    payload={"user_id": 123},
    headers={"topic": "send_welcome_email"}
)
```

If you specify `operation_id`, Repid looks it up in the registry to determine the underlying
channel. As an added benefit, registering operations allows Repid to automatically include them in
the AsyncAPI schema.

!!! note
    You must provide either a `channel` or an `operation_id`, but never both.
