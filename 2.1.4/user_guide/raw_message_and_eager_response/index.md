# Raw Message & Eager response

When writing actors, sometimes you need to access the raw message data directly or interact with the message broker prematurely (before the actor finishes executing).

Repid provides the `Message` dependency for exactly this purpose.

## The `Message` object

You can access the current message being processed by injecting the `Message` object into your actor. Under the hood, this uses the `MessageDependency` alias in Repid's dependency injection system.

```
from typing import Annotated
from repid import Router
from repid import Message

router = Router()

@router.actor
async def my_actor(message: Message) -> None:
    print(message.payload)  # Raw bytes payload
    print(message.headers)  # Dictionary of headers
    print(message.reply_to)  # Reply destination (if present)
    print(message.channel)  # The channel this message was received from
    print(message.message_id) # The unique ID of the message (if supported by broker)
```

## Eager responses

By default, Repid automatically acknowledges (`ack`) a message if your actor returns successfully, and negatively acknowledges (`nack`) or rejects it if an exception is raised.

However, sometimes you might want to immediately act on the message. Using the `Message` dependency, you can manually trigger these actions inside your actor.

```
from repid import Router, Message

router = Router()

@router.actor
async def my_actor(user_id: int, message: Message) -> None:
    if user_id < 0:
        # Invalid user_id. Nack the message immediately.
        await message.nack()
        return

    # Acknowledge early - fire-and-forget pattern
    await message.ack()

    # Now we can do some long-running processing...
    await do_heavy_lifting(user_id)
```

### Available Actions

The `Message` object provides the following actions:

- `await message.ack()`: Acknowledge the message (successful processing).
- `await message.nack()`: Negatively acknowledge the message (e.g. temporary failure, usually retry or DLQ depending on the broker).
- `await message.reject()`: Reject the message (wasn't accepted for processing, put back in the original queue).
- `await message.reply(payload=b"...")`: Atomically (if supported by the server) acknowledge the message and send a reply message. You must provide either `channel=...` or have an incoming `reply_to` value available on the message.
- `await message.reply_json(payload={"status": "ok"})`: Atomically acknowledge and reply with JSON data.

If reply is not supported natively by the broker, `message.reply()` / `message.reply_json()` will automatically fall back to sending a new message to the specified channel and acknowledging the current message.

You can also check if a message has already been acted upon using the `.is_acted_on` property.

Note

Message can only be acted on once. Any later actions are discarded.

## Sending New Messages

You can also use the `Message` object to publish entirely new messages while processing the current one. This is very useful for chaining tasks or event-driven architectures.

```
from repid import Router, Message

router = Router()

@router.actor
async def process_order(order_id: int, message: Message) -> None:
    # Process the order here...

    # Send an event to another channel
    await message.send_message_json(
        channel="notifications",
        payload={"event": "order_processed", "order_id": order_id}
    )
```
