# Error Handling & Retries

When processing background tasks, exceptions and failures are inevitable. Repid gives you granular control over how errors are handled and how messages are acknowledged back to the message broker.

## Confirmation Mode and Errors

Repid allows you to control exactly when a message is acknowledged (`ack`) back to the broker using the `confirmation_mode` setting on the decorator. The default mode is `"auto"`.

- `"auto"`: Acknowledges the message if the actor completes successfully. If an exception is raised, it handles it based on the `on_error` policy.
- `"always_ack"`: Always acknowledges the message, even if the actor raises an exception.
- `"ack_first"`: Acknowledges the message *before* the actor even runs (Fire-and-forget).
- `"manual"`: Never automatically acknowledges. You must inject the `Message` dependency and manually `await message.ack()`.

When using `"auto"`, the `on_error` policy defaults to `"nack"`. You can change this to `"reject"` to simply return the message to the queue:

```
@router.actor(
    channel="tasks",
    confirmation_mode="auto",
    on_error="reject"  # Can be "nack" or "reject"
)
async def my_actor():
    pass
```

Alternatively, you can provide a callable that inspects the exception dynamically to decide the outcome:

```
def handle_error(exc: Exception) -> Literal["reject", "nack"]:
    # Reject ValueError so they are re-queued, Nack everything else
    if isinstance(exc, ValueError):
        return "reject"
    return "nack"

@router.actor(channel="tasks", on_error=handle_error)
async def my_smart_actor():
    pass
```

## Retries

Repid **does not provide built-in retry policies or delay mechanics out of the box**. Because retry semantics (e.g., dead-lettering, exponential backoff, delaying messages) are highly dependent on the underlying message broker and the specific needs of the user, Repid delegates this responsibility.

If an actor raises an exception, the worker catches it and instructs the broker based on the actor's `on_error` configuration.

If you need to implement retries, you have two main options:

1. **Broker-Level Retries:** Configure your broker (like RabbitMQ) with a Dead Letter Exchange (DLX) and message TTLs to automatically re-route and delay failed messages.
1. **Application-Level Retries:** Wrap your actor function with error handling logic (e.g., using a library like `tenacity`, or writing a custom decorator) that catches exceptions, sleeps, and re- attempts the logic internally before ever yielding an error back to Repid.
