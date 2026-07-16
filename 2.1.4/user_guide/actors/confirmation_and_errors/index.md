# Confirmation Modes & Error Handling

When processing background tasks, exceptions and failures are inevitable. Repid gives you granular control over how errors are handled and how messages are acknowledged back to the message broker.

## Confirmation Mode and Errors

Repid allows you to control exactly when a message is acknowledged (`ack`) back to the broker using the `confirmation_mode` setting on the decorator. The default mode is `"auto"`.

- `"auto"`: Acknowledges the message if the actor completes successfully. If an exception is raised, it handles it based on the `on_error` policy.
- `"always_ack"`: Always acknowledges the message, even if the actor raises an exception.
- `"ack_first"`: Acknowledges the message *before* the actor even runs (Fire-and-forget).
- `"manual"`: Never automatically acknowledges. You must inject the `Message` dependency and manually `await message.ack()`.
- `"manual_explicit"`: Same as `"manual"`, but enforces explicitly returning an acknowledgment status.

When using `"auto"`, the `on_error` policy defaults to `"nack"`. You can change this to `"reject"` to simply return the message to the queue, or `"ack"` to acknowledge it even on failure:

```
@router.actor(
    channel="tasks",
    confirmation_mode="auto",
    on_error="reject"  # Can be "nack", "reject", or "ack"
)
async def my_actor():
    pass
```

Alternatively, you can provide a callable that inspects the exception dynamically to decide the outcome:

```
def handle_error(exc: Exception) -> Literal["reject", "nack", "ack"]:
    # Reject ValueError so they are re-queued, Nack everything else
    if isinstance(exc, ValueError):
        return "reject"
    return "nack"

@router.actor(channel="tasks", on_error=handle_error)
async def my_smart_actor():
    pass
```

Infinite retries

Ensure your message broker is configured with delivery limits (max retries) to prevent infinite reprocessing of failed messages.

### `manual_explicit` confirmation mode

The `"manual_explicit"` mode works exactly like `"manual"`, but ensures that you explicitly return one of the literals `"ack"`, `"nack"`, `"reject"`, or `"no_action"`. The runner will act on the returned value immediately after execution. This helps to prevent accidentally leaving a message without a confirmation.

```
from typing import Literal

@router.actor(channel="tasks", confirmation_mode="manual_explicit")
async def my_explicit_actor() -> Literal["ack", "nack", "reject", "no_action"]:
    try:
        # Do some work
        return "ack"
    except Exception:
        # You must explicitly return a confirmation action
        return "nack"
```

### `always_ack` confirmation mode

The `"always_ack"` mode is essentially the same as setting `confirmation_mode="auto"` combined with `on_error="ack"`.

Note that when using `"always_ack"` or `"ack_first"` modes, you cannot provide an `on_error` parameter to the decorator, as the behavior is already strictly defined by the mode itself.

### Manual modes and validation errors

If you are using `confirmation_mode="manual"` or `confirmation_mode="manual_explicit"`, you must handle your message's acknowledgment. However, if a message fails to parse (e.g. `pydantic.ValidationError` when reading arguments), your actor code will never be executed. In `manual` and `manual_explicit` modes, the easiest way to avoid leaving the message unacknowledged is to use `on_error` (alternatively, you can catch the exception and act on the message in a middleware).

By default, in `manual` and `manual_explicit` modes `on_error` is set to `"no_action"`. You can override this to acknowledge or reject the message directly:

```
@router.actor(
    confirmation_mode="manual_explicit",
    on_error="nack"
)
async def my_actor(payload: MyPydanticModel) -> Literal["ack", "nack", "reject", "no_action"]:
    # the actor body won't run if MyPydanticModel fails validation
    return "ack"
```

## Retries

Repid **does not provide built-in retry policies or delay mechanics out of the box**. Because retry semantics (e.g., dead-lettering, exponential backoff, delaying messages) are highly dependent on the underlying message broker and the specific needs of the user, Repid delegates this responsibility.

If an actor raises an exception, the worker catches it and instructs the broker based on the actor's `on_error` configuration.

If you need to implement retries, you have two main options:

1. **Broker-Level Retries:** Configure your message broker with a Dead Letter Exchange (DLX) to automatically re-route failed messages. You can either leave messages in the DLX for manual inspection and processing, or configure message TTLs to automatically re-queue them after a delay for retry attempts.
1. **Application-Level Retries:** Wrap your actor function with error handling logic (e.g., using a library like `tenacity`, or writing a custom decorator) that catches exceptions, sleeps, and re-attempts the logic internally before ever yielding an error back to Repid. Note that from the broker's perspective, processing of a single message takes much longer, which can lead to worse concurrency.

## Poison Message Handling

A "poison message" is a message that consistently fails to process, potentially causing an infinite loop of errors and retries that clogs your queue.

How Repid handles poison messages depends on *where* the failure occurs: during routing, or during execution.

### Unrouted Messages

If a message is pulled from a channel but Repid cannot find a matching actor to route it to, Repid will initially `reject` (requeue) the message. However, to prevent infinite loops, Repid maintains an internal counter of how many times it has seen the same unrouted message.

If an unrouted message is encountered **10 times** (by default), Repid classifies it as a poison message. It logs an `actor.route.poison_message` error and will `nack` the message instead of requeuing it, allowing the broker to drop it or move it to a Dead Letter Queue (DLQ).

### Actor Execution Failures

Because Repid delegates execution retry logic to the underlying message broker, handling poison messages that crash *inside* your actor code largely depends on your infrastructure:

1. **Broker-Level Delivery Limits (Recommended):** Most mature brokers support configuring maximum delivery attempts. Once a message exceeds this limit, the broker automatically routes it to a Dead Letter Queue (DLQ) or drops it. This prevents the message from being infinitely re-queued and keeps your application logic clean.
1. **Application-Level Handling:** You can also handle poison messages explicitly within your actor. By catching exceptions and explicitly acknowledging (`ack`) the failing message—perhaps after logging it or saving the payload to a separate database table—you remove it from the queue and stop the retry loop.
