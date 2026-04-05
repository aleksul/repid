# RabbitMQ Retries with Exponential Backoff

When a task fails, you often want to retry it automatically, but with increasing delays
between attempts (exponential backoff). This prevents your system from being overwhelmed
by a failing dependency and avoids wasting resources on immediate, likely-to-fail retries.

This cookbook explains how to implement exponential backoff using **RabbitMQ 4.x** (or
3.10+), utilizing **Quorum Queues** to prevent poison messages and a smart routing
topology that allows **one set of backoff queues** for all your work queues.

This guide draws inspiration from
[Brian Storti's excellent article on the topic](https://www.brianstorti.com/rabbitmq-exponential-backoff/).

## The Architecture

RabbitMQ does not support natively delaying messages per-message without custom plugins.
Instead, we simulate delays by placing a message in a queue with no consumers and a
Time-To-Live (`x-message-ttl`). When the TTL expires, the message is dead-lettered to an
exchange that routes it back to its original work queue.

To support multiple backoff delays (e.g., 10s, 60s, 300s) for multiple different work
queues (e.g., `emails`, `reports`) **without** creating a separate delay queue for every
work queue, we use **Topic Exchanges**.

1. **`retry_exchange` (Topic)**: Fails are published here. Routing key format:
   `delay.<delay_ms>.<original_queue_name>`.
2. **Delay Queues**: E.g., `delay_10s`. Bound to `retry_exchange` with routing key
   `delay.10000.#`. Has `x-message-ttl=10000` and its `x-dead-letter-exchange` is set to
   `requeue_exchange`.
3. **`requeue_exchange` (Topic)**: Receives expired messages from delay queues.
4. **Work Queues**: E.g., `emails`. Bound to `requeue_exchange` with routing key
   `*.*.emails`. Configured as a **Quorum Queue** with `x-delivery-limit` to prevent
   immediate poison message loops (e.g. if the worker hard crashes).

## Setting Up the Topology

Because Repid's AMQP server is designed for message processing rather than broker
administration, you should set up your RabbitMQ topology using your preferred tool. For
example, you can use RabbitMQ's native Definitions JSON format, which can be applied via
the HTTP API.

First, create a `definitions.json` file. This declarative format defines the exchanges,
queues, and bindings exactly as described above:

```json
{
  "rabbit_version": "4.0.0",
  "vhosts": [{ "name": "/" }],
  "exchanges": [
    {
      "name": "retry_exchange",
      "vhost": "/",
      "type": "topic",
      "durable": true,
      "auto_delete": false
    },
    {
      "name": "requeue_exchange",
      "vhost": "/",
      "type": "topic",
      "durable": true,
      "auto_delete": false
    }
  ],
  "queues": [
    {
      "name": "delay_10s",
      "vhost": "/",
      "durable": true,
      "auto_delete": false,
      "arguments": {
        "x-queue-type": "quorum",
        "x-message-ttl": 10000,
        "x-dead-letter-exchange": "requeue_exchange"
      }
    },
    {
      "name": "delay_60s",
      "vhost": "/",
      "durable": true,
      "auto_delete": false,
      "arguments": {
        "x-queue-type": "quorum",
        "x-message-ttl": 60000,
        "x-dead-letter-exchange": "requeue_exchange"
      }
    },
    {
      "name": "my_work_queue",
      "vhost": "/",
      "durable": true,
      "auto_delete": false,
      "arguments": {
        "x-queue-type": "quorum",
        "x-delivery-limit": 5
      }
    }
  ],
  "bindings": [
    {
      "source": "retry_exchange",
      "vhost": "/",
      "destination": "delay_10s",
      "destination_type": "queue",
      "routing_key": "delay.10000.#"
    },
    {
      "source": "retry_exchange",
      "vhost": "/",
      "destination": "delay_60s",
      "destination_type": "queue",
      "routing_key": "delay.60000.#"
    },
    {
      "source": "requeue_exchange",
      "vhost": "/",
      "destination": "my_work_queue",
      "destination_type": "queue",
      "routing_key": "*.*.my_work_queue"
    }
  ]
}
```

You can upload this configuration using the RabbitMQ Management HTTP API via `curl`
(assuming the management plugin `rabbitmq_management` is enabled):

```bash
curl -i -u guest:guest -H "content-type:application/json" -X POST \  # (1)
  -d @definitions.json http://localhost:15672/api/definitions
```

1. Or specify your proper credentials instead of `guest:guest`

_(Alternatively, you can load these on server startup by pointing the `load_definitions`
configuration file in `rabbitmq.conf` to this JSON file)._

## The Decorator Implementation

To keep our actors clean and reusable, we can encapsulate the entire retry, delay
calculation, and republishing logic into a Python decorator.

Because Repid relies on `inspect.signature()` to resolve Dependency Injection arguments
(like parsing your JSON payload into Pydantic models or injecting `Message`), we **must**
explicitly manipulate the `__signature__` attribute of our wrapper. If we don't, Repid
either won't see your original arguments (breaking payload parsing) or won't see the
injected `Message` parameter.

By using `typing.Concatenate` and `ParamSpec`, we ensure the wrapper is perfectly
type-safe for your IDE, while dynamically injecting the `message` parameter so Repid knows
to provide it at runtime.

By using Repid's default `confirmation_mode="auto"`, this decorator works elegantly: if it
catches an exception, publishes a retry message, and suppresses the error by returning
normally, Repid will automatically `ack` the original message. If retries are exhausted,
it simply re-raises the exception so Repid can handle it according to the `on_error`
policy (e.g., rejecting or dead-lettering it).

```python
import inspect
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, Concatenate, ParamSpec, TypeVar, cast

from repid import Message, Router

P = ParamSpec("P")
R = TypeVar("R")

def with_rabbitmq_retries(
    max_retries: int = 5,
    backoff_delays: list[int] | None = None,
    retry_exchange: str = "retry_exchange",
    retry_exceptions: type[Exception] | tuple[type[Exception], ...] = Exception,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[Concatenate[Message, P], Awaitable[R]]]:
    """
    Wraps a Repid actor with RabbitMQ exponential backoff logic.
    Assumes the actor uses the default `confirmation_mode="auto"`.
    """
    if backoff_delays is None:
        # Default delays: 10s, 1m, 5m, 10m, 30m
        backoff_delays = [10_000, 60_000, 300_000, 600_000, 1_800_000]

    def decorator(
        func: Callable[P, Awaitable[R]]
    ) -> Callable[Concatenate[Message, P], Awaitable[R]]:
        # 1. We must dynamically merge the signature so Repid injects BOTH
        #    your custom payload arguments AND the `Message` dependency.
        #    Without this, @wraps hides `message`, or removing @wraps hides your payload args!
        sig = inspect.signature(func)

        # Add `message: Message` as a required parameter if it isn't already there
        if "message" not in sig.parameters:
            new_params = [
                inspect.Parameter("message", inspect.Parameter.KEYWORD_ONLY, annotation=Message),
                *list(sig.parameters.values())
            ]
            new_sig = sig.replace(parameters=new_params)
        else:
            new_sig = sig

        @wraps(func)
        async def wrapper(message: Message, *args: P.args, **kwargs: P.kwargs) -> R:
            try:
                # 2. Execute the original actor code
                # If the original func didn't explicitly request 'message', we don't pass it down.
                if "message" in sig.parameters:
                    kw = cast(dict[str, Any], kwargs)
                    kw["message"] = message
                    return await func(*args, **kw)  # type: ignore[arg-type]
                else:
                    return await func(*args, **kwargs)

            except retry_exceptions as e:
                # 3. Handle the failure and calculate backoff
                # Any exception NOT in retry_exceptions will bypass this block and be
                # immediately handled by Repid's on_error policy (no retries).
                headers = message.headers or {}
                retry_count = int(headers.get("x-retry-count", 0))

                if retry_count >= max_retries:
                    print(f"Max retries ({max_retries}) reached for message {message.message_id}")
                    # Re-raise the exception so Repid's auto mode acts on it
                    raise

                delay_index = min(retry_count, len(backoff_delays) - 1)
                delay_ms = backoff_delays[delay_index]

                print(f"Task failed: {e}. Retrying in {delay_ms}ms (Attempt {retry_count + 1})")

                # 4. Republish to the delay exchange
                new_headers = headers.copy()
                new_headers["x-retry-count"] = str(retry_count + 1)

                # We explicitly construct the AMQP 1.0 address for a RabbitMQ exchange
                # Format: /exchanges/<exchange_name>/<routing_key>
                # message.channel contains the original queue name
                amqp_to_address = f"/exchanges/{retry_exchange}/delay.{delay_ms}.{message.channel}"

                await message.send_message(
                    channel=message.channel, # The channel doesn't matter since `to` overrides it
                    payload=message.payload,
                    content_type=message.content_type,
                    headers=new_headers,
                    server_specific_parameters={"to": amqp_to_address},
                )

                # Return normally to suppress the exception.
                # In auto mode, this causes Repid to ACK the original message!
                return None  # type: ignore[return-value]

        # Apply the merged signature to the wrapper so Repid's DI can parse it
        wrapper.__signature__ = new_sig  # type: ignore[attr-defined]
        return wrapper

    return decorator
```

### Using the Decorator

Now, your actual actor implementation becomes incredibly clean. You only need to focus on
your business logic, and the decorator handles the rest:

```python
router = Router()

@router.actor(channel="my_work_queue")
@with_rabbitmq_retries(max_retries=5, retry_exceptions=(ConnectionError, TimeoutError))
async def process_task(data: dict) -> None:
    print(f"Processing data: {data}")

    if data.get("bad_payload"):
        # This will NOT be retried. It immediately propagates to Repid and gets NACK-ed.
        raise ValueError("Invalid data format")

    # This WILL be caught by the decorator and retried with backoff.
    raise ConnectionError("Temporary API failure")
```

## Summary

By combining **Topic Exchanges**, **Message TTLs**, and **Repid's extensible Server
architecture**:

1. You can exponentially back off failed jobs without overwhelming your consumers.
2. A **single** set of backoff queues handles delays for **all** your work queues
   automatically.
3. You maintain safety against unhandled application crashes via Quorum Queue
   `x-delivery-limit`.
