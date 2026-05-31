# RabbitMQ Retries with Exponential Backoff

When a task fails, retrying it with increasing delays (exponential backoff) prevents your system from being overwhelmed by failing dependencies.

This cookbook explains how to implement exponential backoff using **RabbitMQ 4.x** (or 3.13+ by enabling plugin `rabbitmq-plugins enable rabbitmq_amqp1_0`). It uses **Quorum Queues** to prevent poison messages and a smart **Topic Exchange** topology that shares a single set of delay queues across all your work queues.

*(Inspired by [Brian Storti's article](https://www.brianstorti.com/rabbitmq-exponential-backoff/))*

## The Architecture & Message Flow

RabbitMQ lacks native per-message delays. We simulate delays using queues with a Time-To-Live (`x-message-ttl`). When the TTL expires, messages are dead-lettered back to an exchange.

Creating a dedicated delay queue for *every* work queue and delay interval is inefficient. Instead, we use **Topic Exchanges** to share delay queues.

Here is the lifecycle of a retried message:

```
graph TD
    WQ[(my_work_queue)] -->|Consumes| W[Worker]

    W -->|1st Fail Routing Key:<br/>delay.10000.my_work_queue| RX{{retry_exchange}}
    W -->|2nd Fail Routing Key:<br/>delay.60000.my_work_queue| RX

    RX -->|Binding:<br/>delay.10000.#| DQ1[(delay_10s<br/>TTL: 10s)]
    RX -->|Binding:<br/>delay.60000.#| DQ2[(delay_60s<br/>TTL: 60s)]

    DQ1 -.->|Dead-letter<br/>Routing Key preserved| RQX{{requeue_exchange}}
    DQ2 -.->|Dead-letter<br/>Routing Key preserved| RQX

    RQX -->|Binding:<br/>*.*.my_work_queue| WQ
```

1. **Fail**: A worker fails to process a message. The application calculates the delay, increments the retry counter, and publishes it to `retry_exchange` with a routing key like `delay.10000.my_work_queue` (or `delay.60000.my_work_queue` for the next attempt).
1. **Wait**: The exchange routes it to the corresponding shared delay queue (e.g., `delay_10s` or `delay_60s`) via the wildcard bindings. These queues have no consumers and a matching `x-message-ttl`.
1. **Expire**: The TTL expires. RabbitMQ dead-letters the message to `requeue_exchange`, preserving the original routing key (e.g., `delay.10000.my_work_queue`).
1. **Requeue**: `requeue_exchange` routes the message back to `my_work_queue` via the wildcard binding `*.*.my_work_queue`.

## Setting Up the Topology

Configure this topology using your preferred administration tool. Below is an example using RabbitMQ's native Definitions JSON format.

Create a `definitions.json` file:

```
{
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

You can upload this configuration using the RabbitMQ Management HTTP API via `curl` (assuming the management plugin `rabbitmq_management` is enabled):

```
curl -i -u guest:guest -H "content-type:application/json" -X POST \ # (1)!
  -d @definitions.json http://localhost:15672/api/definitions
```

1. Or specify your proper credentials instead of `guest:guest`

*(Alternatively, you can load these on server startup by pointing the `load_definitions` configuration file in `rabbitmq.conf` to this JSON file).*

## The Decorator Implementation

To keep our actors clean and reusable, we can encapsulate the entire retry, delay calculation, and republishing logic into a single Python decorator.

This decorator dynamically merges the `Message` dependency into your actor's signature so Repid can inject it at runtime. It leverages Repid's default `confirmation_mode="auto"` to elegantly handle successes (auto-ack) and exhausted retries (re-raise for `on_error` handling).

```
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
        func: Callable[P, Awaitable[R]],
    ) -> Callable[Concatenate[Message, P], Awaitable[R]]:
        sig = inspect.signature(func)  # (1)!

        if "message" not in sig.parameters:
            new_params = [
                *list(sig.parameters.values()),
                inspect.Parameter("message", inspect.Parameter.KEYWORD_ONLY, annotation=Message)
            ]
            new_sig = sig.replace(parameters=new_params)
        else:
            new_sig = sig

        @wraps(func)
        async def wrapper(message: Message, *args: P.args, **kwargs: P.kwargs) -> R:
            try:
                if "message" in sig.parameters:
                    kw = cast(dict[str, Any], kwargs)
                    kw["message"] = message
                    return await func(*args, **kw)  # (2)!
                else:
                    return await func(*args, **kwargs)

            except retry_exceptions as e:  # (3)!
                headers = message.headers or {}
                retry_count = int(headers.get("x-retry-count", 0))

                if retry_count >= max_retries:
                    print(f"Max retries ({max_retries}) reached for message {message.message_id}")
                    raise  # (4)!

                delay_index = min(retry_count, len(backoff_delays) - 1)
                delay_ms = backoff_delays[delay_index]

                print(f"Task failed: {e}. Retrying in {delay_ms}ms (Attempt {retry_count + 1})")

                new_headers = headers.copy()
                new_headers["x-retry-count"] = str(retry_count + 1)

                amqp_to_address = (
                  f"/exchanges/{retry_exchange}/delay.{delay_ms}.{message.channel}"  # (5)!
                )

                await message.send_message(
                    channel=message.channel,
                    payload=message.payload,
                    content_type=message.content_type,
                    headers=new_headers,
                    server_specific_parameters={"to": amqp_to_address},
                )

                return None  # (6)!

        wrapper.__signature__ = new_sig  # (7)!
        return wrapper

    return decorator
```

1. We must dynamically merge the signature so Repid injects BOTH your custom payload arguments AND the `Message` dependency. Without this, `@wraps` hides `message`, or removing `@wraps` hides your payload args!
1. Execute the original actor code, safely omitting `message` if they didn't explicitly request it in their signature.
1. Handle the failure and calculate backoff. Any exception NOT in `retry_exceptions` will bypass this block and be immediately handled by Repid's `on_error` policy (no retries).
1. Re-raise the exception so Repid's auto mode catches it and naturally nacks/rejects it based on your actor settings.
1. Explicitly construct the AMQP 1.0 address for a RabbitMQ exchange (format: `/exchanges/<exchange_name>/<routing_key>`).
1. Return normally to suppress the exception. In auto mode, this causes Repid to automatically `ack` the original message!
1. Apply the merged signature to the wrapper so Repid's DI parser knows `message` needs to be provided.

### Using the Decorator

Now, your actual actor implementation becomes incredibly clean. You only need to focus on your business logic, and the decorator handles the rest:

```
router = Router()

@router.actor(channel="my_work_queue")
@with_rabbitmq_retries(max_retries=5, retry_exceptions=(ConnectionError, TimeoutError))
async def process_task(data: dict) -> None:
    print(f"Processing data: {data}")

    if data.get("bad_payload"):
        raise ValueError("Invalid data format")  # (1)!

    raise ConnectionError("Temporary API failure")  # (2)!
```

1. This will NOT be retried. It bypasses our decorator exception block and immediately propagates to Repid to be handled (e.g. NACK-ed or Dead Lettered).
1. This WILL be caught by the decorator and retried with exponential backoff!
