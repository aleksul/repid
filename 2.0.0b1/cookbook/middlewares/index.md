# Middlewares

Middlewares in Repid allow you to hook into the lifecycle of messages. You can use middlewares to add logging, telemetry, tracking, or alter messages on the fly.

There are two distinct types of middlewares depending on the part of the lifecycle you want to intercept: **Producer Middlewares** and **Actor Middlewares**.

## Actor Middleware

An actor middleware intercepts the execution of an actor when it receives a message from the queue. This is useful for metrics, tracing, error handling, or performance monitoring.

To create an actor middleware, you implement the `ActorMiddlewareT` protocol.

```
from typing import Any, Coroutine, Callable
from repid import ReceivedMessageT, ActorData

async def logging_middleware[T](
    call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
    message: ReceivedMessageT,
    actor: ActorData,
) -> T:
    print(f"Starting execution for channel: {message.channel}")

    try:
        # call_next invokes the next middleware, and eventually the actor itself
        result = await call_next(message, actor)
        print(f"Finished execution with result: {result}")
        return result
    except Exception as e:
        print(f"Actor raised an exception: {e}")
        raise e
```

```
from typing import Any, Coroutine, Callable, TypeVar
from repid import ReceivedMessageT, ActorData

T = TypeVar("T")

async def logging_middleware(
    call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
    message: ReceivedMessageT,
    actor: ActorData,
) -> T:
    print(f"Starting execution for channel: {message.channel}")

    try:
        # call_next invokes the next middleware, and eventually the actor itself
        result = await call_next(message, actor)
        print(f"Finished execution with result: {result}")
        return result
    except Exception as e:
        print(f"Actor raised an exception: {e}")
        raise e
```

## Producer Middleware

A producer middleware intercepts messages being sent to the message broker. This is perfect for appending distributed tracing IDs, adding default headers, or validating payloads before they are serialized and published.

To create a producer middleware, you implement the `ProducerMiddlewareT` protocol.

```
import uuid
from typing import Any, Coroutine, Callable
from repid import MessageData

async def producer_tracing_middleware[T](
    call_next: Callable[
        [str, MessageData, dict[str, object] | None],
        Coroutine[None, None, T],
    ],
    channel: str,
    message: MessageData,
    server_specific_parameters: dict[str, object] | None,
) -> T:
    # Add a default tracking header if not present
    headers = message.headers or {}

    if "X-Trace-Id" not in headers:
        headers["X-Trace-Id"] = str(uuid.uuid4())

    # Create a new message object with updated headers
    message = MessageData(
        payload=message.payload,
        headers=headers,
        content_type=message.content_type,
    )

    print(f"Publishing to '{channel}' with Trace ID: {headers['X-Trace-Id']}")

    # Proceed with the actual publish
    return await call_next(channel, message, server_specific_parameters)
```

```
import uuid
from typing import Any, Coroutine, Callable, TypeVar
from repid import MessageData

T = TypeVar("T")

async def producer_tracing_middleware(
    call_next: Callable[
        [str, MessageData, dict[str, object] | None],
        Coroutine[None, None, T],
    ],
    channel: str,
    message: MessageData,
    server_specific_parameters: dict[str, object] | None,
) -> T:
    # Add a default tracking header if not present
    headers = message.headers or {}

    if "X-Trace-Id" not in headers:
        headers["X-Trace-Id"] = str(uuid.uuid4())

    # Create a new message object with updated headers
    message = MessageData(
        payload=message.payload,
        headers=headers,
        content_type=message.content_type,
    )

    print(f"Publishing to '{channel}' with Trace ID: {headers['X-Trace-Id']}")

    # Proceed with the actual publish
    return await call_next(channel, message, server_specific_parameters)
```

## Registering Middlewares

Middlewares are registered when you instantiate your components. You can attach actor middlewares to the main `Repid` application, a specific `Router`, or even an individual actor. Producer middlewares are attached to the main `Repid` application.

They execute in an onion-like chain (first added -> wraps everything -> executes first and finishes last).

```
from repid import Repid, Router

# Register on the main app
app = Repid(
    actor_middlewares=[MetricsMiddleware(), LoggingActorMiddleware()],
    producer_middlewares=[TraceIdProducerMiddleware(), ValidationMiddleware()]
)

# Or register specifically on a router
router = Router(middlewares=[SpecificRouterMiddleware()])

# Or even a single actor!
@router.actor(middlewares=[SingleActorMiddleware()])
async def my_job() -> None:
    pass
```
