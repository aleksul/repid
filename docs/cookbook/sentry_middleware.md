# Sentry Middleware

[Sentry](https://sentry.io/) is an industry-standard error tracking platform. This
cookbook explains how to write a custom Repid Middleware to capture unhandled exceptions
in your actors and automatically report them to your Sentry dashboard.

## The Actor Middleware

When a worker picks up a message, we want to add context (like the channel and message ID)
and capture any exceptions that bubble up from the actor's execution.

We can achieve this by implementing an **Actor Middleware** that wraps the execution of
our tasks. By using `sentry_sdk.new_scope()`, we ensure that the tags and context are
isolated to this specific task execution and don't bleed into other asynchronous tasks
running concurrently.

```python
import sentry_sdk
from typing import Callable, Coroutine, TypeVar

from repid.connections.abc import ReceivedMessageT
from repid.data import ActorData

T = TypeVar("T")

async def sentry_actor_middleware(
    call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
    message: ReceivedMessageT,
    actor: ActorData,
) -> T:
    # 1. Isolate the context for this specific task execution
    with sentry_sdk.new_scope() as scope:
        # 2. Add useful contextual tags
        scope.set_tag("repid.channel", message.channel)
        scope.set_tag("repid.actor", actor.name)
        if message.message_id:
            scope.set_tag("repid.message_id", message.message_id)

        try:
            # 3. Execute the actor
            return await call_next(message, actor)

        except Exception as e:
            # 4. Capture the exception before letting it bubble up to Repid
            sentry_sdk.capture_exception(e)
            raise
```

## Registering the Middleware

Once defined, you apply the middleware to your Repid application, router, or specific
actor. Registering it on the main `Repid` app ensures all actors automatically report
errors.

```python
import sentry_sdk
from repid import Repid, Router

# Initialize Sentry
sentry_sdk.init()

# Register the middleware globally
app = Repid(actor_middlewares=[sentry_actor_middleware])

router = Router()

# Now any actor on this router will automatically report errors!
@router.actor(channel="my_queue")
async def process_task(data: dict) -> None:
    print("Processing task...")

    # Any exceptions here will immediately show up in your Sentry dashboard!
    # They will include the channel, actor name, and message ID tags!
    raise ValueError("Something went wrong!")
```
