# Actors

Actors are the functions that actually do the work. When a worker pulls a message from the broker, it looks for a matching actor and executes it.

## Defining Actors with Routers

Repid routes queue messages to functions using a `Router`. By attaching actors to a router, you can organize your tasks logically and keep your application modular.

To declare an actor, create a `Router` and decorate an async function with `@router.actor()`.

```
from repid import Router

router = Router()

@router.actor(channel="user_tasks")
async def send_welcome_email() -> None:
    print("Sending welcome email...")
```

Unit Testing

The `@router.actor` decorator does not mutate your function. You can still import and call `send_welcome_email()` directly in your unit tests without needing a running Repid application.
