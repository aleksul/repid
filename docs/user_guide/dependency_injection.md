# Dependency injection

Repid includes a powerful, flexible Dependency Injection (DI) system out-of-the-box.

Dependency Injection allows you to inject required components (like database connections, settings,
or generic logic) directly into your actor functions without tightly coupling them or passing the
same objects repeatedly.

## Depends

In your actor, you can declare a dependency as follows:

```python hl_lines="7-8 12"
from typing import Annotated
from repid import Repid, Router, Depends

app = Repid()
router = Router()

def dependency_function() -> str:
    return "Hello!"

@router.actor
async def my_actor(
    my_dependency: Annotated[str, Depends(dependency_function)]
) -> None:
    print(my_dependency)  # Will print `Hello!`
```

## Sub-dependencies

Your dependency can also have some dependencies of its own!

```python hl_lines="6-7 10"
from typing import Annotated
from repid import Depends, Router

router = Router()

def subdependency_function() -> str:
    return "world!"

def dependency_function(
    sub: Annotated[str, Depends(subdependency_function)]
) -> str:
    return "Hello " + sub

@router.actor
async def my_actor(
    my_dependency: Annotated[str, Depends(dependency_function)]
) -> None:
    print(my_dependency)  # Will print `Hello world!`
```

## Extracting parameters from payload

A dependency can even request fields that are provided via the incoming message payload or headers.
If a parameter in the dependency has no `Depends` annotation, Repid will look for it in the parsed
JSON payload.

Dependencies can request the exact same parameters that the main actor requests, but they can also
request their own unique parameters. Repid intelligently inspects the main actor and all of its
dependencies, combining all requested fields into one big model that is parsed simultaneously!

```python
from repid import Header
from typing import Annotated

async def verify_user(
    # These fields must exist in the payload
    user_id: int,
    verification_token: str,
    # This will extract a header
    correlation_id: Annotated[str | None, Header(alias="X-Correlation-ID")] = None
):
    if user_id <= 0:
        raise ValueError("Invalid user_id")
    return True

@router.actor
async def update_user(
    # user_id is requested by BOTH the actor and the dependency!
    user_id: int,
    # The dependency requires verification_token, so it becomes required in the payload!
    is_verified: Annotated[bool, Depends(verify_user)]
):
    # `user_id` and `verification_token` from the payload were passed to `verify_user` first!
    return f"User {user_id} is verified: {is_verified}"
```

## Sync and Async dependencies

Your dependencies' functions can be both synchronous and asynchronous.

### Asynchronous

```python hl_lines="1"
async def dependency_function() -> str:
    await asyncio.sleep(0.1)  # Imitates some async work
    return "Hello world!"

Depends(dependency_function)
```

### Synchronous

```python hl_lines="1"
def dependency_function() -> str:
    return "Hello world!"

Depends(dependency_function)
```

!!! note "Sync Overhead"
    Because synchronous dependencies are executed in a thread or process pool
    (to avoid blocking the main event loop), they introduce a small amount of context-switching
    overhead. If your dependency is extremely lightweight and doesn't actually block (e.g., simply
    returning a value from a dictionary or reading an environment variable), it is often more performant
    to define it as an `async def` function anyway, even if it contains no `await` statements.

### Synchronous (CPU-heavy)

In case your function is synchronous, it will be run in a thread pool executor to avoid blocking the
event loop.

You can also opt to run it in a process pool executor if your function is CPU bound.

```python hl_lines="5"
def dependency_function() -> int:
    # some CPU-heavy computation here
    return 123

Depends(dependency_function, run_in_process=True)
```

## Overriding dependencies

You can override a dependency globally.

!!! warning "For Testing Purposes Only"
    Dependency overrides are designed **primarily for testing**.
    Because overrides cause global state mutations across your application, it is highly recommended
    *not* to use them in production code. Use them exclusively in your test suites to safely mock
    external resources like database connections or API clients.

```python hl_lines="6"
def dependency_function() -> str:
    return "Hello!"

d = Depends(dependency_function)

d.override(lambda: "Overridden!")

@router.actor
async def my_actor(
    my_dependency: Annotated[str, d]
) -> None:
    print(my_dependency)  # Will print `Overridden!`
```
