# Connecting & Nesting Routers

By themselves, routers do nothing. You must include them in your main Repid application instance so
the worker knows they exist and can start listening for their tasks.

```python
from repid import Repid
from myapp.routers import router

app = Repid()

# Register all actors defined on the router
app.include_router(router)
```

## Default Configurations

When you create a `Router`, you can pass default configurations. Any actor attached to this router
will inherit these defaults unless they explicitly override them.

```python
from repid import Router

# All actors on this router will default to the "background_jobs" channel
# and have a timeout of 10 seconds.
router = Router(channel="background_jobs", timeout=10.0)

@router.actor()  # Inherits "background_jobs" and 10s timeout
async def task_a():
    pass

@router.actor(timeout=30.0)  # Overrides timeout, but inherits "background_jobs"
async def task_b():
    pass
```

## Nesting Routers

Just like how you can attach routers to your `Repid` app, you can also attach routers *to other
routers* using `router.include_router()`. This allows you to build deeply nested, highly modular
applications.

When you include a child router into a parent router, **defaults propagate downwards**. If the child
router doesn't specify a configuration (like `channel` or `timeout`), it will inherit the parent's
configuration!

```python
parent_router = Router(channel="main_queue", timeout=60.0)
child_router = Router()

@child_router.actor()
async def my_nested_actor():
    pass

# The child_router will now inherit `channel="main_queue"` and `timeout=60.0`
parent_router.include_router(child_router)

# Finally, connect the parent to the app
app.include_router(parent_router)
```

!!! note "Middlewares Concatenation"
    When dealing with middlewares during nesting, they are **concatenated**,
    not overwritten. The parent router's middlewares will run *before*
    the child router's middlewares!
