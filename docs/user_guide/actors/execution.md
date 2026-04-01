# Execution & Timouts

Repid gives you several options for configuring how an actor executes under the hood, depending on
whether it is CPU-bound, I/O-bound, or prone to hanging.

## Timeouts

By default, an actor can run indefinitely. If a task hangs (e.g. waiting for a database lock or an
external API without a timeout), it will occupy a slot in the worker forever.

You can set a `timeout` in seconds to ensure the actor is killed if it takes too long. If the
timeout is reached, Repid will cancel the task and reject the message (returning it to the queue).

```python
import asyncio

@router.actor(channel="api_tasks", timeout=10.0)
async def fetch_slow_api():
    # If this takes more than 10 seconds, it will be cancelled
    await asyncio.sleep(20)
```

## Thread vs Process Execution

If your actor function is purely synchronous (e.g., standard `def` instead of `async def`), Repid
will automatically run it in a thread pool to avoid blocking the main async event loop.

However, if your synchronous function is heavily CPU-bound (like image processing or large math
computations), threads will bottleneck due to Python's Global Interpreter Lock (GIL). You can tell
Repid to run the actor in a completely separate process using `run_in_process=True`.

```python
import math

@router.actor(channel="heavy_math", run_in_process=True)
def calculate_prime(number: int) -> bool:
    # This CPU-heavy computation runs in an isolated process!
    if number <= 1:
        return False
    for i in range(2, int(math.sqrt(number)) + 1):
        if number % i == 0:
            return False
    return True
```

!!! warning "Process Limitations"
    When using `run_in_process=True`, your actor function and all of
    its arguments must be fully `pickle`-able. Dependencies cannot be
    injected if they maintain active sockets or unpicklable state.

## Custom Executors

When you run synchronous functions in threads or processes, Repid creates default
`ThreadPoolExecutor` or `ProcessPoolExecutor` pools for you.

If you want finer control (like limiting the number of worker threads specifically for one actor, or
reusing an existing executor), you can pass a custom `pool_executor`:

```python
from concurrent.futures import ThreadPoolExecutor

# Create a dedicated thread pool for database tasks, limiting concurrent connections
db_pool = ThreadPoolExecutor(max_workers=5)

@router.actor(channel="db_tasks", pool_executor=db_pool)
def run_heavy_query():
    # Only up to 5 of these will run at the same time across the entire worker
    pass
```

!!! note
    If both `pool_executor` and `run_in_process` are specified,
    `pool_executor` will be used.
