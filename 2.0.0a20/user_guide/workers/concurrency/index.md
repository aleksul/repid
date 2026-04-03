# Tasks Limit

A Repid worker runs inside a single `asyncio` event loop. It can pull multiple messages from the broker and execute them concurrently.

You can configure `tasks_limit` when starting the worker:

```
# Cap the worker at processing up to 2000 messages concurrently
await app.run_worker(tasks_limit=2000)
```

By default, Repid allows up to **1000 tasks** inside a single worker.

Concurrency vs Bottlenecks

The `tasks_limit` is not primarily meant as a strict business concurrency restriction, but rather a way to avoid overloading the Python process. Because `asyncio` runs entirely on a single CPU core, scheduling too many concurrent tasks will cause the event loop to start throttling. This typically happens in the range of ~1000 to 4000 concurrent tasks depending on your hardware. You are highly encouraged to benchmark your specific workload and adjust this limit to find your process's optimal sweet spot.

Scaling Out

If you need to process more messages than a single Python event loop can handle without throttling, you should scale vertically (run multiple workers using multiprocessing) and/or horizontally (by running multiple instances of the application, e.g. docker containers or kubernetes pods) rather than endlessly increasing `tasks_limit`.
