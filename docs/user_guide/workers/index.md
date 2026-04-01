# Workers

Workers are the long-running loops that continuously pull messages from the broker and pass them to
your actors for processing.

## Running a Worker

To start processing messages, you call `run_worker()` on your Repid application instance. Keep in
mind that it blocks for the whole time of the worker execution, like a `while True`.

```python title="worker.py" hl_lines="17"
import asyncio
from repid import Repid, Router, AmqpServer

app = Repid()
app.servers.register_server("default", AmqpServer("amqp://localhost"), is_default=True)

router = Router()

@router.actor(channel="user_events")
async def process_user_event(event_type: str):
    print(f"Event: {event_type}")

app.include_router(router)

async def main():
    async with app.servers.default.connection():
        await app.run_worker()

if __name__ == "__main__":
    asyncio.run(main())
```

!!! note "Deployment"
    You usually run your worker loop as an independent process/instance,
    separately from your main web server.

## Restarts & Messages Limit

In distributed orchestrated systems (like Kubernetes or Docker Swarm), it is a common practice to
periodically restart worker processes to prevent long-term memory leaks or zombie connections.

You can configure the worker to gracefully exit after processing a certain number of messages (by
default, it runs indefinitely):

```python
# Worker will exit successfully after processing exactly 1000 messages
await app.run_worker(messages_limit=1000)
```

Once the limit is reached, the worker triggers its internal shutdown sequence and returns control
back to your script, allowing the process to restart naturally.
