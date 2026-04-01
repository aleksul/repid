# Quickstart

## App structure

Usually, Repid applications are split into two conceptual parts:

- **Producer**: Enqueues or sends new messages to the queue (usually within your web server).
- **Consumer (Worker)**: A background process that loops, pulls messages, and executes them via
  actors.

The framework is intentionally designed to separate these concerns, meaning you can easily have them
in different codebases.

!!! tip "Framework & Language Agnostic"
    Repid is highly unopinionated. It does not enforce any
    specific internal message format! Because you can easily override how payloads are decoded (via
    serializers/converters) and how messages are routed (via custom routing strategies), you can use
    Repid strictly as a Consumer to process jobs sent from an entirely different system, or
    strictly as a Producer to push jobs into queues.

## Servers (Choosing a Broker)

First of all, you need to establish a connection to a message broker. Repid abstracts brokers into
"Servers".

Repid provides a few servers out-of-the-box (like AMQP, Redis, and GCP PubSub), but you will need to
specify extra dependencies to use them:

For AMQP / RabbitMQ, install:

```bash
pip install repid[amqp]
```

For Redis, install:

```bash
pip install repid[redis]
```

For Google Cloud PubSub, install:

```bash
pip install repid[pubsub]
```

Mostly for test purposes or simple local scripts, Repid provides an `InMemoryServer`. You don't need
any extra dependencies for it.

## Infrastructure Topology

!!! warning "Queue Creation"
    Repid assumes that your underlying infrastructure (queues, topics,
    exchanges) **already exists**. It does not handle "topology creation" (like running `queue_declare`
    on RabbitMQ or `create_topic` on Google Cloud PubSub) automatically. It is up to you (or your
    deployment scripts) to ensure the target queues/channels exist on your broker before attempting
    to send or consume messages.

## The Repid Application

To initialize Repid, you instantiate a `Repid` object and register your servers.

```python
import os
from repid import Repid, AmqpServer

# 1. Create your application
app = Repid(title="My Awesome App")

# 2. Register a server and mark it as default
server = AmqpServer("amqp://localhost")
app.servers.register_server("my_rabbitmq", server, is_default=True)
```

The name you provide (e.g., `"my_rabbitmq"`) is just an identifier you can use later. Because we
passed `is_default=True`, any messages sent without specifying a particular server will
automatically be routed to this one.

## Sending Messages (Producer)

Whenever you want to send a message to a queue, you need to ensure the connection to the server is
open. You can handle connections using async context managers.

```python
import asyncio
from repid import Repid, InMemoryServer

app = Repid()
app.servers.register_server("default", InMemoryServer(), is_default=True)

async def main():
    # Retrieve the default server and open a connection
    async with app.servers.default.connection():
        # You can now safely publish messages
        await app.send_message_json(
            channel="default_queue",
            payload={"message": "Hello, Repid!"},
            headers={"topic": "my_actor"} # Don't forget the topic if using topic routing!
        )

if __name__ == "__main__":
    asyncio.run(main())
```

### Integration with Web Frameworks

If you are using Repid alongside an ASGI web framework (like FastAPI, Robyn, or Litestar), you
should open the server connection during the application's startup phase and close it during
shutdown using the framework's native `lifespan` handlers.

You can find a complete, ready-to-run code example of this exact pattern in our
[Integrations Guide](../integrations/fastapi_and_pydantic.md).

## Running Workers (Consumer)

To actually *process* the messages you just sent, you need to define an Actor and run a Worker.

Workers should generally be run as an entirely separate script or process from your web server. It
does not need to be the exact same `Repid` application instance in memory, it just needs to be
connected to the same underlying server/broker!

```python title="worker.py"
import asyncio
from repid import Repid, Router, InMemoryServer

# 1. Initialize an app for the worker to use
app = Repid()
app.servers.register_server("default", InMemoryServer(), is_default=True)

# 2. Create a router and an actor
router = Router()

@router.actor(channel="default_queue")
async def my_action(message: str):
    print(f"Executing with payload: {message}")

app.include_router(router)

# 3. Start the worker loop!
async def run_worker():
    # Like the producer, the connection must be opened first
    async with app.servers.default.connection():
        # Blocks forever, constantly pulling new messages and running actors
        await app.run_worker()

if __name__ == "__main__":
    asyncio.run(run_worker())
```
