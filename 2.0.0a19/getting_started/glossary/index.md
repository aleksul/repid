# Glossary

When working with Repid, you'll encounter a few core concepts and terms that form the foundation of the framework. This brief glossary will help you familiarize yourself with the terminology used throughout the documentation.

## Core Concepts

- **Message:** The fundamental unit of data sent through the queue. A message contains a payload (your data) and metadata (like a topic, queue name, and headers).
- **Actor:** An asynchronous Python function decorated with `@router.actor` that is responsible for processing a specific type of message. It is the "consumer" of your background tasks.
- **Topic:** A string label attached to a message that dictates how it should be routed. By default, an Actor listens for messages with a topic matching its function name.
- **Queue:** A named buffer within the message broker where messages wait before being processed by a worker.
- **Worker:** A background process (managed by `app.run_worker()`) that constantly pulls messages from the message broker and dispatches them to the corresponding actors.
- **Router:** An object (like `repid.Router()`) used to organize and group multiple actors together. You attach routers to your application when starting a worker.
- **Server / Broker:** The underlying infrastructure (like RabbitMQ, Redis, or an in-memory store) that physically stores and routes your messages. Repid connects to these via "Server" adapters (e.g., `AmqpServer`, `RedisServer`).

## Message States & Acknowledgments

When an actor finishes processing a message (or encounters an error), Repid must signal the broker about the outcome. This is known as acknowledgment. The framework handles this based on the `@router.actor(confirmation_mode=...)` setting, but the core states remain the same:

- **Ack (Acknowledge):** The actor successfully processed the message. The broker is instructed to safely remove the message from the queue.
- **Nack (Negative Acknowledge):** The actor encountered an error and failed to process the message. The broker is instructed to negatively acknowledge it. Depending on your specific broker's configuration, this might mean the message is retried, dropped, or moved to a Dead Letter Queue (DLQ).
- **Reject:** The execution is rejected, returning the message back to its original queue. This doesn't necessarily mean an error occurred; it can simply indicate that the actor or worker cannot accept the message at the moment. For example, Repid uses this state to return messages to the queue during a graceful shutdown, allowing them to be attempted again later or processed by another worker.

## AsyncAPI & Channels

Repid implements the AsyncAPI 3.0 specification for schema generation. A key concept here is the "Channel."

- **Channel:** In AsyncAPI, a channel is a generic pathway for transmitting messages. However, Repid seamlessly maps this generic concept to the specific routing infrastructure of your chosen message broker. Depending on the context:
- **AMQP (RabbitMQ):** For a producer (sending a message), a channel can map to either a *queue* (the default behavior) or an *exchange*. For a consumer (a worker receiving messages), a channel always maps strictly to a *queue*.
- **Google Cloud PubSub:** A producer's channel maps to a *Topic*, whereas a consumer's channel maps to a *Subscription*.
- **Redis / In-Memory:** The channel typically maps directly to the target list/queue key name.

## Advanced Concepts

- **Dependency Injection (DI):** A mechanism in Repid that allows your actor functions to declare dependencies (like a database connection, or specific message headers) in their signature. Repid automatically resolves and provides these dependencies when the actor is called.
- **Middleware:** Functions that wrap around the sending or processing of messages.
- **Producer Middleware:** Intercepts a message right before it is sent to the broker.
- **Actor Middleware:** Intercepts a message right before it is executed by an actor, and can modify the result or handle exceptions.
- **Health Check Server:** An optional, lightweight embedded HTTP server provided by Repid that exposes endpoints (like `/healthz` or `/livez`) to signal the worker's status to container orchestration tools (like Kubernetes).
- **AsyncAPI:** An open-source specification for defining asynchronous APIs. Repid can automatically generate AsyncAPI schemas from your actors and routers, and serve them via an embedded UI.
