# Features

Repid is built to handle everything from simple local scripts to complex, multi-broker enterprise
architectures. Below is a comprehensive overview of the framework's capabilities.

## Architecture & Extensibility

- **[Message Protocols Abstraction](getting_started/supported_brokers.md)**:
  Switch between RabbitMQ, Redis, Google Cloud Pub/Sub,
  or an In-Memory broker by simply changing one line of code.
- **Language & Framework Agnostic**: Repid doesn't enforce an internal message format. You can use
  Repid strictly as a Consumer to process jobs sent from an external server, or strictly as a
  Producer to push jobs into target queues.
- **Pluggable Core**: Disagree with the defaults? You can easily inject your own custom data
  [Serializers](advanced_user_guide/your_own_serializer.md)
  (e.g. replacing JSON with MessagePack or Protobuf)
  and custom [Converters](advanced_user_guide/your_own_converter.md) to handle
  payload-to-argument mapping.
- **[Middleware System](cookbook/middlewares.md)**: Hook into the lifecycle of messages globally
  or on a per-actor basis. Intercept messages before they are sent (Producer Middleware)
  or right before they are executed (Actor Middleware) to add custom telemetry, logging, or tracing.

## Execution & Concurrency

- **Asyncio Top-to-Bottom**: Built natively on Python's `asyncio` for maximum I/O concurrency
  without thread overhead.
- **Thread & Process Pools**: Not all tasks are I/O bound. You can configure synchronous CPU-heavy
  actors to automatically run in separate `ProcessPoolExecutor` or `ThreadPoolExecutor`
  environments, preventing them from blocking the main event loop.
- **[Granular Concurrency Limits](user_guide/workers/concurrency.md)**: Control exactly how many
  tasks a worker can process concurrently to prevent overloading your Python process.
- **[Timeouts](user_guide/actors/execution.md)**: Protect your queues from hanging connections
  by enforcing strict execution timeouts on an actor-by-actor basis.

## Routing & Dispatch

- **[Application-Level Routing](user_guide/actors/routers.md)**: Route messages from a single queue
  to multiple actors without relying on complex broker configurations like RabbitMQ exchanges.
- **[Built-in Routing Strategies](user_guide/actors/routing.md)**: Choose from topic-based routing
  (actors pick up messages with matching `topic` headers), catch-all routing,
  or define custom functions based on headers or payload inspection to determine message delivery.

## Production Readiness

- **[Native Graceful Shutdowns](user_guide/workers/lifecycle.md)**: Workers automatically intercept
  `SIGTERM` and `SIGINT` signals, pausing the consumption of new messages and waiting for
  currently running actors to finish safely before disconnecting.
- **[Embedded Health Check Server](user_guide/workers/built_in_servers.md)**: Easily integrate
  with Docker Swarm or Kubernetes liveness/readiness probes without needing
  to install an external ASGI web framework.
- **[Message Acknowledgments](user_guide/actors/confirmation_and_errors.md)**: Granular control
  over when messages are acknowledged (`ack`), negatively acknowledged (`nack`), or rejected
  back to the queue (auto, manual, or ack-first modes).

## Developer Experience (DX)

- **[Dependency Injection](user_guide/dependency_injection.md)**: Inject database connections,
  tracking IDs, or parsed headers directly into your actor signatures.
- **[Built-in TestClient](cookbook/testing.md)**: Write fast, isolated unit and integration tests
  without needing a real message broker running.
- **[AsyncAPI Integration](integrations/asyncapi.md)**: Serve a beautiful, interactive web UI
  documenting your exact queue architecture, channels, and payload schemas out of the box.
