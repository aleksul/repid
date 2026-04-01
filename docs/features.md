# Features

Repid is built to handle everything from simple local scripts to complex, multi-broker enterprise
architectures. Below is a comprehensive overview of the framework's capabilities.

## Architecture & Extensibility

- **Message Protocols Abstraction**: Switch between RabbitMQ, Redis, Google Cloud PubSub,
  or an In-Memory broker by simply changing one line of code.
- **Language & Framework Agnostic**: Repid doesn't enforce an internal message format. You can use
  Repid strictly as a Consumer to process JSON jobs sent from an external server, or strictly as a
  Producer to push jobs into target queues.
- **Pluggable Core**: Disagree with the defaults? You can easily inject your own custom data
  Serializers (e.g. replacing JSON with MessagePack or Protobuf) and custom Converters to handle
  payload-to-argument mapping.
- **Middleware System**: Hook into the lifecycle of messages globally or on a per-actor basis.
  Intercept messages before they are sent (Producer Middleware) or right before they are executed
  (Actor Middleware) to add custom telemetry, logging, or tracing.

## Execution & Concurrency

- **Asyncio Top-to-Bottom**: Built natively on Python's `asyncio` for maximum I/O concurrency
  without thread overhead.
- **Thread & Process Pools**: Not all tasks are I/O bound. You can configure synchronous CPU-heavy
  actors to automatically run in separate `ProcessPoolExecutor` or `ThreadPoolExecutor`
  environments, preventing them from blocking the main event loop.
- **Granular Concurrency Limits**: Control exactly how many tasks a worker can process concurrently
  to prevent overloading your Python process.
- **Timeouts**: Protect your queues from hanging connections by enforcing strict execution timeouts
  on an actor-by-actor basis.

## Routing & Dispatch

- **Application-Level Routing**: Route messages from a single queue to multiple actors without
  relying on complex broker configurations like RabbitMQ exchanges.
- **Built-in Routing Strategies**: Choose from topic-based routing (actors pick up messages with
  matching `topic` headers), catch-all routing (for legacy systems), or define custom functions
  based on headers or payload inspection to determine message delivery.

## Production Readiness

- **Native Graceful Shutdowns**: Workers automatically intercept `SIGTERM` and `SIGINT` signals,
  pausing the consumption of new messages and waiting for currently running actors to finish safely
  before disconnecting.
- **Embedded Health Check Server**: Easily integrate with Docker Swarm or Kubernetes
  liveness/readiness probes without needing to install an external ASGI web framework.
- **Message Acknowledgments**: Granular control over when messages are acknowledged (`ack`),
  negatively acknowledged (`nack`), or rejected back to the queue (auto, manual, or ack-first
  modes).
- **Poison Message Handling**: Automatically detect and drop/DLQ messages that crash the worker loop
  repeatedly.

## Developer Experience (DX)

- **Dependency Injection**: Inject database connections, tracking IDs, or parsed headers directly
  into your actor signatures.
- **Built-in TestClient**: Write fast, isolated unit and integration tests without needing a real
  message broker running.
- **AsyncAPI Integration**: Serve a beautiful, interactive web UI documenting your exact queue
  architecture, channels, and payload schemas out of the box.
