# Supported Brokers

Repid includes built-in support for several message brokers, accommodating various infrastructure requirements from local development to distributed enterprise systems.

Here is a quick overview to help you choose the right one, followed by detailed descriptions of each.

| Broker                                           | Best for...                                                     | Installation                  |
| ------------------------------------------------ | --------------------------------------------------------------- | ----------------------------- |
| **[In-Memory](#in-memory-built-in)**             | Local development, testing, and rapid prototyping               | *Built-in*                    |
| **[AMQP 1.0](#amqp-10-rabbitmq)**                | Enterprise architectures, robust routing, and strong guarantees | `pip install "repid[amqp]"`   |
| **[NATS](#nats)**                                | High-performance, lightweight distributed systems               | `pip install "repid[nats]"`   |
| **[Redis](#redis)**                              | Simple deployments and existing Redis stacks                    | `pip install "repid[redis]"`  |
| **[Google Cloud Pub/Sub](#google-cloud-pubsub)** | Serverless capabilities and GCP-native applications             | `pip install "repid[pubsub]"` |
| **[Amazon SQS](#amazon-sqs)**                    | Managed queuing service in AWS                                  | `pip install "repid[sqs]"`    |
| **[Apache Kafka](#apache-kafka)**                | High-throughput distributed event streaming                     | `pip install "repid[kafka]"`  |

______________________________________________________________________

## In-Memory (Built-in)

The **In-Memory** broker uses Python's standard `asyncio.Queue` to store messages directly in your application's memory.

- **When to use it:** The default choice for local development, rapid prototyping, and unit testing. While it *can* be used for single-process monolithic applications, it is **not recommended** for production environments.
- **Installation:** *Built-in (no installation required)*
- **Caveats:** All queued messages are permanently lost if your application crashes or restarts. Additionally, it cannot distribute tasks across multiple worker processes or machines.
- **Under the hood:** Relies entirely on native Python asynchronous primitives stored in memory. This ensures zero network overhead and immediate message delivery within a single process.

## AMQP 1.0 (RabbitMQ)

The **AMQP** broker provides seamless integration with mature, enterprise-grade messaging systems using the AMQP 1.0 protocol, most notably RabbitMQ.

- **When to use it:** Required for high reliability, granular message routing, strict acknowledgment mechanisms, or when integrating Repid into a polyglot microservice architecture. It's a good default choice.
- **Installation:** `pip install "repid[amqp]"`
- **Caveats:** Requires a broker that explicitly supports AMQP 1.0. For RabbitMQ versions prior to 4.0, you must enable the `rabbitmq_amqp1_0` plugin. Starting with version 4.0, AMQP 1.0 is supported natively.
- **Under the hood:** Repid manages its own native connection states and protocol framing using a vendorized AMQP layer. **Unlike comparable libraries that typically rely on the legacy AMQP 0.9.1 protocol, Repid uses AMQP 1.0, which is significantly more optimized and reliable.** By avoiding heavy third-party async Python clients, it delivers a streamlined and highly performant experience.

## NATS

The **NATS** broker allows you to use the high-performance NATS messaging system.

- **When to use it:** Great for lightweight, fast, and scalable distributed systems.
- **Installation:** `pip install "repid[nats]"`
- **Caveats:** Requires a running NATS server with JetStream enabled.
- **Under the hood:** Uses the official `nats-py` asynchronous client.

## Redis

The **Redis** broker offers an excellent balance between the simplicity of an in-memory queue and the advanced features of a dedicated message broker.

- **When to use it:** Ideal for persistent queuing and horizontal scalability, particularly if Redis is already part of your infrastructure stack.
- **Installation:** `pip install "repid[redis]"`
- **Caveats:** Requires Redis server version **5.0.0 or higher**, and the Python `redis` client **>=7.0.0**. While performant for many use cases, it lacks the complex routing capabilities found in brokers like RabbitMQ.
- **Under the hood:** Built on top of `redis.asyncio`, Repid leverages modern **Redis Streams** (using commands like `XADD`, `XREADGROUP`, and `XACK`). It natively handles consumer group offsets, making it significantly more reliable than older Redis Pub/Sub or list-based (`BLPOP`) task queue implementations.

## Google Cloud Pub/Sub

The **Google Cloud Pub/Sub** broker is designed specifically for serverless, horizontally scaling architectures on the Google Cloud Platform.

- **When to use it:** Ideal for fully managed, globally scalable queuing in GCP-exclusive environments, eliminating the maintenance overhead of self-managed clusters.
- **Installation:** `pip install "repid[pubsub]"`
- **Caveats:** Tightly coupled to the Google Cloud ecosystem. Not suitable for on-premise deployments or other cloud providers.
- **Under the hood:** Rather than wrapping the standard `google-cloud-pubsub` client library (which is internally synchronous), Repid reimplements the connection asynchronously. By utilizing raw async gRPC calls (`grpc.aio`) directly to Pub/Sub endpoints, Repid achieves superior connection resilience, concurrency, and performance tailored to its task processing needs.

## Amazon SQS

The **Amazon SQS** broker provides integration with AWS Simple Queue Service.

- **When to use it:** Ideal for fully managed, highly scalable queuing in AWS environments.
- **Installation:** `pip install "repid[sqs]"`
- **Caveats:** Tightly coupled to the AWS ecosystem.
- **Under the hood:** Leverages `aiobotocore` for asynchronous communication with SQS API.

## Apache Kafka

The **Apache Kafka** broker integrates Repid with Kafka for high-throughput event streaming.

- **When to use it:** Essential for large-scale distributed systems requiring robust event streaming and replay capabilities.
- **Installation:** `pip install "repid[kafka]"`
- **Caveats:** Requires a Kafka cluster.
- **Under the hood:** Utilizes `aiokafka` for asynchronous interaction with Kafka brokers.

______________________________________________________________________

## What if my broker isn't supported?

Repid's core framework is completely decoupled from its network layer. If your infrastructure relies on other systems, you can implement a custom connector.

Check out the [Your own brokers](https://repid.aleksul.space/latest/advanced_user_guide/your_own_brokers/index.md) guide for instructions on extending Repid.
