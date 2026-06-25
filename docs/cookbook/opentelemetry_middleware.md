# OpenTelemetry Middleware

[OpenTelemetry](https://opentelemetry.io/) gives you vendor-neutral traces and metrics for your
Repid producers and actors. This cookbook shows how to configure Repid middlewares that propagate
trace context through message headers and record message processing and publishing metrics.

## Prerequisites

Install the OpenTelemetry API, SDK, and an exporter. This example uses the OTLP gRPC exporter,
which works with OpenTelemetry Collector and many observability vendors.

```bash
pip install repid opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc
```

If your application already configures OpenTelemetry, keep that setup and only add the Repid
middlewares below.

## OpenTelemetry Setup

Configure the OpenTelemetry SDK once when your process starts. A worker process and any process that
publishes Repid messages should both initialize OpenTelemetry. The exact exporter, endpoint,
resource attributes, sampling, and authentication settings are up to your application and depend
heavily on your observability infrastructure.

Below is a minimal OTLP gRPC example, not a required Repid configuration.

```python
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_opentelemetry(service_name: str = "repid-worker") -> None:
    resource = Resource.create({"service.name": service_name})

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)

    metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter())
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)
```

## The Middlewares

In total, we will create 4 middlewares: 2 metric reporting middlewares, and
2 tracing middlewares - each for producer and consumer parts.

When a producer sends a Repid message, the active `traceparent` and `tracestate` headers
are injected into the message. When a worker consumes that message,
the actor span continues the same distributed trace.

The span names and attributes follow the
[OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/#messaging-attributes).

```python
from collections.abc import Callable, Coroutine
from dataclasses import replace
from time import perf_counter
from typing import Any, TypeVar

from opentelemetry import context as context_api
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract, inject
from repid import ActorData, MessageData, ReceivedMessageT

T = TypeVar("T")

TRACER = trace.get_tracer(__name__)
METER = metrics.get_meter(__name__)

# Pick the semantic-convention value for your broker, for example
# "rabbitmq", "kafka", "aws_sqs", or "gcp_pubsub". If none applies, use a
# stable low-cardinality custom value.
MESSAGING_SYSTEM = "rabbitmq"

# Use operation names that match your broker or client terminology.
PRODUCER_OPERATION_NAME = "send"
CONSUMER_OPERATION_NAME = "process"

REPID_CONSUMER_MESSAGES = METER.create_counter(
    name="repid.consumer.messages",
    unit="{message}",
    description="The number of messages consumed by Repid actors.",
)
REPID_CONSUMER_FAILURES = METER.create_counter(
    name="repid.consumer.failures",
    unit="{message}",
    description="The number of Repid actor message handling failures.",
)
REPID_CONSUMER_DURATION = METER.create_histogram(
    name="repid.consumer.processing.duration",
    unit="s",
    description="The time spent handling a consumed Repid message.",
)

REPID_PRODUCER_MESSAGES = METER.create_counter(
    name="repid.producer.messages",
    unit="{message}",
    description="The number of messages published by Repid producers.",
)
REPID_PRODUCER_FAILURES = METER.create_counter(
    name="repid.producer.failures",
    unit="{message}",
    description="The number of Repid producer publish failures.",
)
REPID_PRODUCER_DURATION = METER.create_histogram(
    name="repid.producer.publish.duration",
    unit="s",
    description="The time spent publishing a Repid message.",
)


async def consumer_tracing_middleware(
    call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, T]],
    message: ReceivedMessageT,
    actor: ActorData,
) -> T:
    span_attributes = {
        "messaging.system": MESSAGING_SYSTEM,
        "messaging.destination.name": message.channel,
        "messaging.operation.type": "process",
        "messaging.operation.name": CONSUMER_OPERATION_NAME,
        "repid.actor.name": actor.name,
        "repid.actor.channel_address": actor.channel_address,
    }
    if message.message_id:
        span_attributes["messaging.message.id"] = message.message_id

    ctx = extract(message.headers or {})
    token = context_api.attach(ctx)
    try:
        with TRACER.start_as_current_span(
            f"{CONSUMER_OPERATION_NAME} {message.channel}",
            kind=trace.SpanKind.CONSUMER,
            attributes=span_attributes,
        ) as span:
            try:
                return await call_next(message, actor)
            except Exception as exc:
                span.set_attribute("error.type", type(exc).__name__)
                raise
    finally:
        context_api.detach(token)


async def consumer_metrics_middleware(
    call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, T]],
    message: ReceivedMessageT,
    actor: ActorData,
) -> T:
    metric_attributes = {
        "messaging.system": MESSAGING_SYSTEM,
        "messaging.destination.name": message.channel,
        "repid.actor.name": actor.name,
        "repid.actor.channel_address": actor.channel_address,
    }
    started_at = perf_counter()
    REPID_CONSUMER_MESSAGES.add(1, attributes=metric_attributes)

    try:
        return await call_next(message, actor)
    except Exception as exc:
        REPID_CONSUMER_FAILURES.add(
            1,
            attributes={**metric_attributes, "error.type": type(exc).__name__},
        )
        raise
    finally:
        REPID_CONSUMER_DURATION.record(
            perf_counter() - started_at,
            attributes=metric_attributes,
        )


async def producer_tracing_middleware(
    call_next: Callable[
        [str, MessageData, dict[str, Any] | None],
        Coroutine[Any, Any, T],
    ],
    channel: str,
    message: MessageData,
    server_specific_parameters: dict[str, Any] | None,
) -> T:
    span_attributes = {
        "messaging.system": MESSAGING_SYSTEM,
        "messaging.destination.name": channel,
        "messaging.operation.type": "send",
        "messaging.operation.name": PRODUCER_OPERATION_NAME,
    }

    with TRACER.start_as_current_span(
        f"{PRODUCER_OPERATION_NAME} {channel}",
        kind=trace.SpanKind.PRODUCER,
        attributes=span_attributes,
    ) as span:
        headers = dict(message.headers or {})
        inject(headers)
        replaced_message = replace(message, headers=headers)

        try:
            return await call_next(channel, replaced_message, server_specific_parameters)
        except Exception as exc:
            span.set_attribute("error.type", type(exc).__name__)
            raise


async def producer_metrics_middleware(
    call_next: Callable[
        [str, MessageData, dict[str, Any] | None],
        Coroutine[Any, Any, T],
    ],
    channel: str,
    message: MessageData,
    server_specific_parameters: dict[str, Any] | None,
) -> T:
    metric_attributes = {
        "messaging.system": MESSAGING_SYSTEM,
        "messaging.destination.name": channel,
    }
    started_at = perf_counter()
    REPID_PRODUCER_MESSAGES.add(1, attributes=metric_attributes)

    try:
        return await call_next(channel, message, server_specific_parameters)
    except Exception as exc:
        REPID_PRODUCER_FAILURES.add(
            1,
            attributes={**metric_attributes, "error.type": type(exc).__name__},
        )
        raise
    finally:
        REPID_PRODUCER_DURATION.record(
            perf_counter() - started_at,
            attributes=metric_attributes,
        )
```

!!! note
    Keep metric attributes low-cardinality. Channel names, actor names, and error types are usually
    safe. Message IDs are useful on spans, but should not be added to metrics.

## Registering the Middlewares

Lastly, you need to register those middlewares:

```python
from repid import Repid, Router

from my_project.observability import configure_opentelemetry
from my_project.repid_otel import (
    consumer_metrics_middleware,
    consumer_tracing_middleware,
    producer_metrics_middleware,
    producer_tracing_middleware,
)

configure_opentelemetry(service_name="repid-worker")

app = Repid(
    actor_middlewares=[consumer_tracing_middleware, consumer_metrics_middleware],
    producer_middlewares=[producer_tracing_middleware, producer_metrics_middleware],
)

router = Router()


@router.actor(channel="charge_customer")
async def charge_customer(customer_id: str) -> None:
    print(f"Charging {customer_id}")


app.include_router(router)
```
