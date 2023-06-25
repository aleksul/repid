# Middlewares

!!! warning "Compatibility"
    Middlewares are a subject to change, as those are representing internal functions.

    They don't follow semantic versioning and can introduce breaking changes in minor versions.
    Patch versions, on the other hand, are guaranteed to **not** to introduce any breaking changes.

    Any breaking changes will be mentioned in release notes.

Some of Repid functions are wrapped to emit events before and after its execution.
You can subscribe to those events with your own middlewares.

To create a middleware, you can either use class-based or function-based approach.

## Class-based

For example, let's create a middleware which will report errors to Sentry.

```python
import os
import sentry_sdk
from repid import Repid, Connection, InMemoryMessageBroker
from repid.actor import ActorResult

app = Repid(Connection(InMemoryMessageBroker()))  # (1)


class SentryMiddleware:
    def __init__(self, sentry_dsn: str) -> None:
        sentry_sdk.init(dsn=sentry_dsn)

    def after_actor_run(self, result: ActorResult) -> None:  # (2)
        if not result.success:
            sentry_sdk.capture_exception(result.exception)  # (3)


sentry_middleware_instance = SentryMiddleware(os.environ.get("SENTRY_DSN"))  # (4)

app.connection.middleware.add_middleware(sentry_middleware_instance)  # (5)
```

1. Create a Repid app and a Connection as you normally would
2. Create a specifically named method, which will receive specifically named arguments
3. Report exceptions to Sentry
4. Initialize the middleware
5. Pass middleware to Repid's connection

## Function-based

Let's replicate the example above using function-based approach.

```python
import os
import sentry_sdk
from repid import Repid, Connection, InMemoryMessageBroker
from repid.actor import ActorResult

app = Repid(Connection(InMemoryMessageBroker()))  # (1)

sentry_sdk.init(dsn=os.environ.get("SENTRY_DSN"))


def after_actor_run(result: ActorResult) -> None:  # (2)
    if not result.success:
        sentry_sdk.capture_exception(result.exception)  # (3)


app.connection.middleware.add_subscriber(after_actor_run)  # (4)
```

1. Create a Repid app and a Connection as you normally would
2. Create a specifically named function, which will receive specifically named arguments
3. Report exceptions to Sentry
4. Subscribe the function to the appropriate event

## Naming convention, arguments, async

Only functions named accordingly to emitted events (see glossary below) will be executed.

You can optionally include any of the suggested arguments. Only included arguments will be passed
to the subscriber. The "after_" functions also contain a special argument `result`,
which represents the return of the function emitting the signal.

You can use both asynchronous & synchronous functions.
The latter will be executed in threads to not to block async loop.

## Events glossary

Here is the full list of events, their arguments and type hints, which you can subscribe to.

### Consume

- `#!python before_consume()`
- `#!python after_consume(result: tuple[RoutingKeyT, EncodedPayloadT, ParametersT])`

### Enqueue

- `#!python before_enqueue(key: RoutingKeyT, payload: EncodedPayloadT, params: ParametersT | None)`
- `#!python after_enqueue(key: RoutingKeyT, payload: EncodedPayloadT,
params: ParametersT | None, result: None)`

### Queue Declare

- `#!python before_queue_declare(queue_name: str)`
- `#!python after_queue_declare(queue_name: str, result: None)`

### Queue Flush

- `#!python before_queue_flush(queue_name: str)`
- `#!python after_queue_flush(queue_name: str, result: None)`

### Queue Delete

- `#!python before_queue_delete(queue_name: str)`
- `#!python after_queue_delete(queue_name: str, result: None)`

### Ack

- `#!python before_ack(key: RoutingKeyT)`
- `#!python after_ack(key: RoutingKeyT, result: None)`

### Nack

- `#!python before_nack(key: RoutingKeyT)`
- `#!python after_nack(key: RoutingKeyT, result: None)`

### Reject

- `#!python before_reject(key: RoutingKeyT)`
- `#!python after_reject(key: RoutingKeyT, result: None)`

### Requeue

- `#!python before_requeue(key: RoutingKeyT, payload: str, params: ParametersT | None)`
- `#!python after_requeue(key: RoutingKeyT, payload: str, params: ParametersT | None, result: None)`

### Get Bucket

- `#!python before_get_bucket(id_: str)`
- `#!python after_get_bucket(id_: str, result: BucketT | None)`

### Store Bucket

- `#!python before_store_bucket(id_: str, payload: BucketT)`
- `#!python after_store_bucket(id_: str, payload: BucketT, result: None)`

### Delete Bucket

- `#!python before_delete_bucket(id_: str)`
- `#!python after_delete_bucket(id_: str, result: None)`

### Actor Run

- `#!python before_actor_run(actor: ActorData, key: RoutingKeyT, parameters: ParametersT,
payload: str)`
- `#!python after_actor_run(actor: ActorData, key: RoutingKeyT, parameters: ParametersT,
payload: str, result: ActorResult)`
