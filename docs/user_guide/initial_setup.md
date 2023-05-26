# Initial setup

## App structure

Usually repid apps are split up in two parts: **producer** (the one, who enqueues/creates new jobs)
and **consumer** (the one who actually does all the heavy lifting).

The framework is intentionally designed to separate those two parts
and you may even have them in different codebases.

With that said, for simplicity, most of the examples in docs are using `InMemoryMessageBroker`,
which requires both sides to be run in one process.

## Brokers

First of all you will have to establish a connection using one of broker implementations.

Repid provides some brokers (RabbitMQ, Redis, etc.) out-of-the-box, but you will have to specify
extra dependencies for them to work:

For RabbitMQ install

```bash
pip install repid[amqp]
```

For Redis install

```bash
pip install repid[redis]
```

Mostly for test purposes, repid also provides `InMemoryMessageBroker` & `InMemoryBucketBroker`,
which are using RAM to store data, thus requiring your app to run in one context/process/etc.
You don't need any extra dependencies to use those.

Different brokers may have different initialization parameters, but often the pattern is to simply
pass a connection string. Bucket brokers may also have some way of specifying whether they are
supposed to be used to store results or not. This will come in handy later.

Let's suppose we want to use `RabbitMessageBroker`, so after we've installed
the necessary dependencies, you may write something like this:

```python
import os
from repid import RabbitMessageBroker

my_broker = RabbitMessageBroker(dsn=os.environ.get("RABBIT_CONNECTION_STRING"))
```

## Connection

After creating a broker instance you will have to create a `Connection`. Connection is a data
structure which will tie your message broker, bucker broker, result bucket broker & middleware.

Upon creation of a `Connection`, it will create a new `Middleware` instance and assign it
to supplied brokers. Thus, calling a broker before it was assigned a middleware won't call
any middleware functions.

Each `Connection` instance has `is_open` boolean flag. This flag is only updated when connection
instance itself is changing state (== using `connect` & `disconnect` methods).

!!! warning
    `is_open` flag doesn't track state of underlying broker connections. `Connection` class
    is **not** responsible for reconnection in case of a failure.

Using our previous example with `RabbitMessageBroker`, let's create a `Connection`:

```python hl_lines="2 6"
import os
from repid import Connection, RabbitMessageBroker

my_broker = RabbitMessageBroker(dsn=os.environ.get("RABBIT_CONNECTION_STRING"))

my_connection = Connection(my_broker)
```

Or here is another example with bucket brokers also specified:

```python
import os
from repid import Connection, RabbitMessageBroker, RedisBucketBroker

my_connection = Connection(
    message_broker=RabbitMessageBroker(dsn=os.environ.get("RABBIT_CONNECTION_STRING")),
    args_bucket_broker=RedisBucketBroker(dsn=os.environ.get("REDIS_ARGS_CONNECTION_STRING")),
    results_bucket_broker=RedisBucketBroker(
        dsn=os.environ.get("REDIS_RESULT_CONNECTION_STRING"),
        use_result_bucket=True,
    ),
)
```

## Magic

...or not really :upside_down:

So now you have this connection instance, but you would need to provide it to every object manually.
To avoid doing so, create a `Repid` instance and provide connection to it. Then you will be able
to use `magic` async context manager, which will automatically bind your connection
to the needed objects.

```python hl_lines="8"
from repid import Repid, Job

# `my_connection` definition is omitted

app = Repid(my_connection)

async def main() -> None:
    async with app.magic():
        j = Job("my_awesome_job")  # (1)
        await j.enqueue()

# `main` function call is omitted
```

1. You don't have to supply `_connection` argument to the `Job`, it's done :sparkles:auto-magically:sparkles:

If you want more control over the magic, you can use `magic_connect` & `magic_disconnect`.
So here is how you can use it with `FastAPI`:

```python hl_lines="13 18"
from fastapi import FastAPI
from repid import Repid, Job

# `my_connection` definition is omitted

repid_app = Repid(my_connection)

fastapi_app = FastAPI()


@fastapi_app.on_event("startup")
async def open_repid_connection() -> None:
    await repid_app.magic_connect()


@fastapi_app.on_event("shutdown")
async def close_repid_connection() -> None:
    await repid_app.magic_disconnect()


@fastapi_app.post("/create-job")
async def create_job() -> None:
    await Job("my_awesome_fastapi_job").enqueue()  # (1)
```

1. Again, you don't have to supply `_connection` argument to the `Job`, as long as `magic_connect`
was called before.

### ...but how does it work?

Internally, `Repid` class (not to be confused with **instances** of the `Repid` class) holds
a thread-local variable, which is used to store `Connection` object.
It also calls `connect`/`disconnect` method so you don't have to!

!!! warning
    Keep in mind that as connections are meant to be long-lived

    ```python
    async with app.magic()
        ...
    ```

    doesn't close connection on exit by default!

    To close the connection, set `auto_disconnect` flag to True

    ```python
    async with app.magic(auto_disconnect=True)
        ...
    ```

## Recap

1. Create a broker
2. Submit it to a `Connection`
3. Add it to `Repid` to get the :sparkles:magic:sparkles:
4. Use it!

```python
import asyncio
import os

from repid import Connection, Job, RabbitMessageBroker, RedisBucketBroker, Repid

app = Repid(
    Connection(
        message_broker=RabbitMessageBroker(dsn=os.environ.get("RABBIT_CONNECTION_STRING")),
        args_bucket_broker=RedisBucketBroker(dsn=os.environ.get("REDIS_ARGS_CONNECTION_STRING")),
        results_bucket_broker=RedisBucketBroker(
            dsn=os.environ.get("REDIS_RESULT_CONNECTION_STRING"),
            use_result_bucket=True,
        ),
    )
)


async def main() -> None:
    async with app.magic():
        await Job("my_awesome_job").enqueue()


if __name__ == "__main__":
    asyncio.run(main())
```
