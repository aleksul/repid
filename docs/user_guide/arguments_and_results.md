# Arguments & Results

Repid provides mechanisms to pass arguments to a job and retrieve the job's result.

## Preparation

First of all, you will have to define what will be used to store the data. Arguments can either be
stored inside of a message (so you only need message broker) or inside of a bucket broker.
Results, on the other hand, can only be stored in a result bucket broker.

For example, let's create a connection with InMemoryBucketBroker.

```python hl_lines="5-8"
from repid import Connection, InMemoryMessageBroker, InMemoryBucketBroker

my_connection = Connection(
    message_broker=InMemoryMessageBroker(),
    args_bucket_broker=InMemoryBucketBroker(),
    results_bucket_broker=InMemoryBucketBroker(,
        use_result_bucket=True,
    ),
)
```

We can then experiment with storing of our buckets.

## Arguments

First of all, let's create an actor with some arguments.

```python
from repid import Router

router = Router()


@router.actor
async def actor_with_args(user_id: int, user_name: str, user_messages: list[str]) -> list[str]:
    user_message = f"Hi {user_name}! Your id is: {user_id}."
    user_messages.append(user_message)
    return user_messages
```

Now we would like to schedule a job, which will pass those arguments to the actor.

```python
from repid import Job

# code above is omitted

await Job(
    "actor_with_args",
    args=dict(user_id=123, user_name="Alex", user_messages=["This is your first message!"]),
).enqueue()

# code below is omitted
```

What will happen under the hood?

1. `Job.args` will be serialized using `Config.SERIALIZER`
2. The serialized string will be
    - encoded into the message or
    - passed to arguments bucket broker if `Job.use_args_bucketer` is set to True
        - defaults to True if the current connection contains arguments bucket broker
3. When the message will be received by the actor, arguments will be decoded and mapped to
the function arguments using actor's `Converter.convert_inputs`
4. The return of the actor will be encoded using actor's `Converter.convert_outputs`

## Results

You can define whether to store result of a job or not using `Job.store_result` argument.

```python hl_lines="15"
Job("some_job", store_result=True)
```

The default is to store result, if connection contains result bucket broker,
and not to store result otherwise.

You can access result of a job using `Job.result` async property.

```python
import asyncio
from repid import Job

# code above is omitted

myjob = Job(
    "actor_with_args",
    args=dict(user_id=123, user_name="Alex", user_messages=["This is your first message!"]),
)

await myjob.enqueue()

await asyncio.sleep(1.0)  # wait for the job to complete

result_bucket = await myjob.result
print(result_bucket)

# code below is omitted
```

!!! info
    Results of recurring jobs will be overwritten.

## IDs

By default, an UUID4 will be generated upon creation of a job,
both in case of an arguments bucket and a result bucket.

You can pass your own IDs using appropriate arguments:

```python
Job("some_job", args_id="my_args_id", result_id="my_result_id")
```

## TTL

Repid also supports specifying Time-To-Live for both arguments and result buckets.

```python
from datetime import timedelta

Job("some_job", args_ttl=timedelta(weeks=2), result_ttl=timedelta(days=5))
```

You can also set TTL to None, which equals to no expiration.

```python
Job("some_job", args_ttl=None, result_ttl=None)
```

By default, arguments buckets have no TTL, while result buckets have TTL of one day.

!!! warning
    Not all bucket brokers may have native support for TTL, so be careful not to run out of memory.
