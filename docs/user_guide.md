# User Guide

To follow this guide you need a running instance of `Redis` and virtual environment with `Repid` installed.

## Worker

Let's write a simple pseudo-async function that counts length of a string:

```python
import asyncio

async def string_length(the_string: str) -> int:
    await asyncio.sleep(1)
    return len(the_string)

```

Now, to add queuing and RPC functionality to this function all we have to do is:

```python hl_lines="5 6 8 13"
import asyncio
from redis.asyncio import Redis
import repid

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
worker = repid.Worker(redis)

@worker.actor()
async def string_length(the_string: str) -> int:
    await asyncio.sleep(1)
    return len(the_string)

asyncio.run(worker.run_forever())

```

!!! note "Important"
    All input and return arguments **must** be JSON-encodable.

The example above is a totally valid RPC server but what if we want to use sync code? Let's try this out!

```python hl_lines="13-16"
import asyncio
from redis.asyncio import Redis
import repid

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
worker = repid.Worker(redis)

@worker.actor()
async def string_length(the_string: str) -> int:
    await asyncio.sleep(1)
    return len(the_string)

@worker.actor()
def count_file_length() -> int:
    with open("myfile.txt") as f:
        return len(f.readline())

asyncio.run(worker.run_forever())

```

Every time `count_file_length` function will be called, it will be executed in a separate thread using `ThreadPoolExecutor`, so it won't stop other async code.

## Producer

Simplest example to enqueue a job is:

```python
import asyncio
from redis.asyncio import Redis
import repid

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
producer = repid.Repid(redis)

asyncio.run(producer.enqueue_job("my_func_name"))
```

This will enqueue a job for the func `my_func_name` within queue `default`.

## Queuing

### Type of queue

`Repid` realizes FIFO (First In - First Out) queues.

`Repid` tries to avoid receiving one job twice by different workers.
To do so it pulls out a job from the queue before getting final decision will the job be done or not.
If something goes wrong during this process there may be a situation when execution of the job was missed.

### Deferred tasks

Actually, there are 2 queues under the hood.
One is for normal tasks, and another one is for deferred.
Deferred tasks have priority over normal.

### Unexisting actor

If a job was taken from the queue but not found on the worker it will be re-enqueued.
You should avoid these situations, however it may be helpful if you are doing canary release or refactoring.

### Non-default queue

If a queue is not mentioned `default` queue will be used.

```python hl_lines="9"
import asyncio
from redis.asyncio import Redis
import repid
from datetime import timedelta

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
producer = repid.Repid(redis)

asyncio.run(await producer.enqueue_job("hello_another_queue", queue="myqueue"))
```

```python hl_lines="8"
import asyncio
from redis.asyncio import Redis
import repid

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
worker = repid.Worker(redis)

@worker.actor(queue="myqueue")
async def hello_another_queue():
    print("hi")

asyncio.run(worker.run_forever())

```

## Error handling

Let's say you have an actor that throws exception from time to time.

```python
@worker.actor()
async def exceptional_actor(i: int) -> int:
    if i == 3:
        raise Exception
    return i
```

By default it will run once and if it fails... well, it fails.
However, you can configure number of retries:

```python hl_lines="1"
@worker.actor(retries=3)
async def exceptional_actor(i: int) -> int:
    if i == 3:
        raise Exception
    return i
```

Now, if it fails it will retry specified number of times.

!!! note "Important"
    `Repid` doesn't return job back to the queue during retries, so the job is always executed on the same worker.

## Scheduling

You can defer your job's execution until or by some time. To do so, follow examples below.

### Defer job until 1 January 2020

```python hl_lines="10"
import asyncio
from redis.asyncio import Redis
import repid
from datetime import datetime

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
producer = repid.Repid(redis)

async def main():
    await producer.enqueue_job("happy_new_year", defer_until=datetime(2020, 1, 1))

asyncio.run(main())
```

### Run job every day

```python hl_lines="10"
import asyncio
from redis.asyncio import Redis
import repid
from datetime import timedelta

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
producer = repid.Repid(redis)

async def main():
    await producer.enqueue_job("good_morning", defer_by=timedelta(days=1))

asyncio.run(main())
```

## Results

Every job will have result stored even if job's function returns none.
You can access it with result property on the job.

```python hl_lines="12"
import asyncio
from redis.asyncio import Redis
import repid
from datetime import timedelta

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
producer = repid.Repid(redis)

async def main():
    myjob = await producer.enqueue_job("some_job")
    await asyncio.sleep(5)  # wait for the job to complete
    result: repid.JobResult = await myjob.result
    print(result)

asyncio.run(main())
```
