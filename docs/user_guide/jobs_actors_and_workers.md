# Jobs, Actors and Workers

Now, that you now how to properly set up a connection, let's focus on creation of jobs and
their processing.

## Jobs

Job is a data structure, which describes what and how you want to be executed.

As you may want the execution to be periodic or to be retried in case of a failure, 1 job doesn't
necessarily translate to 1 message on the broker's side.

The only required parameter of a Job is `name`. It specifies what `actor` has to be called on the
consumer side. Routing is done on application level.

### Simple job

It's extremely easy to create a job:

```python hl_lines="10"
import asyncio

from repid import Connection, InMemoryMessageBroker, Job, Repid

app = Repid(Connection(InMemoryMessageBroker()))


async def main() -> None:
    async with app.magic():
        Job(name="my_awesome_job")


if __name__ == "__main__":
    asyncio.run(main())
```

### Jobs and queues

To schedule an execution of a job, you have to call `enqueue` method...

```python
# code above is omitted

async with app.magic():
    j = Job(name="my_awesome_job")

    await j.enqueue()  # 💥 KeyError: 'default'

# code below is omitted
```

...but it will fail, because we haven't initialized the default queue. Let's fix it:

```python hl_lines="5"
# code above is omitted

async with app.magic():
    j = Job(name="my_awesome_job")  # (1)
    await j.queue.declare()
    await j.enqueue()

    # ✅ Success!

# code below is omitted
```

1. By default queue is set to `Queue(name="default")`

You may also want to initialize queue(-s) preemptively, in, e.g., `setup` function.

```python
from repid import Queue

# code above is omitted

async def setup() -> None:
    async with app.magic():
        await Queue().declare()  # (1)
        await Queue(name="my_awesome_queue").declare()


# code below is omitted
```

1. Will declare a queue with name set to `default`

!!! tip
    Usually brokers support multiple queue declarations (== re-declaring already existing queue
    will have no effect), meaning that you can declare a queue on every app startup
    to ensure correct workflow of your application.

You can specify `queue` argument either as a string or as a `Queue` object. In case of a string it
will be automatically converted to a `Queue` object during `Job` object initialization.

```python
my_awesome_job = Job(name="my_awesome_job", queue="non-default-queue")
another_job = Job(name="another_job", queue=Queue("another-queue"))

print(type(my_awesome_job.queue))  # repid.Queue
print(type(another_job.queue))  # repid.Queue
```

### Job priority

You can specify priority of the job using `PrioritiesT` enum.
Default is set to `PrioritiesT.MEDIUM`. `HIGH`, `MEDIUM` & `LOW` levels are officially supported.

```python
from repid import Job, PrioritiesT

# code above is omitted

Job("awesome_job_with_priority", priority=PrioritiesT.HIGH)

# code below is omitted
```

### Job id and uniqueness

By default Job doesn't have any id and on every `enqueue` call a message will be generated
a new id using `uuid.uuid4().hex`. Therefore, by default, a Job isn't considered unique.

You can check it with `is_unique` flag.

```python
print(Job("non_unique_job").is_unique)  # False
```

If you specify `id_` argument - it will not be regenerated on every call of `enqueue` and therefore
the Job is considered unique.

```python
print(Job("unique_job", id_="my_unique_id").is_unique)  # True
```

!!! warning
    Some, although not all, brokers are ensuring that there are no messages with
    the same id in the queue.

### Retries

If you want a Job to be retried in case of a failure during the execution -
specify `retries` argument. Default is set to `0`, which means that the Job will only be executed
once and put in a dead-letter queue in case of a failure.

```python
Job("retryable_job", retries=3)
```

### Timeout

You can also specify maximum allowed execution time for a Job using `timeout` argument.
Default is set to `10 minutes`.

```python
from datetime import timedelta

# code above is omitted

Job("very_fast_job", timeout=timedelta(seconds=5))
Job("very_slow_job", timeout=timedelta(hours=2))

# code below is omitted
```

Execution timeout is also a valid reason for a retry.

!!! tip
    You should sensibly limit Job's timeout, as it can be a deciding factor for rescheduling in case
    of a worker disconnection.

## Actors

Actors are functions, which are meant to be executed when a worker (== consumer) receives a message.

To create an actor you first have to create a `Router` and then use `Router.actor` as a decorator
for your function.

```python
from repid import Router

my_router = Router()


@my_router.actor
async def my_awesome_function() -> None:
    print("Hello Repid!")
```

!!! note
    Actor decorator does not anyhow change the decorated function. The initial function remains
    callable as it was before.

### Actor name

By default, name of an actor is the same as the name of the function.
You can override it with `name` argument.

```python hl_lines="6"
from repid import Router

my_router = Router()


@my_router.actor(name="another_name")
async def my_awesome_function() -> None:
    print("Hello Repid!")
```

### Actor queue

You can also specify what queue should actor listen on using `queue` argument.

```python hl_lines="1"
@my_router.actor(queue="another_queue")
async def my_awesome_function() -> None:
    ...
```

### Thread vs Process

If your function is synchronous, you can specify if you want it to be run in a separate process
instead of a thread by setting `run_in_process` to True.

```python hl_lines="1"
@my_router.actor(run_in_process=True)
def my_synchronous_function() -> None:
    ...
```

??? warning
    If you are running on the `emscripten` platform (e.g. PyScript) `run_in_process` setting
    doesn't take any effect.

### Retry Policy

In case of a retry, application will reschedule the message with a delay. The delay is calculated
using a retry policy, which defaults to exponential. You can override the behavior
with `retry_policy` argument.

```python hl_lines="7"
from repid import Router, default_retry_policy_factory

my_router = Router()


@my_router.actor(
    retry_policy=default_retry_policy_factory(
        min_backoff=60,
        max_backoff=86400,
        multiplier=5,
        max_exponent=15,
    )
)
async def my_exceptional_function() -> None:
    raise Exception("Some random exception.")
```

...or you can use practically any function, which confirms to `RetryPolicyT`.

```python hl_lines="6"
from repid import Router

my_router = Router()


@my_router.actor(retry_policy=lambda retry_number=1: retry_number * 100)
async def my_exceptional_function() -> None:
    raise Exception("Some random exception.")
```

## Workers

Workers are controllers, which are responsible for receiving messages and executing related actors.

### Workers and Routers

Workers are also routers. You can assign actors directly to them.

```python hl_lines="12"
import asyncio

from repid import Connection, InMemoryMessageBroker, Job, Repid, Worker

app = Repid(Connection(InMemoryMessageBroker()))


async def main() -> None:
    async with app.magic():
        my_worker = Worker()

        @my_worker.actor
        async def my_func() -> None:
            print("Hello!")


if __name__ == "__main__":
    asyncio.run(main())
```

You can include actors from other routers into a worker.

!!! warning
    Adding actor with already existing name will override previously stored actor.

```python
router = Router()
other_router = Router()

# code above is omitted

Worker(routers=[router, other_router])

# or

my_worker = Worker()

my_worker.include_router(router)
my_worker.include_router(other_router)

# code below is omitted
```

### Running a Worker

To run a worker simply call `run` method.
The worker will be running until it receives a signal (`SIGINT` or `SIGTERM`, by default)
or until it reaches `messages_limit`, which by default is set to infinity.

!!! tip
    Worker will declare all queues that it's aware about (via assigned actors) on the execution of
    `run` method. To override this behavior set `auto_declare` flag to False.

```python hl_lines="11"
import asyncio

from repid import Connection, InMemoryMessageBroker, Repid, Worker

app = Repid(Connection(InMemoryMessageBroker()))


async def main() -> None:
    async with app.magic(auto_disconnect=True):
        my_worker = Worker()
        await my_worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

After worker receives a signal or reaches `messages_limit` it will attempt a graceful shutdown.
It means that it will stop receiving new messages and wait for `graceful_shutdown_time`
(default: 25 seconds) to let already running actors complete.

!!! note
    Default `graceful_shutdown_time` is set to 25 seconds because default Kubernetes timeout
    after sending `SIGTERM` and before sending `SIGKILL` is set to 30 seconds. 5 second buffer is
    provided to ensure that every uncompleted task will be rejected before ungraceful termination.

Any actor which exceeds that period will be forced to stop and corresponding message
will be rejected, meaning that the message broker will be responsible for requeueing the message
and retry counter will not be increased.

```python
from signal import SIGQUIT

# code above is omitted

Worker(
    graceful_shutdown_time=100.0,  # seconds
    messages_limit=10_000,
    handle_signals=[SIGQUIT],
)

# code below is omitted
```

??? warning
    If you are running on the `emscripten` platform (e.g. PyScript) signal handling is disabled
    by default, as it isn't supported by the platform.

After Worker is done running, it will return an internal `_Runner` object, which you can use
to retrieve information about amount of actor runs.

```python hl_lines="11-12"
import asyncio

from repid import Connection, InMemoryMessageBroker, Repid, Worker

app = Repid(Connection(InMemoryMessageBroker()))


async def main() -> None:
    async with app.magic(auto_disconnect=True):
        my_worker = Worker()
        runner = await my_worker.run()
        print(f"Total actor runs: {runner.processed}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Recap

1. Declare a `Queue` and enqueue a `Job`
2. Create an `Actor`
3. Assign it to a `Worker` directly or via a `Router`
4. Run the `Worker`

```python
import asyncio

import repid

app = repid.Repid(repid.Connection(repid.InMemoryMessageBroker()))

router = repid.Router()


@router.actor
async def awesome_hello() -> None:
    print("Hello from an actor!")


async def producer_side() -> None:
    async with app.magic():
        await repid.Queue().declare()
        await repid.Job("awesome_hello").enqueue()


async def consumer_side() -> None:
    async with app.magic(auto_disconnect=True):
        await repid.Worker(routers=[router], messages_limit=1).run()


async def main() -> None:
    await producer_side()
    await consumer_side()


if __name__ == "__main__":
    asyncio.run(main())
```
