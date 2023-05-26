# Quickstart Guide

## Before we start

To follow this guide you will need a virtual environment with `repid` installed.
We will use in memory brokers, but feel free to exchange those for any other ones -
it should *just work*.

## Consumer

Let's start by creating a simple pseudo-async function that counts length of a string:

```python
import asyncio


async def string_length(the_string: str) -> int:
    await asyncio.sleep(1)
    print(the_string)
    return len(the_string)
```

Now, we have to create a router for `repid` to know, that `string_length` actor exists.

```python hl_lines="5 8"
import asyncio

import repid

router = repid.Router()


@router.actor
async def string_length(the_string: str) -> int:
    await asyncio.sleep(1)
    print(the_string)
    return len(the_string)
```

Aaand let's finish our application with specifying connection and creating a worker.

```python
import asyncio

import repid

app = repid.Repid(repid.Connection(repid.InMemoryMessageBroker()))  # (1)

router = repid.Router()


@router.actor
async def string_length(the_string: str) -> int:
    await asyncio.sleep(1)
    print(the_string)
    return len(the_string)


async def main() -> None:  # (2)
    async with app.magic():  # (3)
        worker = repid.Worker(routers=[router])  # (4)
        await worker.run()  # (5)


if __name__ == "__main__":
    asyncio.run(main())
```

1. Create a `repid` app with the in-memory message broker.
2. The main function, which will execute our async code.
3. Inside of this context manager every object will be provided with the connection
that is attached to the `repid` app which we've created.
4. Create an instance of a worker. Don't forget to specify our router!
5. Run the worker until it receives a `SIGINT` ( ++ctrl+c++ ) or a `SIGTERM`.

## Producer

Let's enqueue a job!

```python hl_lines="18-24" title="example.py"
import asyncio

import repid

app = repid.Repid(repid.Connection(repid.InMemoryMessageBroker()))

router = repid.Router()


@router.actor
async def string_length(the_string: str) -> int:
    await asyncio.sleep(1)
    print(the_string)
    return len(the_string)


async def main() -> None:
    async with app.magic():
        hello_job = repid.Job(
            "string_length",  # (1)
            args=dict(the_string="Hello world!"),  # (2)
        )
        await hello_job.queue.declare()  # (3)
        await hello_job.enqueue()
        worker = repid.Worker(routers=[router])
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

1. Name of the job will be used to route it to the similarly named actor.
2. Using dictionary to map arguments' names to values.
The dictionary will be encoded with `json` module by default.
3. You only need to declare a queue once. Ideally you would do that on application startup.

This will enqueue a job to the default queue, which than worker will consume
& route to the `string_length` function with argument `the_string` set to `"Hello world!"`.

After running the script, you should receive:

```bash
$ python example.py

Hello world!

```
