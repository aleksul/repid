# Chaining jobs

You can chain jobs using eager response and callback. You can find more details about those
in the user guide.

We will also take advantage of result bucket technically being a superset
of the arguments bucket, which allows us to pass result of one job as arguments to another one.

That is, if simplified, it looks as follows:

```python
Job("job1", result_id="my_chained_id")  # (1)
Job("job2", args_id="my_chained_id")
```

1. Result bucket id corresponds to the second job's arguments bucket id

---

Now, let's imagine you were designing some pipeline of jobs. The first one would construct some sort
of a greetings message and pass it to the second job. The second job will append to the greetings
message some information about user's id.

```python
import asyncio
import os

from repid import (
    Connection,
    Job,
    MessageDependency,
    RedisBucketBroker,
    RedisMessageBroker,
    Repid,
    Router,
    Worker,
)

redis_messages_dsn = os.environ.get("REDIS_CONNECTION")
redis_args_and_results_dsn = os.environ.get("REDIS_ARGS_CONNECTION")

my_connection = Connection(
    message_broker=RedisMessageBroker(redis_messages_dsn),
    args_bucket_broker=RedisBucketBroker(redis_args_and_results_dsn),
    results_bucket_broker=RedisBucketBroker(
        redis_args_and_results_dsn,
        use_result_bucket=True,
    ),
)

app = Repid(my_connection)

my_router = Router()


@my_router.actor
async def add_hello(msg: MessageDependency, user_name: str, user_id: int) -> dict:
    msg.set_result(dict(user_id=user_id, greetings=f"Hello {user_name}!"))

    j = Job("add_id", args_id=msg.parameters.result.id_, result_id="some_result_id")
    msg.add_callback(j.enqueue)

    await msg.ack()


@my_router.actor
async def add_id(user_id: int, greetings: str) -> str:
    return f"{greetings} Your id is {user_id}."


async def main() -> None:
    async with app.magic(auto_disconnect=True):
        w = Worker(routers=[my_router], messages_limit=2)

        await Job(
            "add_hello",
            args=dict(user_name="Alex", user_id=123),
            result_id="chained_id",
        ).enqueue()

        await w.run()

        result_bucket = await Job("add_id", result_id="some_result_id").result
        print(result_bucket.data)  # (1)


if __name__ == "__main__":
    asyncio.run(main())
```

1. Prints `Hello Alex! Your id is 123.`
