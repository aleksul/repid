from __future__ import annotations

import asyncio
from multiprocessing import Process
from time import perf_counter

import uvloop
from redis.asyncio import Redis

from repid import Connection, Job, Queue, RabbitMessageBroker, Repid, Router, Worker

MESSAGES_AMOUNT = 80000
PROCESSES = 8

app = Repid(Connection(RabbitMessageBroker("amqp://user:testtest@localhost:5672")))

myredis = Redis.from_url("redis://:testtest@localhost:6379/0")

r = Router()


@r.actor
async def benchmark_task() -> None:
    await asyncio.sleep(1.0)
    await myredis.incr("tasks_done", 1)


async def prepare() -> None:
    async with app.magic(auto_disconnect=True):
        q = Queue()
        await q.declare()
        await q.flush()
        j = Job("benchmark_task")
        await asyncio.gather(*[j.enqueue() for _ in range(MESSAGES_AMOUNT)])
    await myredis.set("tasks_done", 0)


async def run() -> None:
    async with app.magic(auto_disconnect=True):
        await Worker(
            routers=[r],
            graceful_shutdown_time=0,
        ).run()


async def report(start: float) -> None:
    tasks_done = int(await myredis.get("tasks_done"))

    while tasks_done < MESSAGES_AMOUNT:
        print(
            f"Tasks done: {tasks_done}/{MESSAGES_AMOUNT}.",
            f"Time elapsed: {perf_counter() - start:.2f} sec.",
            end="\r",
            flush=True,
        )
        await asyncio.sleep(0.1)
        tasks_done = int(await myredis.get("tasks_done"))


if __name__ == "__main__":
    uvloop.install()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Enqueueing messages...")
    loop.run_until_complete(prepare())
    print("Done enqueueing.")

    processes: list[Process] = []

    for _ in range(PROCESSES):
        processes.append(Process(target=lambda: asyncio.run(run())))

    print("Starting benchmark.")

    start = perf_counter()

    for process in processes:
        process.start()

    loop.run_until_complete(report(start))

    end = perf_counter()

    print(
        "",
        "Benchmark ended.",
        f"Took {end - start:.2f} sec.",
        f"Rate {MESSAGES_AMOUNT / (end - start):.2f} msg/sec.",
        sep="\n",
    )

    for process in processes:
        process.terminate()
        process.join()
