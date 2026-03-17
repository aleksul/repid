from __future__ import annotations

import asyncio
import subprocess
from time import perf_counter

import uvloop
from redis.asyncio import Redis
from taskiq_aio_pika import AioPikaBroker

MESSAGES_AMOUNT = 8000
PROCESSES = 8
SLEEP_TIME = 1.0  # Try: 0.01, 0.1, 0.5, 1.0, 5.0
RUNS = 3

AMQP_URL = "amqp://user:testtest@localhost:5672"

myredis = Redis.from_url("redis://:testtest@localhost:6379/0")
broker = AioPikaBroker(AMQP_URL)


@broker.task
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    await myredis.incr("tasks_done", 1)


async def prepare() -> None:
    await broker.startup()
    for i in range(MESSAGES_AMOUNT):
        await benchmark_task.kiq()
        print(f"Enqueued: {i}", end="\r", flush=True)
    await broker.shutdown()
    await myredis.set("tasks_done", 0)


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

    rates: list[float] = []

    for run in range(1, RUNS + 1):
        print(f"Run {run}/{RUNS}: Enqueueing messages...")
        loop.run_until_complete(prepare())
        print("Done enqueueing.")

        processes: list[subprocess.Popen] = []

        for _ in range(PROCESSES):
            processes.append(
                subprocess.Popen(
                    ["taskiq", "worker", "benchmark_taskiq:broker"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                ),
            )

        print(f"Run {run}/{RUNS}: Benchmark started.")

        start = perf_counter()

        loop.run_until_complete(report(start))

        end = perf_counter()

        for process in processes:
            process.terminate()
            process.wait()

        rate = MESSAGES_AMOUNT / (end - start)
        rates.append(rate)

        print(
            "",
            f"Run {run}/{RUNS} ended.",
            f"Took {end - start:.2f} sec.",
            f"Rate {rate:.2f} msg/sec.",
            sep="\n",
        )

    if RUNS > 1:
        print(
            "",
            "All runs complete.",
            f"Average rate: {sum(rates) / len(rates):.2f} msg/sec.",
            f"Min rate: {min(rates):.2f} msg/sec.",
            f"Max rate: {max(rates):.2f} msg/sec.",
            sep="\n",
        )
