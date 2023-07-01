from __future__ import annotations

import asyncio
import subprocess
from time import perf_counter

from arq import ArqRedis
from redis.asyncio import Redis

MESSAGES_AMOUNT = 80000
PROCESSES = 8

myredis = Redis.from_url("redis://:testtest@localhost:6379/0")
arq_redis = ArqRedis(myredis.connection_pool)


async def benchmark_task(ctx: dict) -> None:  # noqa: ARG001
    await asyncio.sleep(1.0)
    await myredis.incr("tasks_done", 1)


async def prepare() -> None:
    await arq_redis.delete("arq:queue")
    await myredis.set("tasks_done", 0)
    for i in range(MESSAGES_AMOUNT):
        await arq_redis.enqueue_job("benchmark_task")
        print(f"Enqueued: {i}", end="\r", flush=True)


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


class WorkerSettings:
    functions = [benchmark_task]  # noqa: RUF012
    log_results = False
    redis_pool = arq_redis
    max_jobs = 2000


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Enqueueing messages...")
    loop.run_until_complete(prepare())
    print("Done enqueueing.")

    processes: list[subprocess.Popen] = []

    print("Starting benchmark.")

    start = perf_counter()

    for _ in range(PROCESSES):
        processes.append(
            subprocess.Popen(
                ["arq", "benchmark_arq.WorkerSettings"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ),
        )

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
        process.wait()
