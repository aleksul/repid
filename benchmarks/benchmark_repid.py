from __future__ import annotations

import asyncio
import base64
import urllib.error
import urllib.request
from multiprocessing import Process
from time import perf_counter

import uvloop
from redis.asyncio import Redis

from repid import AmqpServer, Repid, Router

MESSAGES_AMOUNT = 80000
PROCESSES = 8
ENQUEUE_BATCH_SIZE = 500

RABBITMQ_DSN = "amqp://user:testtest@localhost:5672"
RABBITMQ_MGMT_URL = "http://localhost:15672"
RABBITMQ_USER = "user"
RABBITMQ_PASS = "testtest"
CHANNEL = "default"

server = AmqpServer(RABBITMQ_DSN)

app = Repid()
app.servers.register_server("main", server, is_default=True)

myredis = Redis.from_url("redis://:testtest@localhost:6379/0")

r = Router()


@r.actor
async def benchmark_task() -> None:
    await asyncio.sleep(1.0)
    await myredis.incr("tasks_done", 1)


app.include_router(r)


def _purge_queue() -> None:
    """Purge the benchmark queue via the RabbitMQ Management API (ignores 404)."""
    url = f"{RABBITMQ_MGMT_URL}/api/queues/%2F/{CHANNEL}/contents"
    credentials = base64.b64encode(f"{RABBITMQ_USER}:{RABBITMQ_PASS}".encode()).decode()
    request = urllib.request.Request(
        url,
        method="DELETE",
        headers={"Authorization": f"Basic {credentials}"},
    )
    try:
        urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


async def prepare() -> None:
    _purge_queue()
    async with server.connection():
        for start_idx in range(0, MESSAGES_AMOUNT, ENQUEUE_BATCH_SIZE):
            count = min(ENQUEUE_BATCH_SIZE, MESSAGES_AMOUNT - start_idx)
            await asyncio.gather(
                *[
                    app.send_message(
                        channel=CHANNEL,
                        payload=b"",
                        headers={"topic": "benchmark_task"},
                    )
                    for _ in range(count)
                ],
            )
            print(f"Enqueued: {start_idx + count}/{MESSAGES_AMOUNT}", end="\r", flush=True)
    await myredis.set("tasks_done", 0)


async def run() -> None:
    async with server.connection():
        await app.run_worker(graceful_shutdown_time=0, tasks_limit=2000)


def _worker_process() -> None:
    uvloop.install()
    asyncio.run(run())


async def report(start: float) -> None:
    tasks_done = int(await myredis.get("tasks_done") or 0)

    while tasks_done < MESSAGES_AMOUNT:
        print(
            f"Tasks done: {tasks_done}/{MESSAGES_AMOUNT}.",
            f"Time elapsed: {perf_counter() - start:.2f} sec.",
            end="\r",
            flush=True,
        )
        await asyncio.sleep(0.1)
        tasks_done = int(await myredis.get("tasks_done") or 0)


if __name__ == "__main__":
    uvloop.install()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Enqueueing messages...")
    loop.run_until_complete(prepare())
    print("\nDone enqueueing.")

    processes: list[Process] = []

    for _ in range(PROCESSES):
        processes.append(Process(target=_worker_process))

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
