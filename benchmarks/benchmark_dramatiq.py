import subprocess
import time

import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from redis import Redis

MESSAGES_AMOUNT = 80000
USE_GREEN_THREADS = False
GEVENTS = 750
PROCESSES = 8
SLEEP_TIME = 1.0  # Try: 0.01, 0.1, 0.5, 1.0, 5.0
RUNS = 3

myredis = Redis.from_url("redis://:testtest@localhost:6379/0")

broker = RabbitmqBroker(url="amqp://user:testtest@localhost:5672")
dramatiq.set_broker(broker)


@dramatiq.actor
def benchmark_task() -> None:
    time.sleep(SLEEP_TIME)
    myredis.incr("tasks_done", 1)


def prepare() -> None:
    for i in range(MESSAGES_AMOUNT):
        benchmark_task.send()
        print(f"Enqueued: {i}", end="\r", flush=True)
    myredis.set("tasks_done", 0)


def report(start: float) -> None:
    tasks_done = int(myredis.get("tasks_done"))

    while tasks_done < MESSAGES_AMOUNT:
        print(
            f"Tasks done: {tasks_done}/{MESSAGES_AMOUNT}.",
            f"Time elapsed: {time.perf_counter() - start:.2f} sec.",
            end="\r",
            flush=True,
        )
        time.sleep(0.1)
        tasks_done = int(myredis.get("tasks_done"))


if __name__ == "__main__":
    rates: list[float] = []

    for run in range(1, RUNS + 1):
        print(f"Run {run}/{RUNS}: Enqueueing messages...")
        prepare()
        print("Done enqueueing.")

        print(f"Run {run}/{RUNS}: Benchmark started.")

        start_time = time.perf_counter()

        if USE_GREEN_THREADS:
            subprocess_args = [
                "dramatiq-gevent",
                "benchmark_dramatiq",
                "-p",
                str(PROCESSES),
                "-t",
                str(GEVENTS),
            ]
        else:
            subprocess_args = [
                "dramatiq",
                "benchmark_dramatiq",
                "-p",
                str(PROCESSES),
            ]

        proc = subprocess.Popen(subprocess_args)

        report(start_time)

        duration = time.perf_counter() - start_time

        proc.terminate()
        proc.wait()

        rate = MESSAGES_AMOUNT / duration
        rates.append(rate)

        print(
            "",
            f"Run {run}/{RUNS} ended.",
            f"Took {duration:.2f} sec.",
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
