import subprocess
import time

import celery
from redis import Redis

MESSAGES_AMOUNT = 80000
USE_GREEN_THREADS = True
EVENTLETS = 750
PROCESSES = 8

myredis = Redis.from_url("redis://:testtest@localhost:6379/0")

celery_app = celery.Celery(broker="pyamqp://user:testtest@localhost:5672")


@celery_app.task(name="latency-bench", acks_late=True)
def latency_bench() -> None:
    time.sleep(1.0)
    myredis.incr("tasks_done", 1)


def prepare() -> None:
    for _ in range(MESSAGES_AMOUNT):
        latency_bench.delay()
    myredis.set("tasks_done", 0)


def report(start: float) -> None:
    tasks_done = int(myredis.get("tasks_done"))

    while tasks_done < MESSAGES_AMOUNT:
        print(
            f"Tasks done: {tasks_done}/{MESSAGES_AMOUNT} msg.",
            f"Time elapsed: {time.perf_counter() - start:.2f} sec.",
            sep="\n",
            end="\n\n",
        )
        time.sleep(0.1)
        tasks_done = int(myredis.get("tasks_done"))


if __name__ == "__main__":
    print("Enqueueing messages...")
    prepare()
    print("Done enqueueing.")

    print("Starting benchmark.")

    start_time = time.perf_counter()

    subprocess_args = ["celery", "-A", "benchmark_celery.celery_app", "worker"]
    if USE_GREEN_THREADS:
        subprocess_args.extend(["-P", "eventlet", "-c", str(EVENTLETS)])
    else:
        subprocess_args.extend(["-c", str(PROCESSES)])

    proc = subprocess.Popen(subprocess_args)

    report(start_time)

    duration = time.perf_counter() - start_time
    proc.terminate()
    proc.wait()

    print(
        "Benchmark ended.",
        f"Took {duration:.2f} sec.",
        f"Rate {MESSAGES_AMOUNT / (duration):.2f} msg/sec.",
        sep="\n",
    )
