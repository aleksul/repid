import asyncio
import multiprocessing
from datetime import timedelta

from repid import Job, Repid, Router, Worker

COUNTER1 = multiprocessing.Value("i", 0)
COUNTER2 = multiprocessing.Value("i", 0)

router = Router()


@router.actor
async def sleepy() -> None:
    COUNTER1.value += 1  # type: ignore[attr-defined]
    await asyncio.sleep(3)
    COUNTER2.value += 1  # type: ignore[attr-defined]


async def run_worker(repid_conn: Repid) -> None:
    async with repid_conn.magic(auto_disconnect=True):
        w = Worker(
            routers=[router],
            messages_limit=1,
            graceful_shutdown_time=0,
        )

        await w.run()


def run_worker_sync(repid_conn: Repid) -> None:
    asyncio.run(run_worker(repid_conn))


async def test_forced_worker_stop(autoconn: Repid) -> None:
    COUNTER1.value = 0  # type: ignore[attr-defined]
    COUNTER2.value = 0  # type: ignore[attr-defined]

    async with autoconn.magic(auto_disconnect=True):
        j = Job("sleepy", timeout=timedelta(seconds=4))
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    process = multiprocessing.Process(target=run_worker_sync, args=(autoconn,))
    process.start()
    assert process.is_alive()
    await asyncio.sleep(2.0)
    assert process.is_alive()
    process.kill()
    process.join()
    assert not process.is_alive()

    assert COUNTER1.value == 1  # type: ignore[attr-defined]
    assert COUNTER2.value == 0  # type: ignore[attr-defined]

    # wait until job times out
    await asyncio.sleep(3)

    async with autoconn.magic(auto_disconnect=True):
        w = Worker(routers=[router], messages_limit=1, graceful_shutdown_time=0)

        runner = await asyncio.wait_for(w.run(), 5.0)

        assert runner.processed == 1

    assert COUNTER1.value == 2  # type: ignore[attr-defined]
    assert COUNTER2.value == 1  # type: ignore[attr-defined]
