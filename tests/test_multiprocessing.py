import asyncio
import json
from random import random

import pytest

from repid import Job, Router, Worker

pytestmark = pytest.mark.usefixtures("fake_connection")

router = Router()


@router.actor(run_in_process=True)
def job1() -> float:
    return random()


@router.actor(run_in_process=True)
def job2(arg1: float, arg2: float) -> float:
    return arg1 + arg2


async def test_sync_process_job() -> None:
    j = Job("job1")
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[router], messages_limit=1)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    r = await j.result
    assert r is not None
    assert r.success


async def test_sync_process_with_args_job() -> None:
    arg1 = random()
    arg2 = random()

    j = Job("job2", args=dict(arg1=arg1, arg2=arg2))
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[router], messages_limit=1)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    r = await j.result
    assert r is not None
    assert r.success
    assert json.loads(r.data) == arg1 + arg2
