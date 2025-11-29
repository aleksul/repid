from __future__ import annotations

import asyncio
from random import random

from repid import Repid, Router


async def test_simple_job(autoconn: Repid) -> None:
    router = Router()

    hit = False

    @router.actor
    async def awesome_job() -> None:
        nonlocal hit
        hit = True

    autoconn.include_router(router)

    async with autoconn.servers.default.connection():
        await autoconn.send_message(
            channel="default",
            payload=b"",
            headers={"topic": "awesome_job"},
        )
        await asyncio.wait_for(autoconn.run_worker(messages_limit=1, tasks_limit=1), timeout=5.0)

    assert hit


async def test_args_job(autoconn: Repid) -> None:
    assertion1 = random()
    assertion2 = random()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job(my_arg1: float, my_arg2: float) -> None:
        nonlocal hit, assertion1, assertion2
        assert my_arg1 == assertion1
        assert my_arg2 == assertion2
        hit = True

    autoconn.include_router(router)

    async with autoconn.servers.default.connection():
        await autoconn.send_message_json(
            channel="default",
            payload={"my_arg1": assertion1, "my_arg2": assertion2},
            headers={"topic": "awesome_job"},
        )
        await asyncio.wait_for(autoconn.run_worker(messages_limit=1, tasks_limit=1), timeout=5.0)

    assert hit
