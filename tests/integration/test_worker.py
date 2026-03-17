from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from repid import Repid, Router
from repid.connections.abc import ServerT, SubscriberT


@pytest.fixture(autouse=True)
async def seed_conn(
    autoconn: Repid,
    request: pytest.FixtureRequest,
) -> AsyncIterator[ServerT]:
    async with autoconn.servers.default.connection() as conn:
        amount_of_jobs = getattr(request, "param", 1)

        if amount_of_jobs > 0:
            await asyncio.gather(
                *[
                    autoconn.send_message_json(
                        channel="default",
                        payload={},
                        headers={"topic": "awesome_job"},
                    )
                    for _ in range(amount_of_jobs)
                ],
            )

        yield conn


@asynccontextmanager
async def managed_subscriber(subscriber: SubscriberT) -> AsyncGenerator:
    try:
        yield subscriber
    finally:
        await subscriber.close()


@pytest.mark.parametrize("seed_conn", [30], indirect=True)
async def test_more_concurrent_tasks_than_limit(autoconn: Repid) -> None:
    hit = 0

    router = Router()

    @router.actor
    async def awesome_job() -> None:
        nonlocal hit
        await asyncio.sleep(1.0)
        hit += 1

    autoconn.include_router(router)

    # tests capability of the worker to properly handle concurrency limits -
    # timeout is set to 10 seconds, there are 30 tasks that each take 1 second to complete,
    # but concurrency limit is set to 10, so the whole run should take approximately 3 seconds
    await asyncio.wait_for(autoconn.run_worker(messages_limit=30, tasks_limit=10), timeout=10.0)

    assert hit == 30


@pytest.mark.usefixtures("seed_conn")
async def test_forced_worker_stop(autoconn: Repid) -> None:
    router = Router()

    actor_started_event = asyncio.Event()
    actor_allowed_to_continue = asyncio.Event()
    actor_finished_event = asyncio.Event()

    @router.actor
    async def awesome_job() -> None:
        actor_started_event.set()
        await actor_allowed_to_continue.wait()
        actor_finished_event.set()

    autoconn.include_router(router)

    async with autoconn.servers.default.connection():
        worker_task = asyncio.create_task(
            autoconn.run_worker(
                graceful_shutdown_time=0,
                register_signals=[signal.SIGUSR1],
            ),
        )

        await asyncio.wait_for(actor_started_event.wait(), timeout=5.0)

        # terminate the worker immediately, and thus actor cannot finish
        signal.raise_signal(signal.SIGUSR1)

        await worker_task

    assert actor_started_event.is_set()
    assert not actor_finished_event.is_set()

    actor_allowed_to_continue.set()
    actor_started_event.clear()
    actor_finished_event.clear()

    # tests that worker didn't lose the message when shutting down
    async with autoconn.servers.default.connection():
        runner = await autoconn.run_worker(
            messages_limit=1,
            graceful_shutdown_time=0,
        )
        assert runner.processed == 1

    assert actor_started_event.is_set()
    assert actor_finished_event.is_set()


async def test_another_channel_is_not_consumed(seed_conn: ServerT) -> None:
    queue: asyncio.Queue = asyncio.Queue()

    async def wrong_callback(msg: Any) -> None:
        await queue.put(msg)

    consumer = await seed_conn.subscribe(channels_to_callbacks={"another": wrong_callback})
    async with managed_subscriber(consumer):
        with pytest.raises(asyncio.TimeoutError):
            # there should be nothing to consume,
            # since the message was scheduled on 'default' channel
            await asyncio.wait_for(queue.get(), 1.0)

    queue2: asyncio.Queue = asyncio.Queue()

    async def right_callback(msg: Any) -> None:
        await queue2.put(msg)

    consumer = await seed_conn.subscribe(channels_to_callbacks={"default": right_callback})
    async with managed_subscriber(consumer):
        rmsg = await asyncio.wait_for(queue2.get(), 10.0)
        assert rmsg.channel == "default"
        await rmsg.ack()
