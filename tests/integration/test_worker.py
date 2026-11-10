from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from repid import Channel, Concurrency, Repid, Router
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
async def managed_subscriber(subscriber: SubscriberT) -> AsyncGenerator[Any, None]:
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
    await asyncio.wait_for(
        autoconn.run_worker(messages_limit=30, concurrency=Concurrency(messages=10)),
        timeout=10.0,
    )

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
    queue: asyncio.Queue[Any] = asyncio.Queue()

    async def wrong_callback(msg: Any) -> None:
        await queue.put(msg)

    consumer = await seed_conn.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"another": wrong_callback},
    )
    async with managed_subscriber(consumer):
        with pytest.raises(asyncio.TimeoutError):
            # there should be nothing to consume,
            # since the message was scheduled on 'default' channel
            await asyncio.wait_for(queue.get(), 1.0)

    queue2: asyncio.Queue[Any] = asyncio.Queue()

    async def right_callback(msg: Any) -> None:
        await queue2.put(msg)

    consumer = await seed_conn.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"default": right_callback},
    )
    async with managed_subscriber(consumer):
        rmsg = await asyncio.wait_for(queue2.get(), 10.0)
        assert rmsg.channel == "default"
        await rmsg.ack()


@pytest.mark.parametrize("seed_conn", [0], indirect=True)
@pytest.mark.parametrize(
    "dimension",
    ["messages", "payload_bytes", "oversized_channel", "oversized_worker"],
)
async def test_worker_and_channel_concurrency(autoconn: Repid, dimension: str) -> None:
    limits = {
        "messages": Concurrency(messages=1),
        "payload_bytes": Concurrency(payload_bytes=2),
        "oversized_channel": Concurrency(payload_bytes=1),
        "oversized_worker": Concurrency(payload_bytes=8),
    }[dimension]
    worker_limits = Concurrency(
        messages=3,
        payload_bytes=1 if dimension == "oversized_worker" else 8,
    )
    router = Router(channel=Channel(address="default", concurrency=limits))
    active = 0
    peak = 0

    @router.actor
    async def limited_job() -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.02)
        finally:
            active -= 1

    autoconn.include_router(router)
    for _ in range(6):
        await autoconn.send_message(
            channel="default",
            payload=b"{}",
            headers={"topic": "limited_job"},
        )
    result = await asyncio.wait_for(
        autoconn.run_worker(
            messages_limit=6,
            concurrency=worker_limits,
        ),
        timeout=15,
    )
    assert result.processed == 6
    assert peak == 1
    assert active == 0


@pytest.mark.parametrize("seed_conn", [0], indirect=True)
async def test_channel_waiting_for_bytes_leaves_room_for_another_channel(autoconn: Repid) -> None:
    router = Router(channel=Channel(address="default", concurrency=Concurrency(payload_bytes=2)))
    first_started = asyncio.Event()
    other_started = asyncio.Event()
    release_first = asyncio.Event()
    starts = 0

    @router.actor
    async def limited_job() -> None:
        nonlocal starts
        starts += 1
        if starts == 1:
            first_started.set()
            await release_first.wait()

    @router.actor(channel="another")
    async def other_job() -> None:
        other_started.set()

    autoconn.include_router(router)
    for _ in range(3):
        await autoconn.send_message(
            channel="default",
            payload=b"{}",
            headers={"topic": "limited_job"},
        )

    worker = asyncio.create_task(
        autoconn.run_worker(messages_limit=4, concurrency=Concurrency(messages=2, payload_bytes=4)),
    )
    try:
        async with asyncio.timeout(15):
            await first_started.wait()
            await autoconn.send_message(
                channel="another",
                payload=b"{}",
                headers={"topic": "other_job"},
            )
            await other_started.wait()
            assert starts == 1
            release_first.set()
            result = await worker
    finally:
        release_first.set()
        if not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    assert result.processed == 4
    assert starts == 3
