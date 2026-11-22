from __future__ import annotations

import asyncio
import contextlib

import pytest

from repid._deadlines import _COMPACTION_SLACK, _Deadlines


async def test_timeout_raises_timeout_error_when_expired() -> None:
    deadlines = _Deadlines()

    with pytest.raises(TimeoutError):
        async with deadlines.timeout(0.01):
            await asyncio.sleep(1)

    task = asyncio.current_task()
    assert task is not None
    assert task.cancelling() == 0


async def test_timeout_passes_through_when_finished_in_time() -> None:
    deadlines = _Deadlines()
    timeout = deadlines.timeout(1)

    async with timeout:
        await asyncio.sleep(0)
    timer = timeout._queue._timer

    # The emptied queue keeps its timer for the next run.
    async with deadlines.timeout(1):
        await asyncio.sleep(0)

    assert not timeout._queue._entries
    assert timeout._queue._timer is timer


async def test_runs_with_equal_timeouts_share_one_timer() -> None:
    deadlines = _Deadlines()
    started = 0
    all_started = asyncio.Event()
    timers: set[asyncio.TimerHandle | None] = set()

    async def run(duration: float) -> str:
        nonlocal started
        timeout = deadlines.timeout(0.05)
        try:
            async with timeout:
                started += 1
                if started == 3:
                    all_started.set()
                await all_started.wait()
                timers.add(timeout._queue._timer)
                await asyncio.sleep(duration)
        except TimeoutError:
            return "timeout"
        return "done"

    fast = asyncio.create_task(run(0))
    slow = asyncio.create_task(run(1))
    await asyncio.sleep(0.01)
    # Started later, so its deadline follows the slow run's and needs a reschedule.
    later = asyncio.create_task(run(1))

    assert await asyncio.gather(fast, slow, later) == ["done", "timeout", "timeout"]
    assert len(timers) == 1


async def test_timeout_keeps_external_cancellation() -> None:
    deadlines = _Deadlines()
    task = asyncio.current_task()
    assert task is not None

    async def cancelled_while_expiring() -> None:
        async with deadlines.timeout(0.01):
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                # Someone else cancels the task while the timeout's cancellation unwinds.
                task.cancel()
                await asyncio.sleep(0)

    with pytest.raises(asyncio.CancelledError):
        await cancelled_while_expiring()

    assert task.uncancel() == 0


async def test_timeout_clears_swallowed_cancellation() -> None:
    deadlines = _Deadlines()
    task = asyncio.current_task()
    assert task is not None

    async with deadlines.timeout(0.01):
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(1)

    assert task.cancelling() == 0


async def test_queue_compacts_finished_runs_behind_a_long_run() -> None:
    deadlines = _Deadlines()
    head_entered = asyncio.Event()
    release = asyncio.Event()

    async def long_run() -> None:
        async with deadlines.timeout(10):
            head_entered.set()
            await release.wait()

    head = asyncio.create_task(long_run())
    await head_entered.wait()

    timeout = deadlines.timeout(10)
    for _ in range(_COMPACTION_SLACK * 2):
        async with deadlines.timeout(10):
            pass

    assert len(timeout._queue._entries) <= _COMPACTION_SLACK + 3

    release.set()
    await head
    assert not timeout._queue._entries


async def test_expiry_skips_finished_runs_behind_the_head() -> None:
    deadlines = _Deadlines()
    head_entered = asyncio.Event()

    async def long_run() -> None:
        async with deadlines.timeout(0.02):
            head_entered.set()
            await asyncio.sleep(1)

    head = asyncio.create_task(long_run())
    await head_entered.wait()
    timeout = deadlines.timeout(0.02)
    async with timeout:
        pass

    with pytest.raises(TimeoutError):
        await head
    assert not timeout._queue._entries
    assert timeout._queue._timer is None
