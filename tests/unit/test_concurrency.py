from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from repid import Channel, Concurrency, Repid, Router
from repid._concurrency import (
    _channel_hints,
    _ConcurrencyController,
)


@pytest.mark.parametrize("field", ["messages", "payload_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "2", float("inf")])
def test_invalid_budgets(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=field):
        Concurrency(**{field: value})


def test_concurrency_is_immutable() -> None:
    with pytest.raises(FrozenInstanceError):
        Concurrency().messages = 1  # pyrefly: ignore[read-only]


@pytest.mark.parametrize(
    ("worker", "channel"),
    [
        (Concurrency(messages=1), Concurrency()),
        (Concurrency(), Concurrency(messages=1)),
        (Concurrency(payload_bytes=4), Concurrency()),
        (Concurrency(), Concurrency(payload_bytes=4)),
    ],
)
async def test_atomic_intersection(worker: Concurrency, channel: Concurrency) -> None:
    controller = _ConcurrencyController(worker, {"a": channel})
    first = await controller.acquire("a", 4)
    assert first is not None
    waiting = asyncio.create_task(controller.acquire("a", 1))
    await asyncio.sleep(0)
    assert not waiting.done()
    assert controller.worker.messages == 1
    if channel.messages is not None or channel.payload_bytes is not None:
        assert controller.channels["a"].payload_bytes == 4
    first.release()
    first.release()
    second = await asyncio.wait_for(waiting, 1)
    assert second is not None
    second.release()
    assert controller.worker.messages == 0
    assert controller.worker.payload_bytes == 0


async def test_channel_isolation_and_shared_worker() -> None:
    controller = _ConcurrencyController(Concurrency(messages=2), {"a": Concurrency(messages=1)})
    first = await controller.acquire("a", 1)
    waiting = asyncio.create_task(controller.acquire("a", 1))
    other = await controller.acquire("b", 1)
    assert first is not None
    assert other is not None
    await asyncio.sleep(0)
    assert not waiting.done()
    first.release()
    admitted = await waiting
    assert admitted is not None
    admitted.release()
    other.release()


@pytest.mark.parametrize("scope", ["worker", "channel"])
async def test_oversized_is_exclusive_even_with_empty_payloads(scope: str) -> None:
    limits = Concurrency(payload_bytes=4)
    controller = _ConcurrencyController(
        limits if scope == "worker" else Concurrency(),
        {"a": limits} if scope == "channel" else {},
    )
    empty = await controller.acquire("a", 0)
    assert empty is not None
    oversized = asyncio.create_task(controller.acquire("a", 5))
    await asyncio.sleep(0)
    assert not oversized.done()
    empty.release()
    big = await oversized
    assert big is not None
    waiting = asyncio.create_task(controller.acquire("a", 0))
    await asyncio.sleep(0)
    assert not waiting.done()
    big.release()
    next_message = await waiting
    assert next_message is not None
    next_message.release()


async def test_close_and_cancel_waiters() -> None:
    controller = _ConcurrencyController(Concurrency(messages=1))
    first = await controller.acquire("a", 1)
    assert first is not None
    cancelled = asyncio.create_task(controller.acquire("a", 2))
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    waiting = asyncio.create_task(controller.acquire("b", 2))
    await asyncio.sleep(0)
    controller.close()
    assert await waiting is None
    assert await controller.acquire("b", 0) is None
    first.release()
    assert controller.worker.payload_bytes == 0


def test_channel_merge_and_schema() -> None:
    app = Repid()
    router = Router()
    limits = Concurrency(messages=2)

    @router.actor(channel=Channel(address="a", concurrency=limits))
    async def first() -> None:
        pass

    @router.actor(channel=Channel(address="a", title="Title"))
    async def second() -> None:
        pass

    app.include_router(router)
    channel = router.channels[0]
    assert channel.concurrency == limits
    assert channel.title == "Title"
    assert "concurrency" not in str(app.generate_asyncapi_schema())
    assert channel == Channel(address="a", concurrency=Concurrency(messages=5))

    @router.actor(channel=Channel(address="a", concurrency=Concurrency(messages=3)))
    async def third() -> None:
        pass

    with pytest.raises(ValueError, match="Conflicting concurrency"):
        router._materialize()


async def test_worker_budget_drains_for_oversized_waiter() -> None:
    controller = _ConcurrencyController(Concurrency(payload_bytes=4))
    first = await controller.acquire("a", 2)
    second = await controller.acquire("a", 2)
    assert first is not None
    assert second is not None
    oversized = asyncio.create_task(controller.acquire("a", 5))
    smaller = asyncio.create_task(controller.acquire("a", 1))
    await asyncio.sleep(0)

    first.release()
    await asyncio.sleep(0)
    assert not oversized.done()
    assert not smaller.done()
    assert controller.try_acquire("b", 1) is None

    second.release()
    big = await asyncio.wait_for(oversized, 1)
    assert big is not None
    assert not smaller.done()
    big.release()
    admitted = await asyncio.wait_for(smaller, 1)
    assert admitted is not None
    admitted.release()


async def test_eligible_waiters_on_one_channel_keep_arrival_order_across_sizes() -> None:
    controller = _ConcurrencyController(Concurrency(messages=1))
    reservation = await controller.acquire("a", 1)
    assert reservation is not None
    waiters = [asyncio.create_task(controller.acquire("a", size)) for size in (1, 2, 1)]
    await asyncio.sleep(0)

    for waiter in waiters:
        reservation.release()
        reservation = await asyncio.wait_for(waiter, 0.5)
        assert reservation is not None
    reservation.release()


@pytest.mark.parametrize("cancel_after_grant", [False, True])
async def test_cancel_racing_with_close_releases_reserved_capacity(
    cancel_after_grant: bool,
) -> None:
    controller = _ConcurrencyController(Concurrency(messages=1))
    first = await controller.acquire("a", 1)
    assert first is not None
    pending = asyncio.create_task(controller.acquire("a", 1))
    await asyncio.sleep(0)

    if cancel_after_grant:
        first.release()
    pending.cancel()
    controller.close()
    with pytest.raises(asyncio.CancelledError):
        await pending
    first.release()
    assert controller.worker.messages == 0


async def test_close_releases_capacity_granted_to_a_waiter_before_it_resumes() -> None:
    controller = _ConcurrencyController(Concurrency(messages=1))
    first = await controller.acquire("a", 1)
    assert first is not None
    pending = asyncio.create_task(controller.acquire("a", 1))
    await asyncio.sleep(0)
    first.release()
    controller.close()
    assert await asyncio.wait_for(pending, 1) is None
    assert controller.worker.messages == 0


async def test_worker_capacity_rotates_between_waiting_channels() -> None:
    controller = _ConcurrencyController(Concurrency(messages=1))
    first = await controller.acquire("a", 1)
    assert first is not None
    a_first = asyncio.create_task(controller.acquire("a", 1))
    a_second = asyncio.create_task(controller.acquire("a", 1))
    b_first = asyncio.create_task(controller.acquire("b", 1))
    await asyncio.sleep(0)

    first.release()
    granted = await asyncio.wait_for(a_first, 1)
    assert granted is not None
    granted.release()
    granted = await asyncio.wait_for(b_first, 1)
    assert granted is not None
    assert not a_second.done()
    granted.release()
    granted = await asyncio.wait_for(a_second, 1)
    assert granted is not None
    granted.release()


async def test_release_skips_cancelled_head_and_saturated_channels() -> None:
    controller = _ConcurrencyController(
        Concurrency(messages=3),
        {"a": Concurrency(messages=1), "b": Concurrency(messages=1)},
    )
    active = [await controller.acquire(channel, 0) for channel in ("c", "a", "b")]
    cancelled, *waiting = [
        asyncio.create_task(controller.acquire(channel, 0)) for channel in ("c", "a", "b", "d")
    ]
    await asyncio.sleep(0)

    cancelled.cancel()
    assert active[0] is not None
    active[0].release()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    other = await asyncio.wait_for(waiting.pop(), 1)
    assert other is not None
    assert all(not waiter.done() for waiter in waiting)
    other.release()
    for reservation, waiter in zip(active[1:], waiting, strict=True):
        assert reservation is not None
        reservation.release()
        next_reservation = await asyncio.wait_for(waiter, 1)
        assert next_reservation is not None
        next_reservation.release()


async def test_cancelling_parked_channel_head_unblocks_next_waiter() -> None:
    controller = _ConcurrencyController(
        Concurrency(messages=3),
        {"a": Concurrency(payload_bytes=5)},
    )
    active = await controller.acquire("a", 3)
    assert active is not None

    blocked = asyncio.create_task(controller.acquire("a", 3))
    smaller = asyncio.create_task(controller.acquire("a", 2))
    await asyncio.sleep(0)
    assert not blocked.done()
    assert not smaller.done()

    unrelated = controller.try_acquire("b", 0)
    assert unrelated is not None
    unrelated.release()
    assert not smaller.done()

    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    admitted = await asyncio.wait_for(smaller, 1)
    assert admitted is not None
    assert controller.channels["a"].payload_bytes == 5
    admitted.release()
    active.release()


def test_intake_hints_combine_count_and_bytes_independently() -> None:
    assert _channel_hints(
        ("images", "emails", "other"),
        Concurrency(payload_bytes=20 * 1024),
        {
            "images": Concurrency(messages=3, payload_bytes=10 * 1024),
            "emails": Concurrency(messages=8),
        },
    ) == {
        "images": Concurrency(messages=3, payload_bytes=10 * 1024),
        "emails": Concurrency(messages=8, payload_bytes=20 * 1024),
        "other": Concurrency(payload_bytes=20 * 1024),
    }


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("reference", ["a", Channel(address="a", title="Title")])
def test_router_default_budget_applies_to_same_address(
    nested: bool,
    reference: str | Channel,
) -> None:
    budget = Concurrency(messages=2)
    router = Router(channel=Channel(address="a", concurrency=budget))
    if nested:
        actor_router = Router(channel=reference)
        router.include_router(actor_router)
    else:
        actor_router = router

    @actor_router.actor(channel=None if nested else reference)
    async def same_address() -> None:
        pass

    @actor_router.actor(channel="b")
    async def other_address() -> None:
        pass

    assert [channel.concurrency for channel in router.channels] == [budget, None]


def test_same_address_cannot_override_router_default_budget() -> None:
    router = Router(channel=Channel(address="a", concurrency=Concurrency(messages=2)))

    @router.actor(channel=Channel(address="a", concurrency=Concurrency(messages=1)))
    async def conflicting_address() -> None:
        pass

    with pytest.raises(ValueError, match="Conflicting concurrency for channel 'a'"):
        router._materialize()
