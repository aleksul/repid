from __future__ import annotations

import asyncio
from typing import Any

import pytest

from repid import Channel, Concurrency, Repid, Router
from repid._concurrency import _Budget


@pytest.mark.parametrize(
    "field",
    [
        pytest.param("messages", id="message_budget"),
        pytest.param("payload_bytes", id="payload_byte_budget"),
    ],
)
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(0, id="zero"),
        pytest.param(-1, id="negative"),
        pytest.param(True, id="true_boolean"),
        pytest.param(False, id="false_boolean"),
        pytest.param(1.5, id="fractional"),
        pytest.param("2", id="numeric_string"),
        pytest.param(float("inf"), id="infinite"),
    ],
)
def test_invalid_budgets(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=field):
        Concurrency(**{field: value})


@pytest.mark.parametrize(
    ("worker", "channel"),
    [
        pytest.param(Concurrency(messages=1), Concurrency(), id="worker_messages"),
        pytest.param(Concurrency(), Concurrency(messages=1), id="channel_messages"),
        pytest.param(Concurrency(payload_bytes=4), Concurrency(), id="worker_payload_bytes"),
        pytest.param(Concurrency(), Concurrency(payload_bytes=4), id="channel_payload_bytes"),
    ],
)
async def test_atomic_intersection(worker: Concurrency, channel: Concurrency) -> None:
    budget = _Budget(worker, {"a": channel})
    assert await budget.acquire("a", 4)
    waiting = asyncio.create_task(budget.acquire("a", 1))
    await asyncio.sleep(0)
    assert not waiting.done()
    assert budget.worker.messages == 1
    if channel.messages is not None or channel.payload_bytes is not None:
        assert budget.channels["a"].payload_bytes == 4
    budget.release("a", 4)
    assert await asyncio.wait_for(waiting, 1)
    budget.release("a", 1)
    assert budget.worker.messages == 0
    assert budget.worker.payload_bytes == 0


async def test_channel_isolation_and_shared_worker() -> None:
    budget = _Budget(Concurrency(messages=2), {"a": Concurrency(messages=1)})
    assert await budget.acquire("a", 1)
    waiting = asyncio.create_task(budget.acquire("a", 1))
    assert await budget.acquire("b", 1)
    await asyncio.sleep(0)
    assert not waiting.done()
    budget.release("a", 1)
    assert await waiting
    budget.release("a", 1)
    budget.release("b", 1)


@pytest.mark.parametrize(
    "scope",
    [
        pytest.param("worker", id="worker_budget"),
        pytest.param("channel", id="channel_budget"),
    ],
)
async def test_oversized_is_exclusive_even_with_empty_payloads(scope: str) -> None:
    limits = Concurrency(payload_bytes=4)
    budget = _Budget(
        limits if scope == "worker" else Concurrency(),
        {"a": limits} if scope == "channel" else {},
    )
    assert await budget.acquire("a", 0)
    oversized = asyncio.create_task(budget.acquire("a", 5))
    await asyncio.sleep(0)
    assert not oversized.done()
    budget.release("a", 0)
    assert await oversized
    waiting = asyncio.create_task(budget.acquire("a", 0))
    await asyncio.sleep(0)
    assert not waiting.done()
    budget.release("a", 5)
    assert await waiting
    budget.release("a", 0)


async def test_close_and_cancel_waiters() -> None:
    budget = _Budget(Concurrency(messages=1))
    assert await budget.acquire("a", 1)
    cancelled = asyncio.create_task(budget.acquire("a", 2))
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    waiting = asyncio.create_task(budget.acquire("b", 2))
    await asyncio.sleep(0)
    budget.close()
    assert not await waiting
    assert not await budget.acquire("b", 0)
    budget.release("a", 1)
    assert budget.worker.payload_bytes == 0


def test_channel_merge_and_schema() -> None:
    app = Repid()
    router = Router()
    limits = Concurrency(messages=2)

    @router.actor(channel=Channel(address="a", concurrency=limits))
    async def first() -> None:
        pass

    titled_channel = Channel(address="a", title="Title")

    @router.actor(channel=titled_channel)
    async def second() -> None:
        pass

    app.include_router(router)
    channel = router.channels[0]
    assert channel.concurrency == limits
    assert channel.title == "Title"
    assert titled_channel.concurrency is None
    assert "concurrency" not in str(app.generate_asyncapi_schema())
    assert channel == Channel(address="a", concurrency=Concurrency(messages=5))

    @router.actor(channel=Channel(address="a", concurrency=Concurrency(messages=3)))
    async def third() -> None:
        pass

    with pytest.raises(ValueError, match="Conflicting concurrency"):
        router._materialize()


@pytest.mark.parametrize(
    "nested",
    [
        pytest.param(False, id="direct_router"),
        pytest.param(True, id="nested_router"),
    ],
)
@pytest.mark.parametrize(
    "reference",
    [
        pytest.param("a", id="address_string"),
        pytest.param(Channel(address="a", title="Title"), id="channel_object"),
    ],
)
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


def test_nested_router_can_reference_an_ancestor_channel_after_changing_defaults() -> None:
    budget = Concurrency(messages=2)
    router = Router(channel=Channel(address="a", concurrency=budget))
    nested = Router(channel="b")
    router.include_router(nested)

    @nested.actor(channel="a")
    async def same_address() -> None:
        pass

    assert [channel.address for channel in router.channels] == ["a"]
    assert router.channels[0].concurrency == budget


async def test_worker_budget_drains_for_oversized_waiter() -> None:
    budget = _Budget(Concurrency(payload_bytes=4))
    assert await budget.acquire("a", 2)
    assert await budget.acquire("a", 2)
    oversized = asyncio.create_task(budget.acquire("a", 5))
    smaller = asyncio.create_task(budget.acquire("a", 1))
    await asyncio.sleep(0)

    budget.release("a", 2)
    await asyncio.sleep(0)
    assert not oversized.done()
    assert not smaller.done()
    assert not budget.try_acquire("b", 1)

    budget.release("a", 2)
    assert await asyncio.wait_for(oversized, 1)
    assert not smaller.done()
    budget.release("a", 5)
    assert await asyncio.wait_for(smaller, 1)
    budget.release("a", 1)


async def test_eligible_waiters_on_one_channel_keep_arrival_order_across_sizes() -> None:
    budget = _Budget(Concurrency(messages=1))
    assert await budget.acquire("a", 1)
    sizes = (1, 2, 1)
    waiters = [asyncio.create_task(budget.acquire("a", size)) for size in sizes]
    await asyncio.sleep(0)

    held = 1
    for waiter, size in zip(waiters, sizes, strict=True):
        budget.release("a", held)
        assert await asyncio.wait_for(waiter, 0.5)
        assert budget.worker.payload_bytes == size
        held = size
    budget.release("a", held)


@pytest.mark.parametrize(
    "cancel_after_grant",
    [
        pytest.param(False, id="cancel_before_grant"),
        pytest.param(True, id="cancel_after_grant"),
    ],
)
async def test_cancel_racing_with_close_releases_reserved_capacity(
    cancel_after_grant: bool,
) -> None:
    budget = _Budget(Concurrency(messages=1))
    assert await budget.acquire("a", 1)
    pending = asyncio.create_task(budget.acquire("a", 1))
    await asyncio.sleep(0)

    if cancel_after_grant:
        budget.release("a", 1)
    pending.cancel()
    budget.close()
    with pytest.raises(asyncio.CancelledError):
        await pending
    if not cancel_after_grant:
        budget.release("a", 1)
    assert budget.worker.messages == 0


async def test_close_releases_capacity_granted_to_a_waiter_before_it_resumes() -> None:
    budget = _Budget(Concurrency(messages=1))
    assert await budget.acquire("a", 1)
    pending = asyncio.create_task(budget.acquire("a", 1))
    await asyncio.sleep(0)
    budget.release("a", 1)
    budget.close()
    assert not await asyncio.wait_for(pending, 1)
    assert budget.worker.messages == 0


async def test_worker_capacity_rotates_between_waiting_channels() -> None:
    budget = _Budget(Concurrency(messages=1))
    assert await budget.acquire("a", 1)
    a_first = asyncio.create_task(budget.acquire("a", 1))
    a_second = asyncio.create_task(budget.acquire("a", 1))
    b_first = asyncio.create_task(budget.acquire("b", 1))
    await asyncio.sleep(0)

    budget.release("a", 1)
    assert await asyncio.wait_for(a_first, 1)
    budget.release("a", 1)
    assert await asyncio.wait_for(b_first, 1)
    assert not a_second.done()
    budget.release("b", 1)
    assert await asyncio.wait_for(a_second, 1)
    budget.release("a", 1)


async def test_release_skips_cancelled_head_and_saturated_channels() -> None:
    budget = _Budget(
        Concurrency(messages=3),
        {"a": Concurrency(messages=1), "b": Concurrency(messages=1)},
    )
    for channel in ("c", "a", "b"):
        assert await budget.acquire(channel, 0)
    cancelled, *waiting = [
        asyncio.create_task(budget.acquire(channel, 0)) for channel in ("c", "a", "b", "d")
    ]
    await asyncio.sleep(0)

    cancelled.cancel()
    budget.release("c", 0)
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    assert await asyncio.wait_for(waiting.pop(), 1)
    assert all(not waiter.done() for waiter in waiting)
    budget.release("d", 0)
    for channel, waiter in zip(("a", "b"), waiting, strict=True):
        budget.release(channel, 0)
        assert await asyncio.wait_for(waiter, 1)
        budget.release(channel, 0)


async def test_cancelling_parked_channel_head_unblocks_next_waiter() -> None:
    budget = _Budget(
        Concurrency(messages=3),
        {"a": Concurrency(payload_bytes=5)},
    )
    assert await budget.acquire("a", 3)

    blocked = asyncio.create_task(budget.acquire("a", 3))
    smaller = asyncio.create_task(budget.acquire("a", 2))
    await asyncio.sleep(0)
    assert not blocked.done()
    assert not smaller.done()

    assert budget.try_acquire("b", 0)
    budget.release("b", 0)
    assert not smaller.done()

    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    assert await asyncio.wait_for(smaller, 1)
    assert budget.channels["a"].payload_bytes == 5
    budget.release("a", 2)
    budget.release("a", 3)


def test_channel_limits_combine_count_and_bytes_independently() -> None:
    budget = _Budget(
        Concurrency(payload_bytes=20 * 1024),
        {
            "images": Concurrency(messages=3, payload_bytes=10 * 1024),
            "emails": Concurrency(messages=8),
        },
    )
    assert {channel: budget.limits(channel) for channel in ("images", "emails", "other")} == {
        "images": Concurrency(messages=3, payload_bytes=10 * 1024),
        "emails": Concurrency(messages=8, payload_bytes=20 * 1024),
        "other": Concurrency(payload_bytes=20 * 1024),
    }
