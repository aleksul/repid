from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager, nullcontext
from types import CoroutineType
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from repid import Channel, Concurrency, LeaseRenewal, Repid, Router
from repid._concurrency import _Budget
from repid._delivery import _Delivery
from repid._runner import _Runner
from repid.connections.abc import (
    ChannelIntake,
    Intake,
    ReceivedMessageT,
    ServerT,
    SubscriberT,
)
from repid.connections.in_memory import InMemoryServer
from repid.data import ActorData, ActorExecutionContext, MessageData
from repid.data.actor import _ActorCall
from tests.intake import handle_message


def _lease(delivery: _Delivery) -> LeaseRenewal:
    lease = delivery._renewal
    assert lease is not None
    return lease


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


async def test_delivery_releases_its_budget_once() -> None:
    budget = _Budget(Concurrency(messages=1))
    delivery = _Delivery(make_message(), budget)
    assert delivery.try_acquire()
    delivery.release()
    delivery.release()
    assert budget.worker.messages == 0


async def test_lease_renewal_handoff_is_scoped_to_its_delivery() -> None:
    message = make_message()
    renewal = LeaseRenewal(message)
    closed = LeaseRenewal(message)
    await closed.close()
    budget = _Budget(Concurrency())

    with closed.handoff():
        await _Delivery(message, budget).finish()
    with renewal.handoff():
        await _Delivery(make_message(), budget).finish()
    await _Delivery(message, budget).finish()
    # None of the deliveries above adopted the renewal, so it is still open.
    renewal.renew_at(0.001)
    await asyncio.wait_for(_wait_called(message.keep_alive), 1)

    with renewal.handoff():
        adopted = _Delivery(message, budget)
        # Only the first delivery adopts an offered renewal.
        await _Delivery(message, budget).finish()
    await renewal.close_if_unclaimed()
    renewed = message.keep_alive.await_count
    await asyncio.wait_for(_wait_called(message.keep_alive, renewed + 1), 1)
    await adopted.finish()
    stopped = message.keep_alive.await_count
    await asyncio.sleep(0.01)
    assert message.keep_alive.await_count == stopped


async def test_adopted_renewal_keeps_its_pending_deadline() -> None:
    loop = asyncio.get_running_loop()
    message = make_message()
    renewed_at: list[float] = []
    message.keep_alive.side_effect = lambda: renewed_at.append(loop.time())
    renewal = LeaseRenewal(message)
    renewal.renew_at(0.05)
    offered_at = loop.time()

    with renewal.handoff():
        delivery = _Delivery(message, _Budget(Concurrency()))
    timer = renewal._timer
    assert timer is not None
    # A longer actor interval must not push back the renewal already due,
    # and keeping the deadline keeps its timer rather than rescheduling it.
    delivery.renew_at(3600)
    assert renewal._timer is timer
    assert not timer.cancelled()
    try:
        await asyncio.wait_for(_wait_called(message.keep_alive), 1)
    finally:
        await delivery.finish()
        await renewal.close_if_unclaimed()
    assert renewed_at[0] - offered_at < 1


async def _wait_called(mock: AsyncMock, count: int = 1) -> None:
    while mock.await_count < count:
        await asyncio.sleep(0.001)


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


def make_runner(
    fn: Callable[[], Awaitable[object]],
    *,
    limits: Concurrency | None = None,
    max_tasks: int = 100,
    server: ServerT | None = None,
) -> tuple[_Runner, Mock]:
    context = ActorExecutionContext(
        server=server if server is not None else InMemoryServer(),
        publish=AsyncMock(),
        default_serializer=Mock(),
    )

    async def middleware(
        call_next: _ActorCall,
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> object:
        return await call_next(message, actor)

    actor = Mock(
        spec=ActorData,
        timeout=None,
        keep_alive=False,
        confirmation_mode="manual",
        on_error="nack",
        routing_strategy=Mock(return_value=True),
        converter=Mock(convert_inputs=AsyncMock(return_value=([], {}))),
        fn=fn,
        middleware_pipeline=middleware,
    )
    actor.name = "job"
    return _Runner(
        actor_context=context,
        concurrency=limits or Concurrency(messages=1),
        max_tasks=max_tasks,
    ), actor


def make_message(payload: bytes = b"abc") -> Mock:
    return Mock(
        spec=ReceivedMessageT,
        payload=payload,
        channel="a",
        is_acted_on=False,
        keep_alive_interval=None,
        reject=AsyncMock(),
        ack=AsyncMock(),
        nack=AsyncMock(),
        keep_alive=AsyncMock(),
    )


async def _admitted(runner: _Runner, message: ReceivedMessageT) -> None:
    """Wait until the runner starts processing a delivery or is done with it."""
    while True:
        delivery = next((d for d in runner._tasks if d.message is message), None)
        if delivery is None or delivery.started:
            return
        await asyncio.sleep(0)


def admitting_submit(
    runner: _Runner,
    channel: ChannelIntake,
) -> Callable[[ReceivedMessageT], Coroutine[object, None, None]]:
    """Submit a delivery and wait until it is admitted, as an adapter would observe it."""

    async def submit(message: ReceivedMessageT) -> None:
        channel.submit(message)
        await _admitted(runner, message)

    return submit


@asynccontextmanager
async def run_messages(
    runner: _Runner,
    actor: ActorData,
    *,
    grace: float = 0.1,
    cancellation_timeout: float = 1.0,
) -> AsyncGenerator[
    tuple[Callable[[ReceivedMessageT], Coroutine[object, None, None]], asyncio.Task[None]],
    None,
]:
    subscribed = asyncio.Event()
    subscriber = Mock(
        task=asyncio.get_running_loop().create_future(),
        pause=AsyncMock(),
        close=AsyncMock(),
    )

    async def subscribe_to_messages(**_: object) -> Mock:
        subscribed.set()
        return subscriber

    with patch.object(runner.server, "subscribe", side_effect=subscribe_to_messages) as subscribe:
        running = asyncio.create_task(runner.run({"a": [actor]}, grace, cancellation_timeout))
        try:
            await asyncio.wait_for(subscribed.wait(), 1)
            intake: Intake = subscribe.call_args.kwargs["intake"]
            yield admitting_submit(runner, intake.channels["a"]), running
        finally:
            runner.stop_consume_event.set()
            await asyncio.wait_for(running, 1)
            subscriber.task.cancel()


@pytest.mark.parametrize(
    "confirmation",
    [
        pytest.param("manual", id="manual_confirmation"),
        pytest.param("manual_explicit", id="explicit_manual_confirmation"),
        pytest.param("ack_first", id="ack_before_execution"),
    ],
)
async def test_reservation_covers_execution_and_manual_completion(confirmation: str) -> None:
    release = asyncio.Event()
    started = asyncio.Event()

    async def fn() -> str:
        started.set()
        await release.wait()
        return "no_action"

    runner, actor = make_runner(fn, max_tasks=2)
    actor.confirmation_mode = confirmation
    async with run_messages(runner, actor) as (submit, running):
        await submit(make_message())
        await asyncio.wait_for(started.wait(), 1)
        waiting = asyncio.create_task(submit(make_message()))
        await asyncio.sleep(0)
        assert not waiting.done()
        release.set()
        await asyncio.wait_for(waiting, 1)
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert runner.processed == 2


async def test_slow_settlement_holds_capacity_and_failure_releases() -> None:
    runner, actor = make_runner(AsyncMock(), max_tasks=2)
    actor.confirmation_mode = "auto"
    message = make_message()
    settling = asyncio.Event()
    release = asyncio.Event()

    async def ack() -> None:
        settling.set()
        await release.wait()
        raise ConnectionError("settlement")

    message.ack.side_effect = ack
    second = make_message()
    async with run_messages(runner, actor) as (submit, running):
        await submit(message)
        await asyncio.wait_for(settling.wait(), 1)
        waiting = asyncio.create_task(submit(second))
        await asyncio.sleep(0)
        assert not waiting.done()
        release.set()
        await asyncio.wait_for(waiting, 1)
        await asyncio.wait_for(asyncio.shield(running), 1)
        second.ack.assert_awaited_once()
        assert runner.processed == 2


def _cancel_on_create(
    *qualnames: str,
) -> Callable[[Coroutine[object, None, None]], asyncio.Task[None]]:
    """Patch target for `asyncio.create_task` that cancels the named coroutines' tasks."""
    create_task = asyncio.create_task

    def create(coroutine: Coroutine[object, None, None]) -> asyncio.Task[None]:
        task = create_task(coroutine)
        if isinstance(coroutine, CoroutineType) and coroutine.__qualname__ in qualnames:
            task.cancel()
        return task

    return create


def _cancel_before_start(runner: _Runner, message: ReceivedMessageT) -> None:
    """Submit a delivery and cancel its task as shutdown does, before the task runs."""
    runner._intake.channels["a"].submit(message)
    delivery, task = next((d, t) for d, t in runner._tasks.items() if d.message is message)
    runner._cancel(delivery, task)


async def test_cancellation_before_start_rejects_without_consuming_quota() -> None:
    runner, actor = make_runner(AsyncMock(), max_tasks=1)
    message = make_message()
    rejecting = asyncio.Event()
    release = asyncio.Event()

    async def reject() -> None:
        rejecting.set()
        await release.wait()

    message.reject.side_effect = reject
    async with run_messages(runner, actor) as (_, running):
        _cancel_before_start(runner, message)

        await asyncio.wait_for(rejecting.wait(), 1)
        assert not runner.max_tasks_hit
        # Shutdown waits for the rejection issued on the cancelled task's behalf.
        runner.stop_consume_event.set()
        done, _ = await asyncio.wait({running}, timeout=0.05)
        assert not done
        release.set()
        await asyncio.wait_for(asyncio.shield(running), 1)
        actor.fn.assert_not_awaited()
        message.reject.assert_awaited_once()
        assert runner.processed == 0


async def test_prestart_rejection_cancelled_before_running_stops_offered_renewal() -> None:
    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, actor = make_runner(AsyncMock(), max_tasks=1, server=server)
    message = make_message()
    renewal = LeaseRenewal(message)
    renewal.renew_at(3600)

    async with run_messages(runner, actor) as (submit, running):
        intake = runner._intake.channels["a"]
        cancel = _cancel_on_create("_Runner._reject_and_release")
        with patch.object(asyncio, "create_task", side_effect=cancel):
            with renewal.handoff():
                _cancel_before_start(runner, message)
            # Stay patched while the runner schedules the rejection on the task's behalf.
            await asyncio.wait_for(_admitted(runner, message), 1)
            while runner._cleanup_tasks:
                await asyncio.sleep(0)
        assert renewal._closed
        assert renewal._timer is None
        assert runner._budget.worker.messages == 0
        assert intake.demand.messages == 1
        message.reject.assert_not_awaited()
        await asyncio.wait_for(submit(make_message()), 1)
        await asyncio.wait_for(asyncio.shield(running), 1)


async def test_task_creation_failure_leaves_message_with_subscriber() -> None:
    runner, actor = make_runner(AsyncMock(), max_tasks=1)
    message = make_message()
    renewal = LeaseRenewal(message)
    renewal.renew_at(3600)
    async with run_messages(runner, actor) as (submit, running):
        intake = runner._intake.channels["a"]
        with (
            patch.object(asyncio, "create_task", side_effect=RuntimeError("cannot schedule")),
            pytest.raises(RuntimeError, match="cannot schedule"),
            renewal.handoff(),
        ):
            intake.submit(message)
        assert renewal._closed
        assert intake.demand.messages == 1
        message.reject.assert_not_awaited()
        assert not runner.max_tasks_hit
        await asyncio.wait_for(submit(make_message()), 1)
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert runner.processed == 1


async def test_waiter_cancellation_rejects_without_consuming_quota() -> None:
    release = asyncio.Event()
    runner, actor = make_runner(release.wait, max_tasks=2)
    async with run_messages(runner, actor) as (submit, running):
        await submit(make_message())
        waiter_message = make_message()
        waiting = asyncio.create_task(submit(waiter_message))
        task, delivery = None, None
        while delivery is None or not delivery.entered:
            await asyncio.sleep(0)
            task, delivery = next(
                ((t, d) for d, t in runner._tasks.items() if d.message is waiter_message),
                (None, None),
            )
        assert task is not None
        assert not waiting.done()
        # The delivery is waiting at admission when its task is cancelled.
        task.cancel()
        await asyncio.wait_for(waiting, 1)
        waiter_message.reject.assert_awaited_once()
        assert not runner.max_tasks_hit
        release.set()
        await asyncio.wait_for(submit(make_message()), 1)
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert runner.processed == 2


async def test_routing_failure_rejects_without_renewal_and_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner, actor = make_runner(AsyncMock())
    actor.routing_strategy.side_effect = RuntimeError("routing failed")
    message = make_message()
    async with run_messages(runner, actor) as (submit, running):
        await submit(message)
        message.reject.assert_awaited_once()
        assert "runner.processing.error" in caplog.messages
        assert not runner.max_tasks_hit
        actor.fn.assert_not_awaited()
        assert not running.done()


async def test_processing_failure_stops_the_runner(caplog: pytest.LogCaptureFixture) -> None:
    # A server without capabilities fails processing outside the actor pipeline.
    server = Mock(spec=ServerT, capabilities={})
    runner, actor = make_runner(AsyncMock(), server=server)
    actor.keep_alive = True
    async with run_messages(runner, actor) as (submit, running):
        await submit(make_message())
        await asyncio.wait_for(asyncio.shield(running), 1)
    assert runner.stop_consume_event.is_set()
    assert runner.processed == 1
    assert "runner.processing.error" in caplog.messages


async def test_unclaimed_renewal_closes_itself() -> None:
    message = make_message()
    renewal = LeaseRenewal(message)
    renewal.renew_at(3600)
    await renewal.close_if_unclaimed()
    assert renewal._closed
    assert renewal._timer is None


async def test_failed_early_ack_skips_actor_and_keeps_worker_running(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner, actor = make_runner(AsyncMock())
    actor.confirmation_mode = "ack_first"
    message = make_message()
    ack_started = asyncio.Event()
    fail_ack = asyncio.Event()

    async def ack() -> None:
        ack_started.set()
        await fail_ack.wait()
        raise ConnectionError("early ack failed")

    message.ack.side_effect = ack
    async with run_messages(runner, actor) as (submit, running):
        await submit(message)
        await asyncio.wait_for(ack_started.wait(), 1)
        extra = make_message()
        waiting = asyncio.create_task(submit(extra))
        await asyncio.sleep(0)
        assert not waiting.done()
        fail_ack.set()
        await asyncio.wait_for(waiting, 1)

        async def both_processed() -> None:
            while runner.processed < 2:
                await asyncio.sleep(0)

        await asyncio.wait_for(both_processed(), 1)
        # The failed ack is the delivery's problem: the slot frees for the next one.
        assert not running.done()
        assert not runner.stop_consume_event.is_set()
        assert "actor.ack_first.error" in caplog.messages
        assert "runner.processing.error" not in caplog.messages
        actor.fn.assert_awaited_once()
        extra.ack.assert_awaited_once()
        extra.reject.assert_not_awaited()


@pytest.mark.parametrize(
    "actor_keep_alive",
    [
        pytest.param(False, id="renewal_disabled"),
        pytest.param(3600.0, id="long_actor_renewal_interval"),
    ],
)
async def test_waiter_uses_broker_interval_until_refused_during_shutdown(
    actor_keep_alive: bool | float,
) -> None:
    release = asyncio.Event()
    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, actor = make_runner(release.wait, server=server)
    actor.keep_alive = actor_keep_alive
    message = make_message()
    message.keep_alive_interval = 0.001
    keep_alive_called = asyncio.Event()
    rejecting = asyncio.Event()
    renewed_during_reject = asyncio.Event()
    leases: list[LeaseRenewal] = []
    lease_init = LeaseRenewal.__init__

    def track_lease(lease: LeaseRenewal, renewed: ReceivedMessageT) -> None:
        lease_init(lease, renewed)
        if renewed is message:
            leases.append(lease)

    def keep_alive() -> None:
        keep_alive_called.set()
        if rejecting.is_set():
            renewed_during_reject.set()

    async def reject() -> None:
        rejecting.set()
        await asyncio.wait_for(renewed_during_reject.wait(), 0.5)

    message.keep_alive.side_effect = keep_alive
    message.reject.side_effect = reject

    with patch.object(LeaseRenewal, "__init__", track_lease):
        async with run_messages(runner, actor) as (submit, running):
            await submit(make_message())
            waiting = asyncio.create_task(submit(message))
            await asyncio.wait_for(keep_alive_called.wait(), 1)
            runner.stop_consume_event.set()
            await asyncio.wait_for(waiting, 1)
            message.reject.assert_awaited_once()
            release.set()
            await asyncio.wait_for(asyncio.shield(running), 1)
            assert runner.processed == 1
    # One renewal schedule covers waiting and the shutdown rejection.
    (lease,) = leases
    assert lease._closed
    assert lease._timer is None
    assert lease._task is None or lease._task.done()


async def test_actor_renews_during_shutdown_rejection() -> None:
    actor_started = asyncio.Event()
    rejecting = asyncio.Event()
    renewed_during_reject = asyncio.Event()
    rejected = asyncio.Event()

    async def fn() -> None:
        actor_started.set()
        await asyncio.Future()

    def keep_alive() -> None:
        if rejecting.is_set():
            renewed_during_reject.set()

    async def reject() -> None:
        rejecting.set()
        await asyncio.wait_for(renewed_during_reject.wait(), 0.5)
        rejected.set()

    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, actor = make_runner(fn, server=server)
    actor.keep_alive = True
    message = make_message()
    message.keep_alive_interval = 0.01
    message.keep_alive.side_effect = keep_alive
    message.reject.side_effect = reject

    async with run_messages(runner, actor, grace=0.0) as (submit, running):
        await submit(message)
        await asyncio.wait_for(actor_started.wait(), 1)
        runner.stop_consume_event.set()
        await asyncio.wait_for(asyncio.shield(running), 1)

    assert rejected.is_set()


async def test_shutdown_joins_rejection_started_during_renewal_cleanup() -> None:
    renewal_started = asyncio.Event()
    renewal_closing = asyncio.Event()
    rejecting = asyncio.Event()
    allow_reject = asyncio.Event()

    async def fn() -> None:
        await renewal_started.wait()

    async def keep_alive() -> None:
        renewal_started.set()
        try:
            await asyncio.Future()
        finally:
            renewal_closing.set()
            await asyncio.Future()

    async def reject() -> None:
        rejecting.set()
        await allow_reject.wait()

    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, actor = make_runner(fn, server=server)
    actor.keep_alive = True
    message = make_message()
    message.keep_alive_interval = 0.001
    message.keep_alive.side_effect = keep_alive
    message.reject.side_effect = reject

    async with run_messages(runner, actor, grace=0.0) as (submit, running):
        try:
            await submit(message)
            await asyncio.wait_for(renewal_closing.wait(), 1)
            runner.stop_consume_event.set()
            await asyncio.wait_for(rejecting.wait(), 1)
            done, _ = await asyncio.wait({running}, timeout=0.02)
            assert not done
        finally:
            allow_reject.set()
        await asyncio.wait_for(asyncio.shield(running), 1)


async def test_unrouted_settlement_renews_lease_until_it_finishes() -> None:
    renewal_called = asyncio.Event()
    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, _ = make_runner(AsyncMock(), server=server)
    message = make_message()
    message.keep_alive_interval = 0.001
    message.keep_alive.side_effect = renewal_called.set
    message.reject.side_effect = renewal_called.wait

    await asyncio.wait_for(handle_message(runner, [], message), 1)
    message.reject.assert_awaited_once()
    message.keep_alive.assert_awaited_once()


async def test_shutdown_timeout_stops_renewal_and_cancels_stuck_rejection() -> None:
    actor_started = asyncio.Event()
    release_actor = asyncio.Event()
    rejection_cancelled = asyncio.Event()

    async def fn() -> None:
        actor_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await release_actor.wait()

    async def reject() -> None:
        try:
            await asyncio.Future()
        finally:
            rejection_cancelled.set()

    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, actor = make_runner(fn, server=server)
    actor.keep_alive = True
    message = make_message()
    message.keep_alive_interval = 0.001

    message.reject.side_effect = reject

    async with run_messages(runner, actor, grace=0, cancellation_timeout=0.02) as (
        submit,
        running,
    ):
        await submit(message)
        await asyncio.wait_for(actor_started.wait(), 1)
        delivery, processing = next(iter(runner._tasks.items()))
        renewal = _lease(delivery)
        assert renewal._timer is not None or renewal._task is not None
        try:
            runner.stop_consume_event.set()
            await asyncio.wait_for(asyncio.shield(running), 1)
            await asyncio.wait_for(rejection_cancelled.wait(), 1)
            assert renewal._closed
            assert renewal._timer is None
            assert renewal._task is None or renewal._task.done()
            assert not processing.done()
        finally:
            release_actor.set()
            await asyncio.wait_for(processing, 1)


async def test_shutdown_rejects_if_a_reserved_settlement_fails_on_cancellation() -> None:
    runner, actor = make_runner(AsyncMock())
    actor.confirmation_mode = "ack_first"
    message = make_message()
    ack_started = asyncio.Event()

    async def ack() -> None:
        message.is_acted_on = True
        ack_started.set()
        try:
            await asyncio.Future()
        finally:
            # An adapter clears its reserved action after a confirmed RPC failure.
            message.is_acted_on = False

    message.ack.side_effect = ack
    async with run_messages(runner, actor, grace=0.0) as (submit, running):
        await submit(message)
        await asyncio.wait_for(ack_started.wait(), 1)
        runner.stop_consume_event.set()
        await asyncio.wait_for(asyncio.shield(running), 1)

    message.reject.assert_awaited_once()
    actor.fn.assert_not_awaited()


async def test_processing_reuses_waiting_renewal_task_and_deadline() -> None:
    release = asyncio.Event()
    waiting = asyncio.Event()
    processing = asyncio.Event()
    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, actor = make_runner(release.wait, server=server)
    actor.keep_alive = 3600.0
    message = make_message()
    message.keep_alive_interval = 20.0
    schedules: list[tuple[LeaseRenewal, float | None]] = []
    capacity = runner._budget
    assert capacity.try_acquire("a", 1)

    renew_at = _Delivery.renew_at

    def record_schedule(delivery: _Delivery, interval: float | None) -> None:
        renew_at(delivery, interval)
        schedules.append((_lease(delivery), _lease(delivery)._deadline))
        if len(schedules) == 1:
            waiting.set()
        else:
            processing.set()

    async with run_messages(runner, actor) as (submit, running):
        with patch.object(_Delivery, "renew_at", record_schedule):
            submission = asyncio.create_task(submit(message))
            await asyncio.wait_for(waiting.wait(), 1)
            capacity.release("a", 1)
            await asyncio.wait_for(submission, 1)
            await asyncio.wait_for(processing.wait(), 1)
            release.set()
            runner.stop_consume_event.set()
            await asyncio.wait_for(asyncio.shield(running), 1)

    first, second = schedules
    assert first == second
    renewal, deadline = first
    assert renewal._closed
    assert renewal._timer is None
    assert deadline is not None


async def test_cancellation_during_keep_alive_cleanup_releases_capacity() -> None:
    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    finish_actor = asyncio.Event()
    runner, actor = make_runner(finish_actor.wait, max_tasks=2, server=server)
    actor.keep_alive = True
    message = make_message()
    message.keep_alive_interval = 0.001
    keep_alive_started = asyncio.Event()
    cleaning = asyncio.Event()

    async def keep_alive() -> None:
        keep_alive_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await asyncio.Event().wait()

    message.keep_alive.side_effect = keep_alive
    async with run_messages(runner, actor) as (submit, running):
        await submit(message)
        await asyncio.wait_for(keep_alive_started.wait(), 1)
        finish_actor.set()
        await asyncio.wait_for(cleaning.wait(), 1)
        processing = next(iter(runner._tasks.values()))
        processing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await processing
        await asyncio.wait_for(submit(make_message()), 1)
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert runner.processed == 2


async def test_zero_lifetime_quota_skips_subscription() -> None:
    runner, _ = make_runner(AsyncMock(), max_tasks=0)
    with patch.object(runner.server, "subscribe", new_callable=AsyncMock) as subscribe:
        await runner.run({}, 0)
    subscribe.assert_not_awaited()


async def test_cancellation_during_shutdown_still_closes_subscriber() -> None:
    runner, actor = make_runner(AsyncMock())
    pause_started = asyncio.Event()
    allow_cleanup = asyncio.Event()

    async def pause() -> None:
        pause_started.set()
        await allow_cleanup.wait()

    subscriber = Mock(
        task=asyncio.create_task(asyncio.Event().wait()),
        pause=AsyncMock(side_effect=pause),
        close=AsyncMock(),
    )
    runner.server = Mock(subscribe=AsyncMock(return_value=subscriber))
    running = asyncio.create_task(runner.run({"a": [actor]}, 0))
    runner.stop_consume_event.set()
    await pause_started.wait()
    running.cancel()
    await asyncio.sleep(0)
    assert not running.done()
    allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await running
    subscriber.close.assert_awaited_once()
    subscriber.task.cancel()
    await asyncio.gather(subscriber.task, return_exceptions=True)


@pytest.mark.parametrize(
    "blocked_reject",
    [
        pytest.param(False, id="immediate_rejection"),
        pytest.param(True, id="blocked_rejection"),
    ],
)
async def test_shutdown_is_bounded_with_inline_submission(blocked_reject: bool) -> None:  # noqa: PLR0915
    started = asyncio.Event()
    waiting = asyncio.Event()
    paused = asyncio.Event()
    closed = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    async def actor_fn() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def reject() -> None:
        await paused.wait()
        if blocked_reject:
            try:
                await release.wait()
            except asyncio.CancelledError:
                # Simulate an adapter still waiting for its network operation to finish.
                await release.wait()

    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, actor = make_runner(actor_fn, server=server)
    extra = make_message()
    extra.reject.side_effect = reject

    extra.keep_alive_interval = 0.001

    async def subscribe(*, intake: Intake) -> Mock:
        submit = admitting_submit(runner, intake.channels["a"])

        async def reader() -> None:
            await submit(make_message())
            await started.wait()
            waiting.set()
            await submit(extra)
            await closed.wait()

        reader_task = asyncio.create_task(reader())

        async def close() -> None:
            closed.set()
            await reader_task

        return Mock(
            task=reader_task,
            pause=AsyncMock(side_effect=paused.set),
            close=AsyncMock(side_effect=close),
        )

    with patch.object(runner.server, "subscribe", side_effect=subscribe):
        running = asyncio.create_task(runner.run({"a": [actor]}, 0.01, 0.01))
        try:
            await asyncio.wait_for(waiting.wait(), 1)
            runner.stop_consume_event.set()
            await asyncio.wait_for(asyncio.shield(running), 0.5)
            extra.reject.assert_awaited_once()
            assert cancelled.is_set()
            assert closed.is_set()
        finally:
            release.set()
            runner.stop_consume_event.set()
            await asyncio.wait_for(running, 1)


@pytest.mark.parametrize(
    "fail_rejection",
    [
        pytest.param(False, id="successful_rejection"),
        pytest.param(True, id="failed_rejection"),
    ],
)
async def test_shutdown_bounds_prestart_cleanup(fail_rejection: bool) -> None:
    runner, actor = make_runner(AsyncMock(), max_tasks=1)
    message = make_message()
    rejecting = asyncio.Event()
    cancelled = asyncio.Event()

    async def reject() -> None:
        rejecting.set()
        if fail_rejection:
            raise ConnectionError("reject failed")
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    message.reject.side_effect = reject
    async with run_messages(runner, actor, grace=0.01, cancellation_timeout=0.01) as (
        _,
        running,
    ):
        _cancel_before_start(runner, message)
        await asyncio.wait_for(rejecting.wait(), 1)
        runner.stop_consume_event.set()
        await asyncio.wait_for(asyncio.shield(running), 0.5)
        assert cancelled.is_set() != fail_rejection
        assert not runner._cleanup_tasks


async def test_in_memory_saturated_channel_leaves_room_for_another_channel() -> None:
    app = Repid()
    server = InMemoryServer()
    app.servers.register_server("default", server, is_default=True)
    router = Router()
    other_started = asyncio.Event()

    @router.actor(channel=Channel(address="a", concurrency=Concurrency(messages=1)))
    async def waits_for_other() -> None:
        await other_started.wait()

    @router.actor(channel="b")
    async def releases_other() -> None:
        other_started.set()

    app.include_router(router)
    async with server.connection():
        for _ in range(5):
            await server.publish(
                channel="a",
                message=MessageData(
                    payload=b"{}",
                    headers={"topic": "waits_for_other"},
                    content_type="application/json",
                ),
            )
        running = asyncio.create_task(
            app.run_worker(
                concurrency=Concurrency(messages=2),
                messages_limit=6,
                graceful_shutdown_time=0.1,
                register_signals=[],
            ),
        )
        try:
            # Let A fill its channel's capacity before B receives a message.
            async with asyncio.timeout(1):
                while not server.queues["a"].processing:
                    await asyncio.sleep(0)
            await server.publish(
                channel="b",
                message=MessageData(
                    payload=b"{}",
                    headers={"topic": "releases_other"},
                    content_type="application/json",
                ),
            )
            result = await asyncio.wait_for(asyncio.shield(running), 1)
            assert result.processed == 6
        finally:
            other_started.set()
            if not running.done():
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)


async def test_lifetime_quota_gives_admitted_actor_time_to_finish() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def fn() -> None:
        started.set()
        await release.wait()
        finished.set()

    runner, actor = make_runner(fn, max_tasks=1)
    async with run_messages(runner, actor, grace=0.01) as (submit, running):
        await submit(make_message())
        await asyncio.wait_for(started.wait(), 1)
        assert runner.max_tasks_hit

        extra = make_message()
        await submit(extra)
        extra.reject.assert_awaited_once()

        done, _ = await asyncio.wait({running}, timeout=0.05)
        assert not done
        assert runner.processed == 0

        release.set()
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert finished.is_set()
        assert runner.processed == 1


async def test_shutdown_still_cancels_actor_after_reaching_quota() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def fn() -> None:
        try:
            started.set()
            await asyncio.Future()
        finally:
            cancelled.set()

    runner, actor = make_runner(fn, max_tasks=1)
    message = make_message()
    async with run_messages(runner, actor, grace=0.01) as (submit, running):
        await submit(message)
        await asyncio.wait_for(started.wait(), 1)
        runner.stop_consume_event.set()
        await asyncio.wait_for(asyncio.shield(running), 0.5)

    assert cancelled.is_set()
    message.reject.assert_awaited_once()
    assert runner.processed == 1


async def test_stopped_runner_leaves_already_settled_delivery_alone() -> None:
    runner, actor = make_runner(AsyncMock())
    message = make_message()
    message.is_acted_on = True
    runner.stop_consume_event.set()

    await handle_message(runner, [actor], message)

    message.reject.assert_not_awaited()
    actor.fn.assert_not_awaited()


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


async def test_first_completion_after_quota_starts_graceful_shutdown() -> None:
    first_can_finish = asyncio.Event()
    second_started = asyncio.Event()
    second_cancelled = asyncio.Event()

    calls = 0

    async def job() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            await first_can_finish.wait()
            return
        try:
            second_started.set()
            await asyncio.Future()
        finally:
            second_cancelled.set()

    runner, actor = make_runner(
        job,
        limits=Concurrency(messages=2),
        max_tasks=2,
    )
    async with run_messages(runner, actor, grace=0.01) as (submit, running):
        await submit(make_message())
        message = make_message()
        await submit(message)
        await asyncio.wait_for(second_started.wait(), 1)
        assert runner.max_tasks_hit

        first_can_finish.set()
        await asyncio.wait_for(asyncio.shield(running), 0.5)
        assert second_cancelled.is_set()
        message.reject.assert_awaited_once()
        assert runner.processed == 2


@pytest.mark.parametrize(
    ("tasks_limit", "concurrency", "worker_limit"),
    [
        pytest.param(2, None, 2, id="legacy_task_limit"),
        pytest.param(None, Concurrency(messages=2), 2, id="concurrency_message_limit"),
        pytest.param(None, None, 1000, id="default_message_limit"),
    ],
)
async def test_worker_passes_channel_concurrency_on_every_subscription(
    tasks_limit: int | None,
    concurrency: Concurrency | None,
    worker_limit: int,
) -> None:
    server = InMemoryServer()
    app = Repid()
    app.servers.register_server("default", server, is_default=True)
    router = Router()

    channel_limit = Concurrency(messages=3, payload_bytes=2048)

    @router.actor(channel=Channel(address="a", concurrency=channel_limit))
    async def job() -> None:
        pass

    app.include_router(router)
    subscribe = server.subscribe

    async def capture_subscribe(*, intake: Intake) -> SubscriberT:
        assert {channel: c.limits for channel, c in intake.channels.items()} == {
            "a": Concurrency(messages=min(worker_limit, 3), payload_bytes=2048),
        }
        return await subscribe(intake=intake)

    async with server.connection():
        await server.publish(
            channel="a",
            message=MessageData(payload=b"{}", headers={"topic": "job"}),
        )
        warning = (
            pytest.warns(DeprecationWarning, match="tasks_limit is deprecated")
            if tasks_limit is not None
            else nullcontext()
        )
        with patch.object(server, "subscribe", capture_subscribe), warning:
            async with asyncio.timeout(1):
                result = await app.run_worker(
                    messages_limit=1,
                    register_signals=[],
                    tasks_limit=tasks_limit,
                    concurrency=concurrency,
                )
    assert result.processed == 1


async def test_tasks_limit_and_concurrency_are_mutually_exclusive() -> None:
    app = Repid()
    app.servers.register_server("default", InMemoryServer(), is_default=True)
    with pytest.raises(ValueError, match="either tasks_limit or concurrency"):
        await app.run_worker(tasks_limit=2, concurrency=Concurrency(messages=2))


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


async def test_delivery_creates_renewal_only_for_an_interval() -> None:
    delivery = _Delivery(Mock(keep_alive=AsyncMock()), _Budget(Concurrency()))
    delivery.renew_at(None)
    delivery.renew_at(0)
    delivery.stop_renewal()
    assert delivery._renewal is None
    await delivery.finish()

    delivery.renew_at(3600)
    # An idle renewal is a timer; no task runs until a renewal is due.
    assert _lease(delivery)._timer is not None
    assert _lease(delivery)._task is None
    # A non-positive interval stops an existing renewal's schedule.
    delivery.renew_at(0)
    assert _lease(delivery)._deadline is None
    assert _lease(delivery)._timer is None
    await delivery.finish()


async def test_delivery_acquire_does_not_charge_a_held_budget_twice() -> None:
    budget = _Budget(Concurrency(messages=2))
    delivery = _Delivery(Mock(channel="a"), budget)
    assert delivery.try_acquire()
    assert delivery.try_acquire()
    assert await delivery.acquire()
    assert budget.worker.messages == 1

    delivery.release()
    assert budget.worker.messages == 0


async def test_delivery_finished_inside_create_task_is_not_registered() -> None:
    # An eager task factory can run a delivery's task to completion before
    # create_task returns.
    runner, actor = make_runner(AsyncMock())
    message = make_message()
    loop = asyncio.get_running_loop()

    def run_to_completion(coroutine: Coroutine[None, None, None]) -> asyncio.Future[None]:
        finished: asyncio.Future[None] = loop.create_future()
        with pytest.raises(StopIteration):
            coroutine.send(None)
        finished.set_result(None)
        return finished

    async with run_messages(runner, actor) as (_, running):
        with patch.object(asyncio, "create_task", side_effect=run_to_completion):
            runner._intake.channels["a"].submit(message)
        assert not runner._tasks
        assert runner.processed == 1
        message.ack.assert_not_awaited()
        assert runner._intake.channels["a"].demand.messages == 1
        assert not running.done()


@pytest.mark.skipif(sys.version_info < (3, 12), reason="eager task factory needs Python 3.12")
async def test_runner_works_with_an_eager_task_factory() -> None:
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)  # pyrefly: ignore[missing-attribute]
    try:
        runner, actor = make_runner(AsyncMock(), max_tasks=2)
        async with run_messages(runner, actor) as (submit, running):
            await submit(make_message())
            await submit(make_message())
            await asyncio.wait_for(asyncio.shield(running), 1)
        assert runner.processed == 2
        assert not runner._tasks
    finally:
        loop.set_task_factory(previous)
