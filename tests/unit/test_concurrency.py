from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager, nullcontext
from dataclasses import FrozenInstanceError
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from repid import Channel, Concurrency, LeaseRenewal, Repid, Router
from repid._concurrency import _channel_hints, _ConcurrencyController
from repid._runner import _Runner
from repid.connections.abc import (
    MessageCallbackT,
    ReceivedMessageT,
    ServerT,
    SubscriberT,
)
from repid.connections.in_memory import InMemoryServer
from repid.data import ActorData, ActorExecutionContext, MessageData


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


def make_runner(
    fn: Callable[..., Any],
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
    actor = Mock(spec=ActorData)
    actor.name = "job"
    actor.timeout = None
    actor.keep_alive = False
    actor.confirmation_mode = "manual"
    actor.on_error = "nack"
    actor.routing_strategy = lambda _message: True  # pyrefly: ignore[implicit-any-lambda]
    actor.converter.convert_inputs = AsyncMock(return_value=([], {}))
    actor.fn = fn

    async def middleware(call_next: Any, message: Any, actor: Any) -> Any:
        return await call_next(message, actor)

    actor.middleware_pipeline = middleware
    return _Runner(
        actor_context=context,
        concurrency=limits or Concurrency(messages=1),
        max_tasks=max_tasks,
    ), actor


def make_message(payload: bytes = b"abc") -> Mock:
    message = Mock(spec=ReceivedMessageT)
    message.payload = payload
    message.channel = "a"
    message.is_acted_on = False
    message.keep_alive_interval = None
    message.reject = AsyncMock()
    message.ack = AsyncMock()
    message.nack = AsyncMock()
    return message


@asynccontextmanager
async def run_messages(
    runner: _Runner,
    actor: ActorData,
    *,
    grace: float = 0.1,
    cancellation_timeout: float = 1.0,
) -> AsyncGenerator[tuple[MessageCallbackT, asyncio.Task[None]], None]:
    subscribed = asyncio.Event()
    subscriber = Mock(
        task=asyncio.get_running_loop().create_future(),
        pause=AsyncMock(),
        close=AsyncMock(),
    )

    async def subscribe_to_messages(**_: Any) -> Mock:
        subscribed.set()
        return subscriber

    with patch.object(runner.server, "subscribe", side_effect=subscribe_to_messages) as subscribe:
        running = asyncio.create_task(runner.run({"a": [actor]}, grace, cancellation_timeout))
        try:
            await asyncio.wait_for(subscribed.wait(), 1)
            yield subscribe.call_args.kwargs["channels_to_callbacks"]["a"], running
        finally:
            runner.stop_consume_event.set()
            await asyncio.wait_for(running, 1)
            subscriber.task.cancel()


@pytest.mark.parametrize("confirmation", ["manual", "manual_explicit", "ack_first"])
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


async def test_prestart_cancellation_consumes_quota_and_shutdown_waits_for_rejection() -> None:
    runner, actor = make_runner(AsyncMock(), max_tasks=1)
    message = make_message()
    rejecting = asyncio.Event()
    release = asyncio.Event()

    async def reject() -> None:
        rejecting.set()
        await release.wait()

    message.reject.side_effect = reject
    async with run_messages(runner, actor) as (submit, running):
        create_task = asyncio.create_task

        def cancel_on_create(coroutine: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
            task = create_task(coroutine)
            task.cancel()
            return task

        with patch("repid._message_lifecycle.asyncio.create_task", side_effect=cancel_on_create):
            await submit(message)

        await asyncio.wait_for(rejecting.wait(), 1)
        assert runner.max_tasks_hit
        assert not running.done()
        extra = make_message()
        await submit(extra)
        extra.reject.assert_awaited_once()
        release.set()
        await asyncio.wait_for(asyncio.shield(running), 1)
        actor.fn.assert_not_awaited()
        message.reject.assert_awaited_once()
        assert runner.processed == 0


async def test_prestart_rejection_cancelled_before_running_cancels_renewal() -> None:
    server = Mock(spec=ServerT, capabilities={"supports_keep_alive": True})
    runner, actor = make_runner(AsyncMock(), max_tasks=1, server=server)
    message = make_message()
    message.keep_alive_interval = 0.001
    renewed: asyncio.Future[asyncio.Task[object] | None] = (
        asyncio.get_running_loop().create_future()
    )
    capacity = runner._messages._capacity
    held = capacity.try_acquire("a", 0)
    assert held is not None

    def record_renewal() -> None:
        if not renewed.done():
            renewed.set_result(asyncio.current_task())

    message.keep_alive.side_effect = record_renewal
    create_task = asyncio.create_task

    def cancel_before_start(coroutine: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = create_task(coroutine)
        if getattr(coroutine, "__qualname__", "") in (
            "_MessageLifecycle._process",
            "_MessageLifecycle._reject_and_release",
        ):
            task.cancel()
        return task

    async with run_messages(runner, actor) as (submit, running):
        with patch("repid._message_lifecycle.asyncio.create_task", side_effect=cancel_before_start):
            submission = asyncio.create_task(submit(message))
            renewal = await asyncio.wait_for(renewed, 1)
            held.release()
            await asyncio.wait_for(submission, 1)
            await asyncio.wait_for(asyncio.shield(running), 1)

    assert renewal is not None
    assert renewal.cancelled()
    assert capacity.worker.messages == 0
    message.reject.assert_not_awaited()


async def test_processing_task_creation_failure_rejects_and_releases_capacity() -> None:
    runner, actor = make_runner(AsyncMock(), max_tasks=1)
    message = make_message()
    async with run_messages(runner, actor) as (submit, running):
        with (
            patch(
                "repid._message_lifecycle.asyncio.create_task",
                side_effect=RuntimeError("cannot schedule"),
            ),
            pytest.raises(RuntimeError, match="cannot schedule"),
        ):
            await submit(message)
        message.reject.assert_awaited_once()
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
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        waiter_message.reject.assert_awaited_once()
        release.set()
        await asyncio.wait_for(submit(make_message()), 1)
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert runner.processed == 2


async def test_failed_early_ack_stops_processing(caplog: pytest.LogCaptureFixture) -> None:
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
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert runner.stop_consume_event.is_set()
        assert runner.processed == 1
        assert any(record.message == "runner.processing.error" for record in caplog.records)
        actor.fn.assert_not_awaited()
        extra.reject.assert_awaited_once()


@pytest.mark.parametrize("actor_keep_alive", [False, 3600.0])
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
    renewal_tasks: set[asyncio.Task[object] | None] = set()

    def keep_alive() -> None:
        renewal_tasks.add(asyncio.current_task())
        keep_alive_called.set()
        if rejecting.is_set():
            renewed_during_reject.set()

    async def reject() -> None:
        rejecting.set()
        await asyncio.wait_for(renewed_during_reject.wait(), 0.5)

    message.keep_alive = AsyncMock(side_effect=keep_alive)
    message.reject.side_effect = reject

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
    assert len(renewal_tasks) == 1
    assert all(task is not None and task.done() for task in renewal_tasks)


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
    message.keep_alive = AsyncMock(side_effect=keep_alive)
    message.reject.side_effect = reject

    async with run_messages(runner, actor, grace=0.0) as (submit, running):
        await submit(message)
        await asyncio.wait_for(actor_started.wait(), 1)
        runner.stop_consume_event.set()
        await asyncio.wait_for(asyncio.shield(running), 1)

    assert rejected.is_set()


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
    schedules: list[tuple[asyncio.Task[None] | None, float | None]] = []
    capacity = runner._messages._capacity
    held = capacity.try_acquire("a", 1)
    assert held is not None

    set_interval = LeaseRenewal.set_interval

    def record_schedule(renewal: LeaseRenewal, interval: float | None) -> None:
        set_interval(renewal, interval)
        schedules.append((renewal._task, renewal._deadline))
        if len(schedules) == 1:
            waiting.set()
        else:
            processing.set()

    async with run_messages(runner, actor) as (submit, running):
        with patch.object(LeaseRenewal, "set_interval", record_schedule):
            submission = asyncio.create_task(submit(message))
            await asyncio.wait_for(waiting.wait(), 1)
            held.release()
            await asyncio.wait_for(submission, 1)
            await asyncio.wait_for(processing.wait(), 1)
            release.set()
            runner.stop_consume_event.set()
            await asyncio.wait_for(asyncio.shield(running), 1)

    first, second = schedules
    assert first == second
    task, deadline = first
    assert task is not None
    assert task.done()
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

    message.keep_alive = AsyncMock(side_effect=keep_alive)
    async with run_messages(runner, actor) as (submit, running):
        await submit(message)
        await asyncio.wait_for(keep_alive_started.wait(), 1)
        finish_actor.set()
        await asyncio.wait_for(cleaning.wait(), 1)
        processing = next(iter(runner._messages._processing))
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


@pytest.mark.parametrize("blocked_reject", [False, True])
async def test_shutdown_is_bounded_with_inline_submission(blocked_reject: bool) -> None:
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

    runner, actor = make_runner(actor_fn)
    extra = make_message()
    extra.reject.side_effect = reject

    async def subscribe(**kwargs: Any) -> Mock:
        submit = kwargs["channels_to_callbacks"]["a"]

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


@pytest.mark.parametrize("fail_rejection", [False, True])
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
        submit,
        running,
    ):
        create_task = asyncio.create_task

        def cancel_on_create(coroutine: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
            task = create_task(coroutine)
            task.cancel()
            return task

        with patch("repid._message_lifecycle.asyncio.create_task", side_effect=cancel_on_create):
            await submit(message)
        await asyncio.wait_for(rejecting.wait(), 1)
        await asyncio.wait_for(asyncio.shield(running), 0.5)
        assert cancelled.is_set() != fail_rejection
        assert not runner._messages._prestart_cleanup


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
            # Let A fill the subscription's intake before B receives a message.
            async with asyncio.timeout(1):
                while len(server.queues["a"].processing) < 2:
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

    await runner._message_handler([actor], message)

    message.reject.assert_not_awaited()
    actor.fn.assert_not_awaited()


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
    ("options", "worker_limit"),
    [
        ({"tasks_limit": 2}, 2),
        ({"concurrency": Concurrency(messages=2)}, 2),
        ({}, 1000),
    ],
)
async def test_worker_passes_channel_concurrency_on_every_subscription(
    options: dict[str, Any],
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

    async def capture_subscribe(
        *,
        channels_to_callbacks: dict[str, MessageCallbackT],
        concurrency: Concurrency,
        channel_concurrency: dict[str, Concurrency],
    ) -> SubscriberT:
        assert concurrency == Concurrency(messages=worker_limit)
        assert channel_concurrency == {"a": channel_limit}
        return await subscribe(
            channels_to_callbacks=channels_to_callbacks,
            concurrency=concurrency,
            channel_concurrency=channel_concurrency,
        )

    async with server.connection():
        await server.publish(
            channel="a",
            message=MessageData(payload=b"{}", headers={"topic": "job"}),
        )
        warning = (
            pytest.warns(DeprecationWarning, match="tasks_limit is deprecated")
            if "tasks_limit" in options
            else nullcontext()
        )
        with patch.object(server, "subscribe", capture_subscribe), warning:
            async with asyncio.timeout(1):
                result = await app.run_worker(messages_limit=1, register_signals=[], **options)
    assert result.processed == 1


async def test_tasks_limit_and_concurrency_are_mutually_exclusive() -> None:
    app = Repid()
    app.servers.register_server("default", InMemoryServer(), is_default=True)
    with pytest.raises(ValueError, match="either tasks_limit or concurrency"):
        await app.run_worker(tasks_limit=2, concurrency=Concurrency(messages=2))


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


async def test_lease_renewal_handoff_is_scoped_to_its_delivery() -> None:
    message = make_message()
    other = make_message()
    renewal = LeaseRenewal(message)
    closed = LeaseRenewal(message)
    await closed.close()

    with closed.handoff():
        assert LeaseRenewal.claim_for(message) is None
    with renewal.handoff():
        assert LeaseRenewal.claim_for(other) is None
    assert LeaseRenewal.claim_for(message) is None
    with renewal.handoff():
        assert LeaseRenewal.claim_for(message) is renewal
        assert LeaseRenewal.claim_for(message) is None

    await renewal.close_if_unclaimed()
    await renewal.close()
