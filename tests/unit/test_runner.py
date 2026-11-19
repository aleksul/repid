from __future__ import annotations

import asyncio
import logging
from typing import Literal
from unittest.mock import AsyncMock, Mock, patch

import pytest

from repid import Concurrency, Repid, Router, ServerT
from repid._concurrency import _Budget
from repid._delivery import LeaseRenewal, _Delivery
from repid._runner import _processing_interval, _run_actor_pipeline, _Runner
from repid.connections.abc import ReceivedMessageT
from repid.connections.in_memory import InMemoryServer
from repid.connections.in_memory.message_broker import InMemoryReceivedMessage
from repid.data import ActorData, ActorExecutionContext, MessageData
from repid.health_check_server import HealthCheckServer, HealthCheckStatus
from repid.serializer import default_serializer
from repid.test_client import TestClient


def _lease(delivery: _Delivery) -> LeaseRenewal:
    lease = delivery._renewal
    assert lease is not None
    return lease


async def _actor_run(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
) -> object:
    delivery = _Delivery(message, _Budget(Concurrency()))
    try:
        delivery.renew_at(_processing_interval(actor, message, actor_context))
        return await _run_actor_pipeline(actor, message, actor_context, delivery.stop_renewal)
    finally:
        await delivery.finish()


async def test_runner_actor_ack_first_mode() -> None:
    app = Repid()
    router = Router()

    processed = False

    @router.actor(confirmation_mode="ack_first")
    async def ack_first_actor(arg1: str) -> None:  # noqa: ARG001
        nonlocal processed
        processed = True

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"arg1": "test"},
            headers={"topic": "ack_first_actor"},
        )
        assert client.get_processed_messages()[0].acked


async def test_runner_ack_first_ack_failure_skips_actor_without_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    router = Router()
    actor_ran = False

    @router.actor(confirmation_mode="ack_first")
    async def ack_first_actor() -> None:
        nonlocal actor_ran
        actor_ran = True

    actor = router._actors_per_channel_address["default"][0]
    message = Mock(spec=ReceivedMessageT, is_acted_on=False, message_id="1")
    message.ack = AsyncMock(side_effect=ConnectionError("ack failed"))
    context = ActorExecutionContext(
        server=Mock(spec=ServerT),
        publish=AsyncMock(),
        default_serializer=default_serializer,
    )

    with caplog.at_level(logging.WARNING, logger="repid"):
        result = await _run_actor_pipeline(actor, message, context)

    assert isinstance(result, ConnectionError)
    assert not actor_ran
    assert "actor.ack_first.error" in caplog.messages


async def test_runner_actor_keep_alive_disabled() -> None:
    app = Repid()
    router = Router(keep_alive=False)

    processed = False

    @router.actor()
    async def ka_actor(arg1: str) -> None:  # noqa: ARG001
        nonlocal processed
        processed = True

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"arg1": "test"},
            headers={"topic": "ka_actor"},
        )
        assert processed


async def test_runner_actor_always_ack_mode_with_exception() -> None:
    app = Repid()
    router = Router()

    @router.actor(confirmation_mode="always_ack")
    async def always_ack_actor() -> None:
        raise ValueError("Intentional error")

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "always_ack_actor"},
        )
        assert client.get_processed_messages()[0].acked


async def test_runner_actor_manual_mode_unacknowledged_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = Repid()
    router = Router()

    @router.actor(confirmation_mode="manual")
    async def manual_actor() -> None:
        pass  # Does not ack/nack/reject the message

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "manual_actor"},
        )

    warning_log = caplog.get_records(when="call")[0]
    assert warning_log.name == "repid"
    assert warning_log.levelno == logging.WARNING
    assert warning_log.message == "actor.ack.manual.unacknowledged"


async def test_runner_actor_auto_mode_nack_on_exception() -> None:
    app = Repid()
    router = Router()

    @router.actor(confirmation_mode="auto")
    async def auto_actor() -> None:
        raise ValueError("Intentional error")

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "auto_actor"},
        )
        assert client.get_processed_messages()[0].nacked


@pytest.mark.parametrize(
    "timeout_value",
    [0, float("inf")],
    ids=["zero_timeout", "infinite_timeout"],
)
async def test_runner_actor_without_timeout(timeout_value: float) -> None:
    app = Repid()
    router = Router(timeout=timeout_value)

    received = None

    @router.actor()
    async def no_timeout_actor(arg1: str) -> None:
        nonlocal received
        received = arg1

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"arg1": "test"},
            headers={"topic": "no_timeout_actor"},
        )
        assert client.get_processed_messages()[0].acked

    assert received == "test"


async def test_runner_actor_timeout_exceeded() -> None:
    app = Repid()
    router = Router()

    @router.actor(timeout=0.01)  # Very short timeout
    async def slow_actor() -> None:
        await asyncio.sleep(1)  # Sleep longer than timeout

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "slow_actor"},
        )

        msg = client.get_processed_messages()[0]
        assert msg.nacked
        assert isinstance(msg.exception, asyncio.TimeoutError)


async def test_runner_actor_always_ack_mode_with_timeout() -> None:
    app = Repid()
    router = Router()

    @router.actor(confirmation_mode="always_ack", timeout=0.01)
    async def slow_always_ack_actor() -> None:
        await asyncio.sleep(1)

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "slow_always_ack_actor"},
        )

        msg = client.get_processed_messages()[0]
        assert msg.acked
        assert isinstance(msg.exception, asyncio.TimeoutError)


@pytest.mark.parametrize(
    "supports_lightweight_pause",
    [True, False],
    ids=["lightweight_pause", "close_to_pause"],
)
async def test_runner_max_tasks_hit(supports_lightweight_pause: bool) -> None:
    server = InMemoryServer()
    mocked_server = Mock(spec=server, wraps=server)
    mocked_server.capabilities.side_effect = {
        **server.capabilities,
        "supports_lightweight_pause": supports_lightweight_pause,
    }

    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(mocked_server),
            max_tasks=5,
            concurrency=Concurrency(messages=10),
        )

        assert not runner.max_tasks_hit

        for _ in range(5):
            await server.publish(
                channel="default",
                message=MessageData(
                    payload=b"",
                    headers={"topic": "test_actor"},
                    content_type="application/json",
                ),
            )

        await runner.run(
            channels_to_actors=router._actors_per_channel_address,
            graceful_termination_timeout=0.1,
        )

        assert runner.processed == 5
        assert runner.max_tasks_hit


async def test_runner_cancel_event_during_actor_execution() -> None:
    server = InMemoryServer()
    router = Router()

    started = asyncio.Event()
    cancelled = False

    @router.actor()
    async def cancellable_actor() -> None:
        nonlocal cancelled
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled = True
            raise

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(server),
        )

        async def trigger_cancel() -> None:
            await started.wait()
            await asyncio.sleep(0.01)
            runner.stop_consume_event.set()
            await asyncio.sleep(0.01)
            runner.cancel_event.set()

        async def publish_message() -> None:
            await server.publish(
                channel="default",
                message=MessageData(
                    payload=b"{}",
                    headers={"topic": "cancellable_actor"},
                    content_type="application/json",
                ),
            )

        cancel_task = asyncio.create_task(trigger_cancel())
        publish_task = asyncio.create_task(publish_message())
        run_task = asyncio.create_task(
            runner.run(
                channels_to_actors=router._actors_per_channel_address,
                graceful_termination_timeout=0.1,
                cancellation_timeout=0.1,
            ),
        )

        await asyncio.gather(cancel_task, publish_task, run_task)
        assert cancelled


async def test_runner_no_matching_actor_rejects_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = InMemoryServer()
    router = Router()

    @router.actor()
    async def some_actor() -> None:
        pass

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(server),
        )

        await server.publish(
            channel="default",
            message=MessageData(
                payload=b"{}",
                headers={"topic": "nonexistent_actor"},
                content_type="application/json",
            ),
        )

        queue = server.queues["default"]
        raw = await queue.queue.get()
        queue.processing.add(raw)
        await runner._message_handler(
            router._actors_per_channel_address["default"],
            InMemoryReceivedMessage(raw, queue, "default"),
        )
        runner.stop_consume_event.set()

        await runner.run(
            channels_to_actors=router._actors_per_channel_address,
            graceful_termination_timeout=0.1,
        )

        warning_log = next(
            (r for r in caplog.get_records(when="call") if r.levelno == logging.WARNING),
            None,
        )
        assert warning_log is not None
        assert warning_log.message == "actor.route.not_found"


async def test_runner_pause_and_resume_with_concurrency_limit() -> None:
    server = InMemoryServer()
    router = Router()

    processing = []
    start_event = asyncio.Event()
    continue_event = asyncio.Event()

    @router.actor()
    async def slow_actor(index: int) -> None:
        processing.append(index)
        if len(processing) == 2:
            start_event.set()
        await continue_event.wait()

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(server),
            concurrency=Concurrency(messages=2),
        )

        async def publish_messages() -> None:
            for i in range(3):
                await server.publish(
                    channel="default",
                    message=MessageData(
                        payload=default_serializer({"index": i}),
                        headers={"topic": "slow_actor"},
                        content_type="application/json",
                    ),
                )

        async def trigger_continue() -> None:
            await start_event.wait()
            await asyncio.sleep(0.1)
            assert len(processing) == 2
            continue_event.set()
            await asyncio.sleep(0.2)
            runner.stop_consume_event.set()

        publish_task = asyncio.create_task(publish_messages())
        continue_task = asyncio.create_task(trigger_continue())
        run_task = asyncio.create_task(
            runner.run(
                channels_to_actors=router._actors_per_channel_address,
                graceful_termination_timeout=0.5,
            ),
        )

        await asyncio.gather(publish_task, continue_task, run_task)
        assert len(processing) == 3


@pytest.mark.parametrize(
    "message_cap",
    [1, 2, 10],
    ids=["single_message", "two_messages", "ten_messages"],
)
async def test_runner_resumes_saturated_in_memory_subscriber(message_cap: int) -> None:
    server = InMemoryServer()
    router = Router()
    release = asyncio.Event()
    saturated = asyncio.Event()
    started = finished = 0
    message_count = message_cap * 2 + 2

    @router.actor
    async def job() -> None:
        nonlocal started, finished
        started += 1
        if started == message_cap:
            saturated.set()
        await release.wait()
        finished += 1

    async with server.connection():
        for _ in range(message_count):
            await server.publish(
                channel="default",
                message=MessageData(
                    payload=b"{}",
                    headers={"topic": "job"},
                    content_type="application/json",
                ),
            )
        runner = _Runner(
            actor_context=_make_actor_context(server),
            concurrency=Concurrency(messages=message_cap),
            max_tasks=message_count,
        )
        running = asyncio.create_task(
            runner.run(
                channels_to_actors=router._actors_per_channel_address,
                graceful_termination_timeout=1,
            ),
        )
        try:
            await asyncio.wait_for(saturated.wait(), timeout=1)
            release.set()
            await asyncio.wait_for(asyncio.shield(running), timeout=1)
            assert finished == message_count
            assert runner.processed == message_count
        finally:
            release.set()
            runner.stop_consume_event.set()
            await asyncio.wait_for(running, timeout=1)


async def test_runner_subscriber_exception_sets_unhealthy() -> None:
    server = InMemoryServer()

    class FailingSubscriber:
        def __init__(self) -> None:
            self.task = asyncio.create_task(self._fail())

        async def _fail(self) -> None:
            raise RuntimeError("Subscriber failed")

        async def pause(self) -> None:
            pass

        async def resume(self) -> None:
            pass

        async def close(self) -> None:
            pass

    async def failing_subscribe(*args, **kwargs):  # pyrefly: ignore[implicit-any-parameter, unannotated-return]  # noqa: ARG001
        return FailingSubscriber()

    health_check_server = HealthCheckServer()
    router = Router()

    @router.actor()
    async def test_actor() -> None:
        pass

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(server),
            health_check_server=health_check_server,
        )

        with patch.object(server, "subscribe", side_effect=failing_subscribe):
            await runner.run(
                channels_to_actors=router._actors_per_channel_address,
                graceful_termination_timeout=0.1,
            )

        assert health_check_server.health_status == HealthCheckStatus.UNHEALTHY


async def test_runner_graceful_shutdown_with_timeout(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = InMemoryServer()
    router = Router()

    processing_event = asyncio.Event()

    @router.actor()
    async def long_running_actor() -> None:
        processing_event.set()
        await asyncio.sleep(10)

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(server),
        )

        async def publish_and_stop() -> None:
            await server.publish(
                channel="default",
                message=MessageData(
                    payload=b"{}",
                    headers={"topic": "long_running_actor"},
                    content_type="application/json",
                ),
            )
            await processing_event.wait()
            await asyncio.sleep(0.01)
            runner.stop_consume_event.set()

        publish_task = asyncio.create_task(publish_and_stop())
        await runner.run(
            channels_to_actors=router._actors_per_channel_address,
            graceful_termination_timeout=0.05,
        )
        await publish_task

        error_log = next(
            (
                r
                for r in caplog.get_records(when="call")
                if r.levelno == logging.ERROR and r.message == "runner.shutdown.tasks_timeout"
            ),
            None,
        )
        assert error_log is not None


async def test_runner_pause_exception_during_shutdown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = InMemoryServer()

    class FailingPauseSubscriber:
        def __init__(self, original_subscriber) -> None:  # pyrefly: ignore[implicit-any-parameter]
            self.original_subscriber = original_subscriber
            self.task = original_subscriber.task

        async def pause(self) -> None:
            raise RuntimeError("Pause failed")

        async def resume(self) -> None:
            await self.original_subscriber.resume()

        async def close(self) -> None:
            await self.original_subscriber.close()

    original_subscribe = server.subscribe

    async def failing_pause_subscribe(*args, **kwargs):  # pyrefly: ignore[implicit-any-parameter, unannotated-return]
        original = await original_subscribe(*args, **kwargs)
        return FailingPauseSubscriber(original)

    router = Router()

    @router.actor()
    async def test_actor() -> None:
        pass

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(server),
        )

        runner.stop_consume_event.set()

        with patch.object(server, "subscribe", side_effect=failing_pause_subscribe):
            await runner.run(
                channels_to_actors=router._actors_per_channel_address,
                graceful_termination_timeout=0.1,
            )

        exception_log = next(
            (
                r
                for r in caplog.get_records(when="call")
                if r.message == "runner.subscriber.pause.error"
            ),
            None,
        )
        assert exception_log is not None


async def test_runner_close_exception_during_shutdown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = InMemoryServer()

    class FailingCloseSubscriber:
        def __init__(self, original_subscriber) -> None:  # pyrefly: ignore[implicit-any-parameter]
            self.original_subscriber = original_subscriber
            self.task = original_subscriber.task

        async def pause(self) -> None:
            await self.original_subscriber.pause()

        async def resume(self) -> None:
            await self.original_subscriber.resume()

        async def close(self) -> None:
            raise RuntimeError("Close failed")

    original_subscribe = server.subscribe

    async def failing_close_subscribe(*args, **kwargs):  # pyrefly: ignore[implicit-any-parameter, unannotated-return]
        original = await original_subscribe(*args, **kwargs)
        return FailingCloseSubscriber(original)

    router = Router()

    @router.actor()
    async def test_actor() -> None:
        pass

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(server),
        )

        runner.stop_consume_event.set()

        with patch.object(server, "subscribe", side_effect=failing_close_subscribe):
            await runner.run(
                channels_to_actors=router._actors_per_channel_address,
                graceful_termination_timeout=0.1,
            )

        exception_log = next(
            (
                r
                for r in caplog.get_records(when="call")
                if r.message == "runner.subscriber.close.error"
            ),
            None,
        )
        assert exception_log is not None


async def test_runner_tasks_not_finishing_after_cancellation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = InMemoryServer()
    router = Router()

    processing_event = asyncio.Event()
    release_event = asyncio.Event()
    finished_event = asyncio.Event()

    @router.actor()
    async def stubborn_actor() -> None:
        processing_event.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await release_event.wait()
        finally:
            finished_event.set()

    async with server.connection():
        runner = _Runner(
            actor_context=_make_actor_context(server),
        )

        async def run_with_message() -> None:
            await server.publish(
                channel="default",
                message=MessageData(
                    payload=b"{}",
                    headers={"topic": "stubborn_actor"},
                    content_type="application/json",
                ),
            )
            await processing_event.wait()
            runner.stop_consume_event.set()

        run_task = asyncio.create_task(run_with_message())
        runner_task = asyncio.create_task(
            runner.run(
                channels_to_actors=router._actors_per_channel_address,
                graceful_termination_timeout=0.01,
                cancellation_timeout=0.01,
            ),
        )

        await run_task
        await runner_task

        try:
            assert not finished_event.is_set()
            queue = server.queues["default"]
            assert not queue.processing
            assert queue.queue.qsize() == 1
            assert runner.processed == 0
            assert any(
                r.levelno == logging.ERROR and r.message == "runner.shutdown.tasks_unfinished"
                for r in caplog.get_records(when="call")
            )
        finally:
            release_event.set()
            await asyncio.wait_for(finished_event.wait(), timeout=1)
            async with asyncio.timeout(1):
                while runner.processed == 0:
                    await asyncio.sleep(0)
        assert runner.processed == 1


async def test_runner_on_error_reject_rejects_message() -> None:
    app = Repid()
    router = Router()

    @router.actor(on_error="reject")
    async def reject_on_error_actor() -> None:
        raise ValueError("Intentional error")

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "reject_on_error_actor"},
        )
        assert client.get_processed_messages()[0].rejected


async def test_runner_on_error_nack_nacks_message() -> None:
    app = Repid()
    router = Router()

    @router.actor(on_error="nack")
    async def nack_on_error_actor() -> None:
        raise ValueError("Intentional error")

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "nack_on_error_actor"},
        )
        assert client.get_processed_messages()[0].nacked


async def test_runner_on_error_ack_acks_message() -> None:
    app = Repid()
    router = Router()

    @router.actor(on_error="ack")
    async def ack_on_error_actor() -> None:
        raise ValueError("Intentional error")

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "ack_on_error_actor"},
        )
        assert client.get_processed_messages()[0].acked


@pytest.mark.parametrize(
    ("exc_type", "expected_rejected"),
    [
        pytest.param(ValueError, True, id="value_error_is_rejected"),
        pytest.param(RuntimeError, False, id="runtime_error_is_nacked"),
    ],
)
async def test_runner_on_error_callable_routes_by_exception_type(
    exc_type: type[Exception],
    expected_rejected: bool,
) -> None:
    app = Repid()
    router = Router()

    def on_error_fn(exc: BaseException) -> Literal["nack", "reject"]:
        return "reject" if isinstance(exc, ValueError) else "nack"

    @router.actor(on_error=on_error_fn)
    async def callable_on_error_actor() -> None:
        raise exc_type("Intentional error")

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "callable_on_error_actor"},
        )
        msg = client.get_processed_messages()[0]
        assert msg.rejected == expected_rejected
        assert msg.nacked == (not expected_rejected)


def _make_unrouted_message(message_id: str | None, channel: str = "default") -> Mock:
    return Mock(message_id=message_id, channel=channel, nack=AsyncMock(), reject=AsyncMock())


async def test_runner_unrouted_message_reject_below_threshold() -> None:
    server = InMemoryServer()
    runner = _Runner(actor_context=_make_actor_context(server), max_unrouted_retries=3)

    for _ in range(2):
        msg = _make_unrouted_message("msg-poison")
        await runner._message_handler([], msg)
        msg.reject.assert_called_once()
        msg.nack.assert_not_called()


@pytest.mark.parametrize(
    "limit",
    [1, 3],
    ids=["first_attempt_threshold", "third_attempt_threshold"],
)
async def test_runner_unrouted_message_nack_at_threshold(
    caplog: pytest.LogCaptureFixture,
    limit: int,
) -> None:
    server = InMemoryServer()
    runner = _Runner(actor_context=_make_actor_context(server), max_unrouted_retries=limit)

    for _ in range(limit - 1):
        await runner._message_handler([], _make_unrouted_message("msg-poison"))

    msg = _make_unrouted_message("msg-poison")
    await runner._message_handler([], msg)
    msg.nack.assert_called_once()
    msg.reject.assert_not_called()

    error_log = next(
        (r for r in caplog.get_records(when="call") if r.levelno == logging.ERROR),
        None,
    )
    assert error_log is not None
    assert error_log.message == "actor.route.poison_message"


async def test_runner_unrouted_message_counter_cleared_after_nack() -> None:
    server = InMemoryServer()
    runner = _Runner(actor_context=_make_actor_context(server), max_unrouted_retries=2)

    for _ in range(2):
        await runner._message_handler([], _make_unrouted_message("msg-poison"))

    assert "msg-poison" not in runner._unrouted_seen_counts


async def test_runner_unrouted_message_counts_are_channel_scoped() -> None:
    server = InMemoryServer()
    runner = _Runner(actor_context=_make_actor_context(server), max_unrouted_retries=2)

    for _ in range(2):
        msg = _make_unrouted_message("msg-poison", channel="first")
        await runner._message_handler([], msg)
    # Second sighting on "first" hits the threshold and clears the entry.
    assert runner._unrouted_seen_counts == {}

    # The same id on another channel starts a fresh count.
    msg = _make_unrouted_message("msg-poison", channel="second")
    await runner._message_handler([], msg)
    msg.reject.assert_called_once()
    msg.nack.assert_not_called()
    assert runner._unrouted_seen_counts == {("second", "msg-poison"): 1}


async def test_runner_unrouted_message_no_id_always_reject() -> None:
    server = InMemoryServer()
    runner = _Runner(actor_context=_make_actor_context(server), max_unrouted_retries=1)

    for _ in range(3):
        msg = _make_unrouted_message(None)
        await runner._message_handler([], msg)
        msg.reject.assert_called_once()
        msg.nack.assert_not_called()


async def test_keep_alive_loop_calls_keep_alive() -> None:
    renewed = asyncio.Event()
    message = Mock(keep_alive=AsyncMock(side_effect=renewed.set))

    renewal = _Delivery(message, _Budget(Concurrency()))
    renewal.renew_at(0.001)
    await asyncio.wait_for(renewed.wait(), 1)
    await renewal.finish()

    message.keep_alive.assert_awaited()


async def test_keep_alive_skips_renewal_when_paused_as_deadline_expires() -> None:
    message = Mock(keep_alive=AsyncMock())
    renewal = _Delivery(message, _Budget(Concurrency()))
    renewal.renew_at(0.01)
    await asyncio.sleep(0)
    deadline = _lease(renewal)._deadline
    assert deadline is not None
    # Pause in the same loop iteration that the renewal's timeout fires.
    asyncio.get_running_loop().call_at(deadline, renewal.renew_at, None)
    await asyncio.sleep(0.05)
    await renewal.finish()

    message.keep_alive.assert_not_awaited()


async def test_keep_alive_interval_can_change_and_pause_without_restarting() -> None:
    renewed = asyncio.Event()
    message = Mock(keep_alive=AsyncMock(side_effect=renewed.set))
    renewal = _Delivery(message, _Budget(Concurrency()))
    renewal.renew_at(3600)
    lease = _lease(renewal)
    assert lease._timer is not None
    assert lease._task is None
    try:
        renewal.renew_at(0.001)
        await asyncio.wait_for(renewed.wait(), 1)

        renewal.renew_at(None)
        calls = message.keep_alive.await_count
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert message.keep_alive.await_count == calls
        assert lease._timer is None
        assert lease._task is None

        renewed.clear()
        renewal.renew_at(0.001)
        await asyncio.wait_for(renewed.wait(), 1)
        assert _lease(renewal) is lease
    finally:
        await renewal.finish()
    assert lease._timer is None
    assert lease._task is None or lease._task.done()


async def test_keep_alive_reschedule_during_renewal_waits_for_it() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def keep_alive() -> None:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()

    message = Mock(keep_alive=AsyncMock(side_effect=keep_alive))
    renewal = _Delivery(message, _Budget(Concurrency()))
    renewal.renew_at(0.001)
    lease = _lease(renewal)
    try:
        await asyncio.wait_for(started.wait(), 1)
        # A schedule change while renewing must not start a second renewal.
        renewal.renew_at(3600)
        assert lease._timer is None
        release.set()
        async with asyncio.timeout(1):
            while lease._task is not None:
                await asyncio.sleep(0)
        assert lease._timer is not None
        assert calls == 1
    finally:
        await renewal.finish()


async def test_keep_alive_loop_exits_when_adapter_stops_renewal() -> None:
    message = Mock(keep_alive=AsyncMock(return_value=False))

    renewal = _Delivery(message, _Budget(Concurrency()))
    renewal.renew_at(0.001)
    lease = _lease(renewal)
    async with asyncio.timeout(1):
        while lease._task is not None or not lease._closed:
            await asyncio.sleep(0.001)
    renewal.renew_at(1)
    assert lease._timer is None
    await renewal.finish()

    message.keep_alive.assert_awaited_once()


async def test_keep_alive_loop_logs_error_and_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = Mock(
        keep_alive=AsyncMock(side_effect=[RuntimeError("broker error"), False]),
        message_id="test-msg",
    )
    renewal = _Delivery(message, _Budget(Concurrency()))
    renewal.renew_at(0.001)
    lease = _lease(renewal)
    async with asyncio.timeout(1):
        while lease._task is not None or not lease._closed:
            await asyncio.sleep(0.001)

    assert message.keep_alive.await_count == 2
    warning = next(
        (r for r in caplog.records if r.message == "message.keep_alive.error"),
        None,
    )
    assert warning is not None
    assert warning.levelno == logging.WARNING


def _make_mock_actor(fn: AsyncMock | None = None) -> Mock:
    async def middleware(leaf: AsyncMock, message: Mock, actor: Mock) -> object:
        return await leaf(message, actor)

    return Mock(
        keep_alive=True,
        timeout=None,
        confirmation_mode="manual",
        on_error="nack",
        middleware_pipeline=middleware,
        converter=Mock(convert_inputs=AsyncMock(return_value=([], {}))),
        fn=fn if fn is not None else AsyncMock(),
    )


def _make_actor_context(server: Mock | ServerT) -> ActorExecutionContext:
    return ActorExecutionContext(
        server=server,
        publish=AsyncMock(),
        default_serializer=default_serializer,
    )


@pytest.mark.parametrize(
    ("supported", "interval", "enabled"),
    [(False, 0, True), (True, None, True), (True, 0, False)],
    ids=["broker_renewal_unsupported", "broker_interval_missing", "actor_renewal_disabled"],
)
async def test_actor_skips_keep_alive(
    supported: bool,
    interval: int | None,
    enabled: bool,
) -> None:
    message = Mock(is_acted_on=False, keep_alive_interval=interval, keep_alive=AsyncMock())
    server = Mock(capabilities={"supports_keep_alive": supported})
    actor = _make_mock_actor()
    actor.keep_alive = enabled

    async def slow_fn() -> None:
        await asyncio.sleep(0.01)

    actor.fn.side_effect = slow_fn
    await _actor_run(actor, message, _make_actor_context(server))

    message.keep_alive.assert_not_called()


async def test_keep_alive_runs_during_slow_actor() -> None:
    message = Mock(
        keep_alive_interval=0.01,
        is_acted_on=False,
        keep_alive=AsyncMock(),
        message_id="test-msg",
    )
    server = Mock(capabilities={"supports_keep_alive": True})

    async def slow_fn() -> None:
        await asyncio.sleep(0.1)

    await _actor_run(
        _make_mock_actor(AsyncMock(side_effect=slow_fn)),
        message,
        _make_actor_context(server),
    )

    assert message.keep_alive.call_count >= 1


async def test_keep_alive_stops_once_leaf_settles_while_middleware_continues() -> None:
    message = Mock(
        keep_alive_interval=0.01,
        is_acted_on=False,
        keep_alive=AsyncMock(return_value=None),
        message_id="test-msg",
    )

    async def ack() -> None:
        message.is_acted_on = True

    message.ack = AsyncMock(side_effect=ack)
    server = Mock(capabilities={"supports_keep_alive": True})
    actor = _make_mock_actor()
    actor.confirmation_mode = "auto"
    settled_calls: list[int] = []

    async def middleware(leaf: AsyncMock, msg: Mock, act: Mock) -> object:
        result = await leaf(msg, act)
        settled_calls.append(message.keep_alive.await_count)
        # An adapter that never reports settlement must not be renewed here.
        await asyncio.sleep(0.05)
        return result

    actor.middleware_pipeline = middleware
    await _actor_run(actor, message, _make_actor_context(server))

    message.ack.assert_awaited_once()
    assert message.keep_alive.await_count == settled_calls[0]


async def test_keep_alive_continues_when_leaf_settlement_fails() -> None:
    renewed = asyncio.Event()
    message = Mock(
        keep_alive_interval=0.01,
        is_acted_on=False,
        keep_alive=AsyncMock(side_effect=renewed.set),
        message_id="test-msg",
        ack=AsyncMock(side_effect=RuntimeError("ack failed")),
    )
    server = Mock(capabilities={"supports_keep_alive": True})
    actor = _make_mock_actor()
    actor.confirmation_mode = "auto"

    async def middleware(leaf: AsyncMock, msg: Mock, act: Mock) -> object:
        with pytest.raises(RuntimeError, match="ack failed"):
            await leaf(msg, act)
        renewed.clear()
        # The delivery is still unsettled, so its lease keeps being renewed.
        await asyncio.wait_for(renewed.wait(), 1)
        return None

    actor.middleware_pipeline = middleware
    await _actor_run(actor, message, _make_actor_context(server))


@pytest.mark.parametrize("interval", [0, -1.0], ids=["zero", "negative"])
async def test_non_positive_keep_alive_interval_disables_renewal(interval: float) -> None:
    message = Mock(
        keep_alive_interval=interval,
        is_acted_on=False,
        keep_alive=AsyncMock(),
        message_id="test-msg",
    )
    server = Mock(capabilities={"supports_keep_alive": True})

    async def slow_fn() -> None:
        await asyncio.sleep(0.05)

    await _actor_run(
        _make_mock_actor(AsyncMock(side_effect=slow_fn)),
        message,
        _make_actor_context(server),
    )

    message.keep_alive.assert_not_called()


async def test_keep_alive_task_cancelled_on_actor_timeout() -> None:
    message = Mock(
        keep_alive_interval=0.01,
        is_acted_on=False,
        keep_alive=AsyncMock(),
        message_id="test-msg",
        nack=AsyncMock(),
    )
    server = Mock(capabilities={"supports_keep_alive": True})

    async def very_slow_fn() -> None:
        await asyncio.sleep(10)

    actor = _make_mock_actor(AsyncMock(side_effect=very_slow_fn))
    actor.timeout = 0.05
    result = await _actor_run(actor, message, _make_actor_context(server))
    assert isinstance(result, TimeoutError)
    message.nack.assert_awaited_once()

    call_count_after_timeout = message.keep_alive.call_count
    await asyncio.sleep(0.05)
    assert message.keep_alive.call_count == call_count_after_timeout


async def test_actor_keep_alive_propagates_cancellation_during_cleanup() -> None:
    keep_alive_started = asyncio.Event()
    cleaning = asyncio.Event()

    async def keep_alive() -> None:
        keep_alive_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await asyncio.Event().wait()

    message = Mock(is_acted_on=False, keep_alive_interval=0.01, keep_alive=keep_alive)
    actor = _make_mock_actor(AsyncMock(side_effect=keep_alive_started.wait))
    server = Mock(capabilities={"supports_keep_alive": True})
    running = asyncio.create_task(_actor_run(actor, message, _make_actor_context(server)))
    await asyncio.wait_for(cleaning.wait(), 1)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running


@pytest.mark.parametrize("settlement", ["ack", "nack"], ids=["ack_message", "nack_message"])
async def test_keep_alive_runs_during_middleware_and_settlement(settlement: str) -> None:
    keep_alive_before_actor = asyncio.Event()
    keep_alive_during_settlement = asyncio.Event()
    settling = asyncio.Event()

    def record_keep_alive() -> None:
        if settling.is_set():
            keep_alive_during_settlement.set()
        else:
            keep_alive_before_actor.set()

    async def middleware(leaf: AsyncMock, message: Mock, actor: Mock) -> object:
        await asyncio.wait_for(keep_alive_before_actor.wait(), 0.5)
        return await leaf(message, actor)

    async def settle() -> None:
        settling.set()
        await asyncio.wait_for(keep_alive_during_settlement.wait(), 0.5)

    actor = _make_mock_actor()
    actor.confirmation_mode = "auto"
    actor.keep_alive = 0.001
    actor.middleware_pipeline = middleware
    if settlement == "nack":
        actor.fn.side_effect = ValueError("actor failed")
    message = Mock(
        is_acted_on=False,
        keep_alive_interval=None,
        keep_alive=AsyncMock(side_effect=record_keep_alive),
    )
    setattr(message, settlement, AsyncMock(side_effect=settle))
    server = Mock(capabilities={"supports_keep_alive": True})
    await _actor_run(actor, message, _make_actor_context(server))
    assert keep_alive_before_actor.is_set()
    assert keep_alive_during_settlement.is_set()
    getattr(message, settlement).assert_awaited_once()


async def test_actor_failure_after_ack_does_not_settle_again() -> None:
    message = Mock(is_acted_on=False, ack=AsyncMock(), nack=AsyncMock(), reject=AsyncMock())
    error = RuntimeError("actor failed after acknowledging")

    def acknowledge() -> None:
        message.is_acted_on = True

    async def fail_after_ack() -> None:
        await message.ack()
        raise error

    message.ack.side_effect = acknowledge
    actor = _make_mock_actor(AsyncMock(side_effect=fail_after_ack))
    actor.keep_alive = False
    actor.confirmation_mode = "auto"

    result = await _actor_run(actor, message, _make_actor_context(InMemoryServer()))

    assert result is error
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()
