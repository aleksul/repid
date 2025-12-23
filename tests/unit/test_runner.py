from __future__ import annotations

import asyncio
import logging
from unittest.mock import Mock

import pytest

from repid import Repid, Router
from repid._runner import _Runner
from repid.connections.in_memory import InMemoryServer
from repid.data import MessageData
from repid.health_check_server import HealthCheckServer, HealthCheckStatus
from repid.serializer import default_serializer
from repid.test_client import TestClient


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
    assert warning_log.message == (
        "Actor 'manual_actor' is in 'manual' confirmation mode, but the message is not acknowledged."
    )


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


@pytest.mark.parametrize("timeout_value", [0, float("inf")])
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


@pytest.mark.parametrize("supports_lightweight_pause", [True, False])
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
            server=mocked_server,
            max_tasks=5,
            tasks_concurrency_limit=10,
            default_serializer=default_serializer,
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


async def test_runner_unpause_threshold_validation() -> None:
    server = InMemoryServer()

    with pytest.raises(ValueError, match="Subscriber will never unpause"):
        _Runner(
            server=server,
            max_tasks=10,
            tasks_concurrency_limit=1,
            concurrency_unpause_percent=2.0,  # 200% - more than limit
            default_serializer=default_serializer,
        )


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
            server=server,
            default_serializer=default_serializer,
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
            server=server,
            default_serializer=default_serializer,
        )

        await server.publish(
            channel="default",
            message=MessageData(
                payload=b"{}",
                headers={"topic": "nonexistent_actor"},
                content_type="application/json",
            ),
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
        assert "No actor found for message" in warning_log.message


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
            server=server,
            tasks_concurrency_limit=2,
            default_serializer=default_serializer,
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

    async def failing_subscribe(*args, **kwargs):  # type: ignore[no-untyped-def]  # noqa: ARG001
        return FailingSubscriber()

    server.subscribe = failing_subscribe  # type: ignore[method-assign]

    health_check_server = HealthCheckServer()
    router = Router()

    @router.actor()
    async def test_actor() -> None:
        pass

    async with server.connection():
        runner = _Runner(
            server=server,
            health_check_server=health_check_server,
            default_serializer=default_serializer,
        )

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
            server=server,
            default_serializer=default_serializer,
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
                if r.levelno == logging.ERROR and "timeouted" in r.message
            ),
            None,
        )
        assert error_log is not None


async def test_runner_pause_exception_during_shutdown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = InMemoryServer()

    class FailingPauseSubscriber:
        def __init__(self, original_subscriber) -> None:  # type: ignore[no-untyped-def]
            self.original_subscriber = original_subscriber
            self.task = original_subscriber.task

        async def pause(self) -> None:
            raise RuntimeError("Pause failed")

        async def resume(self) -> None:
            await self.original_subscriber.resume()

        async def close(self) -> None:
            await self.original_subscriber.close()

    original_subscribe = server.subscribe

    async def failing_pause_subscribe(*args, **kwargs):  # type: ignore[no-untyped-def]
        original = await original_subscribe(*args, **kwargs)
        return FailingPauseSubscriber(original)

    server.subscribe = failing_pause_subscribe  # type: ignore[method-assign]

    router = Router()

    @router.actor()
    async def test_actor() -> None:
        pass

    async with server.connection():
        runner = _Runner(
            server=server,
            default_serializer=default_serializer,
        )

        runner.stop_consume_event.set()

        await runner.run(
            channels_to_actors=router._actors_per_channel_address,
            graceful_termination_timeout=0.1,
        )

        exception_log = next(
            (
                r
                for r in caplog.get_records(when="call")
                if "Error while pausing subscriber" in r.message
            ),
            None,
        )
        assert exception_log is not None


async def test_runner_close_exception_during_shutdown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = InMemoryServer()

    class FailingCloseSubscriber:
        def __init__(self, original_subscriber) -> None:  # type: ignore[no-untyped-def]
            self.original_subscriber = original_subscriber
            self.task = original_subscriber.task

        async def pause(self) -> None:
            await self.original_subscriber.pause()

        async def resume(self) -> None:
            await self.original_subscriber.resume()

        async def close(self) -> None:
            raise RuntimeError("Close failed")

    original_subscribe = server.subscribe

    async def failing_close_subscribe(*args, **kwargs):  # type: ignore[no-untyped-def]
        original = await original_subscribe(*args, **kwargs)
        return FailingCloseSubscriber(original)

    server.subscribe = failing_close_subscribe  # type: ignore[method-assign]

    router = Router()

    @router.actor()
    async def test_actor() -> None:
        pass

    async with server.connection():
        runner = _Runner(
            server=server,
            default_serializer=default_serializer,
        )

        runner.stop_consume_event.set()

        await runner.run(
            channels_to_actors=router._actors_per_channel_address,
            graceful_termination_timeout=0.1,
        )

        exception_log = next(
            (
                r
                for r in caplog.get_records(when="call")
                if "Error while closing subscriber" in r.message
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

    @router.actor()
    async def stubborn_actor() -> None:
        processing_event.set()
        while True:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                await asyncio.sleep(10)

    async with server.connection():
        runner = _Runner(
            server=server,
            default_serializer=default_serializer,
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

        error_log = next(
            (
                r
                for r in caplog.get_records(when="call")
                if r.levelno == logging.ERROR
                and "did not finish even after cancellation" in r.message
            ),
            None,
        )
        assert error_log is not None
