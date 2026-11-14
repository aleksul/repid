from __future__ import annotations

import asyncio
import signal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import httpx2
import pytest

from repid import Router
from repid._runner import _Runner
from repid._worker import _Worker
from repid.asyncapi import AsyncAPI3Schema
from repid.asyncapi_server import AsyncAPIServerSettings
from repid.connections.in_memory import InMemoryServer
from repid.data import ActorExecutionContext, MessageData
from repid.health_check_server import HealthCheckServerSettings
from repid.serializer import default_serializer


def _make_actor_context(server: InMemoryServer) -> ActorExecutionContext:
    async def publish(
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        await server.publish(
            channel=channel,
            message=message,
            server_specific_parameters=server_specific_parameters,
        )

    return ActorExecutionContext(
        server=server,
        publish=publish,
        default_serializer=default_serializer,
    )


async def test_worker_with_no_actors() -> None:
    router = Router()
    server = InMemoryServer()

    async with server.connection():
        worker = _Worker(
            actor_context=_make_actor_context(server),
            router=router._materialize(),
            graceful_shutdown_time=1.0,
        )

        runner = await worker.run()

        # Worker should exit immediately with no actors
        assert runner.processed == 0


async def test_worker_without_asyncapi_schema_raises() -> None:
    router = Router()
    server = InMemoryServer()

    with pytest.raises(ValueError, match="AsyncAPI schema is required"):
        _Worker(
            actor_context=_make_actor_context(server),
            router=router._materialize(),
            asyncapi_server=AsyncAPIServerSettings(address="127.0.0.1", port=18125),
            asyncapi_schema=None,
        )


async def test_worker_run_with_health_check_server_lifecycle() -> None:
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    server = InMemoryServer()

    async with server.connection():
        worker = _Worker(
            actor_context=_make_actor_context(server),
            router=router._materialize(),
            graceful_shutdown_time=0.1,
            messages_limit=1,
            health_check_server=HealthCheckServerSettings(address="127.0.0.1", port=18126),
            register_signals=[],
        )

        task = asyncio.create_task(worker.run())

        async with httpx2.AsyncClient() as client:
            resposne = await client.get("http://localhost:18126/healthz")
            assert resposne.status_code == 200

        await server.publish(
            channel="default",
            message=MessageData(
                payload=b"",
                headers={"topic": "test_actor"},
                content_type="application/json",
            ),
        )

        await task


async def test_worker_run_with_asyncapi_server_lifecycle() -> None:
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    server = InMemoryServer()

    schema: AsyncAPI3Schema = {
        "asyncapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0.0"},
        "channels": {},
        "operations": {},
        "components": {"messages": {}},
    }

    async with server.connection():
        worker = _Worker(
            actor_context=_make_actor_context(server),
            router=router._materialize(),
            graceful_shutdown_time=0.1,
            messages_limit=1,
            asyncapi_server=AsyncAPIServerSettings(address="127.0.0.1", port=18127),
            asyncapi_schema=schema,
            register_signals=[],
        )

        task = asyncio.create_task(worker.run())

        async with httpx2.AsyncClient() as client:
            response = await client.get("http://localhost:18127")
            assert response.status_code == 200
            assert response.content is not None

        await server.publish(
            channel="default",
            message=MessageData(
                payload=b"",
                headers={"topic": "test_actor"},
                content_type="application/json",
            ),
        )

        await task


async def test_worker_run_with_both_servers_lifecycle() -> None:
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    server = InMemoryServer()

    schema: AsyncAPI3Schema = {
        "asyncapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0.0"},
        "channels": {},
        "operations": {},
        "components": {"messages": {}},
    }

    async with server.connection():
        worker = _Worker(
            actor_context=_make_actor_context(server),
            router=router._materialize(),
            graceful_shutdown_time=0.1,
            messages_limit=1,
            health_check_server=HealthCheckServerSettings(address="127.0.0.1", port=18128),
            asyncapi_server=AsyncAPIServerSettings(address="127.0.0.1", port=18129),
            asyncapi_schema=schema,
            register_signals=[],
        )

        task = asyncio.create_task(worker.run())

        async with httpx2.AsyncClient() as client:
            health_response = await client.get("http://localhost:18128/healthz")
            assert health_response.status_code == 200

            asyncapi_response = await client.get("http://localhost:18129")
            assert asyncapi_response.status_code == 200
            assert asyncapi_response.content is not None

        await server.publish(
            channel="default",
            message=MessageData(
                payload=b"",
                headers={"topic": "test_actor"},
                content_type="application/json",
            ),
        )

        await task


async def test_worker_run_graceful_shutdown() -> None:
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    server = InMemoryServer()

    async with server.connection():
        worker = _Worker(
            actor_context=_make_actor_context(server),
            router=router._materialize(),
            graceful_shutdown_time=0.1,
            register_signals=[signal.SIGUSR1],
        )

        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.1)  # wait for the worker to start

        signal.raise_signal(signal.SIGUSR1)

        await asyncio.wait_for(task, timeout=3.0)


async def test_worker_run_cancel() -> None:
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    server = InMemoryServer()

    async with server.connection():
        worker = _Worker(
            actor_context=_make_actor_context(server),
            router=router._materialize(),
            graceful_shutdown_time=0.1,
            register_signals=[signal.SIGUSR1],
        )

        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.1)  # wait for the worker to start

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3.0)


async def test_worker_cleanup_stops_asyncapi_when_health_stop_fails() -> None:
    server = InMemoryServer()
    worker = _Worker(
        actor_context=_make_actor_context(server),
        router=Router()._materialize(),
        register_signals=[],
    )
    health = MagicMock(start=AsyncMock(), stop=AsyncMock(side_effect=RuntimeError("health failed")))
    asyncapi = MagicMock(start=AsyncMock(), stop=AsyncMock())
    worker.health_check_server = cast(Any, health)
    worker.asyncapi_server = cast(Any, asyncapi)

    with pytest.raises(RuntimeError, match="health failed"):
        await worker.run()

    health.start.assert_awaited_once()
    health.stop.assert_awaited_once()
    asyncapi.start.assert_awaited_once()
    asyncapi.stop.assert_awaited_once()


async def test_worker_cleanup_stops_asyncapi_when_health_stop_times_out() -> None:
    server = InMemoryServer()
    worker = _Worker(
        actor_context=_make_actor_context(server),
        router=Router()._materialize(),
        register_signals=[],
    )

    async def blocked_stop() -> None:
        await asyncio.Future()

    health = MagicMock(start=AsyncMock(), stop=AsyncMock(side_effect=blocked_stop))
    asyncapi = MagicMock(start=AsyncMock(), stop=AsyncMock())
    worker.health_check_server = cast(Any, health)
    worker.asyncapi_server = cast(Any, asyncapi)
    worker.graceful_health_check_server_finish_time = 0.01

    with pytest.raises(asyncio.TimeoutError):
        await worker.run()

    health.stop.assert_awaited_once()
    asyncapi.stop.assert_awaited_once()


async def test_worker_cleanup_stops_servers_when_signal_unregistration_fails() -> None:
    router = Router()

    @router.actor
    async def actor() -> None:
        pass

    worker = _Worker(
        actor_context=_make_actor_context(InMemoryServer()),
        router=router._materialize(),
        register_signals=[signal.SIGUSR1],
    )
    health = MagicMock(start=AsyncMock(), stop=AsyncMock())
    asyncapi = MagicMock(start=AsyncMock(), stop=AsyncMock())
    worker.health_check_server = cast(Any, health)
    worker.asyncapi_server = cast(Any, asyncapi)
    loop = asyncio.get_running_loop()
    registered: list[signal.Signals] = []

    def add_signal_handler(sig: signal.Signals, _callback: Any) -> None:
        registered.append(sig)

    def remove_signal_handler(sig: signal.Signals) -> bool:
        assert sig in registered
        raise RuntimeError("unregister failed")

    with (
        patch.object(loop, "add_signal_handler", side_effect=add_signal_handler),
        patch.object(loop, "remove_signal_handler", side_effect=remove_signal_handler),
        patch.object(_Runner, "run", new=AsyncMock()),
        pytest.raises(RuntimeError, match="unregister failed"),
    ):
        await worker.run()

    assert registered == [signal.SIGUSR1]
    health.stop.assert_awaited_once()
    asyncapi.stop.assert_awaited_once()


def test_worker_unregister_signals_logs_subsequent_errors() -> None:
    worker = _Worker(
        actor_context=_make_actor_context(InMemoryServer()),
        router=Router()._materialize(),
        register_signals=[signal.SIGUSR1, signal.SIGUSR2],
    )
    first_error = RuntimeError("first failure")
    second_error = RuntimeError("second failure")
    loop = MagicMock(spec=asyncio.AbstractEventLoop)
    loop.remove_signal_handler.side_effect = [first_error, second_error]

    with (
        patch("repid._worker.logger.exception") as log_exception,
        pytest.raises(RuntimeError) as raised,
    ):
        worker._unregister_signals(loop)

    assert raised.value is first_error
    assert loop.remove_signal_handler.call_count == 2
    log_exception.assert_called_once_with(
        "worker.signal.unregister.error",
        exc_info=second_error,
    )


async def test_worker_rolls_back_partially_registered_signals() -> None:
    router = Router()

    @router.actor
    async def actor() -> None:
        pass

    worker = _Worker(
        actor_context=_make_actor_context(InMemoryServer()),
        router=router._materialize(),
        register_signals=[signal.SIGUSR1, signal.SIGUSR2],
    )
    health = MagicMock(start=AsyncMock(), stop=AsyncMock())
    asyncapi = MagicMock(start=AsyncMock(), stop=AsyncMock())
    worker.health_check_server = cast(Any, health)
    worker.asyncapi_server = cast(Any, asyncapi)
    loop = asyncio.get_running_loop()
    registered: list[signal.Signals] = []
    removed: list[signal.Signals] = []

    def add_signal_handler(sig: signal.Signals, _callback: Any) -> None:
        if registered:
            raise RuntimeError("register failed")
        registered.append(sig)

    def remove_signal_handler(sig: signal.Signals) -> bool:
        removed.append(sig)
        return True

    with (
        patch.object(loop, "add_signal_handler", side_effect=add_signal_handler),
        patch.object(loop, "remove_signal_handler", side_effect=remove_signal_handler),
        pytest.raises(RuntimeError, match="register failed"),
    ):
        await worker.run()

    assert removed == registered
    health.stop.assert_awaited_once()
    asyncapi.stop.assert_awaited_once()
