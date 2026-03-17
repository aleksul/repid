from __future__ import annotations

import asyncio
import signal

import httpx
import pytest

from repid import Router
from repid._worker import _Worker
from repid.asyncapi import AsyncAPI3Schema
from repid.asyncapi_server import AsyncAPIServerSettings
from repid.connections.in_memory import InMemoryServer
from repid.data import MessageData
from repid.health_check_server import HealthCheckServerSettings


async def test_worker_with_no_actors() -> None:
    router = Router()
    server = InMemoryServer()

    async with server.connection():
        worker = _Worker(
            server=server,
            router=router,
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
            server=server,
            router=router,
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
            server=server,
            router=router,
            graceful_shutdown_time=0.1,
            messages_limit=1,
            health_check_server=HealthCheckServerSettings(address="127.0.0.1", port=18126),
            register_signals=[],
        )

        task = asyncio.create_task(worker.run())

        async with httpx.AsyncClient() as client:
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
            server=server,
            router=router,
            graceful_shutdown_time=0.1,
            messages_limit=1,
            asyncapi_server=AsyncAPIServerSettings(address="127.0.0.1", port=18127),
            asyncapi_schema=schema,
            register_signals=[],
        )

        task = asyncio.create_task(worker.run())

        async with httpx.AsyncClient() as client:
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
            server=server,
            router=router,
            graceful_shutdown_time=0.1,
            messages_limit=1,
            health_check_server=HealthCheckServerSettings(address="127.0.0.1", port=18128),
            asyncapi_server=AsyncAPIServerSettings(address="127.0.0.1", port=18129),
            asyncapi_schema=schema,
            register_signals=[],
        )

        task = asyncio.create_task(worker.run())

        async with httpx.AsyncClient() as client:
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
            server=server,
            router=router,
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
            server=server,
            router=router,
            graceful_shutdown_time=0.1,
            register_signals=[signal.SIGUSR1],
        )

        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.1)  # wait for the worker to start

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3.0)
