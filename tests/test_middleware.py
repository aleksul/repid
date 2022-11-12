from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator

import pytest

from repid import Connection, Queue
from repid._processor import _Processor
from repid.connections.abc import BucketBrokerT, ConsumerT, MessageBrokerT
from repid.main import Repid
from repid.middlewares import WRAPPED
from repid.middlewares.wrapper import _middleware_wrapper

if TYPE_CHECKING:
    from repid.data.protocols import ParametersT, RoutingKeyT


@pytest.fixture()
async def dummy_recursive_connection() -> AsyncIterator[Connection]:
    class TestRecursiveConnection(MessageBrokerT):
        async def queue_flush(self, queue_name: str) -> None:
            pass

        async def queue_delete(self, queue_name: str) -> None:
            await self.queue_flush(queue_name)

        async def connect(self) -> None:
            pass

        async def disconnect(self) -> None:
            pass

        async def enqueue(
            self,
            key: RoutingKeyT,
            payload: str = "",
            params: ParametersT | None = None,
        ) -> None:
            raise NotImplementedError

        async def reject(self, key: RoutingKeyT) -> None:
            raise NotImplementedError

        async def ack(self, key: RoutingKeyT) -> None:
            raise NotImplementedError

        async def nack(self, key: RoutingKeyT) -> None:
            raise NotImplementedError

        async def requeue(
            self,
            key: RoutingKeyT,
            payload: str = "",
            params: ParametersT | None = None,
        ) -> None:
            raise NotImplementedError

        async def queue_declare(self, queue_name: str) -> None:
            raise NotImplementedError

    repid_app = Repid(Connection(TestRecursiveConnection()))
    async with repid_app.magic(auto_disconnect=True) as conn:
        yield conn


async def test_add_middleware(dummy_recursive_connection: Connection) -> None:
    counter = 0

    class TestMiddleware:
        @staticmethod
        async def before_queue_delete(queue_name: str) -> None:
            nonlocal counter
            counter += 1

    app = Repid(dummy_recursive_connection, middlewares=[TestMiddleware])
    async with app.magic(auto_disconnect=True):
        await Queue("test_queue_name").delete()

    assert counter == 1


def test_available_functions(fake_connection: Connection) -> None:
    for name in WRAPPED:
        assert name in (
            MessageBrokerT.__WRAPPED_METHODS__
            + BucketBrokerT.__WRAPPED_METHODS__
            + ConsumerT.__WRAPPED_METHODS__
            + ("actor_run",)
        )
    assert isinstance(_Processor(fake_connection).actor_run, _middleware_wrapper)


async def test_middleware_double_call(dummy_recursive_connection: Connection) -> None:
    counter = 0

    class TestMiddleware:
        async def before_queue_flush(self, queue_name: str) -> None:
            raise Exception("This should not be called")

        async def after_queue_flush(self) -> None:
            raise Exception("This should not be called")

        @staticmethod
        async def before_queue_delete(queue_name: str) -> None:
            nonlocal counter
            counter += 1

        async def after_queue_delete(self) -> None:
            nonlocal counter
            counter += 1

    dummy_recursive_connection.middleware.add_middleware(TestMiddleware())
    await Queue("test_queue_name").delete()
    assert counter == 2
    await Queue("test_another_queue_name").delete()
    assert counter == 4
    await Queue("test_queue_name").flush()
    assert counter == 4


async def test_error_in_middleware(
    caplog: pytest.LogCaptureFixture,
    dummy_recursive_connection: Connection,
) -> None:
    class TestMiddleware:
        async def before_queue_flush(self, queue_name: str) -> None:
            raise Exception("Some random exception")

    dummy_recursive_connection.middleware.add_middleware(TestMiddleware())
    await Queue("test_queue_name").flush()
    assert any(
        map(
            lambda x: all(
                (
                    "ERROR" in x,
                    "Subscriber 'before_queue_flush'" in x,
                )
            ),
            caplog.text.splitlines(),
        )
    )
