from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator
from unittest import mock

import pytest

from repid import Connection, Queue
from repid._processor import _Processor
from repid.connections.abc import BucketBrokerT, ConsumerT, MessageBrokerT
from repid.main import Repid
from repid.middlewares import WRAPPED, Middleware
from repid.middlewares.wrapper import (
    IsInsideMiddleware,
    _middleware_wrapper,
    middleware_wrapper,
)

if TYPE_CHECKING:
    from repid.data.protocols import ParametersT, RoutingKeyT


class RecursiveConnection(MessageBrokerT):
    """queue_delete calls queue_flush. Middleware should only be called once."""

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


@pytest.fixture()
async def dummy_recursive_connection() -> AsyncIterator[Connection]:
    repid_app = Repid(Connection(RecursiveConnection()))
    async with repid_app.magic(auto_disconnect=True) as conn:
        yield conn


async def test_add_middleware(dummy_recursive_connection: Connection) -> None:
    counter = 0

    class TestMiddleware:
        @staticmethod
        async def before_queue_delete(queue_name: str) -> None:  # noqa: ARG004
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
        async def before_queue_flush(self, queue_name: str) -> None:  # noqa: ARG002
            raise Exception("This should not be called")

        async def after_queue_flush(self) -> None:
            raise Exception("This should not be called")

        @staticmethod
        async def before_queue_delete(queue_name: str) -> None:  # noqa: ARG004
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
        async def before_queue_flush(self, queue_name: str) -> None:  # noqa: ARG002
            raise Exception("Some random exception")

    dummy_recursive_connection.middleware.add_middleware(TestMiddleware())
    await Queue("test_queue_name").flush()
    assert any(
        (
            all(
                (
                    "ERROR" in x,
                    "Subscriber 'before_queue_flush'" in x,
                ),
            )
            for x in caplog.text.splitlines()
        ),
    )


async def test_add_subscriber(caplog: pytest.LogCaptureFixture) -> None:
    middleware = Middleware()

    # Test that an async function is properly added as a subscriber
    # if it is named according to one of the signal names
    async def before_consume(a: int, b: int) -> int:
        return a + b

    middleware.add_subscriber(before_consume)
    assert before_consume.__name__ in middleware.subscribers

    # Test that a non-async function is properly added as a subscriber
    # if it is named according to one of the signal names
    def after_enqueue(a: int, b: int) -> int:
        return a + b

    middleware.add_subscriber(after_enqueue)
    assert before_consume.__name__ in middleware.subscribers
    assert after_enqueue.__name__ in middleware.subscribers

    # Test that a function with no corresponding signal is not added as a subscriber
    def foo(a: int, b: int) -> int:
        return a + b

    middleware.add_subscriber(foo)
    assert foo.__name__ not in middleware.subscribers

    assert any(
        (
            all(
                (
                    "WARNING" in x,
                    "Function 'foo' wasn't subscribed, as there is no corresponding signal." in x,
                ),
            )
            for x in caplog.text.splitlines()
        ),
    )


def test_middleware_wrapper() -> None:
    @middleware_wrapper
    async def foo(a: int, b: int) -> int:
        return a + b

    # Check that the middleware_wrapper decorator adds the expected attributes to the function
    assert foo.fn == foo.__wrapped__  # type: ignore[attr-defined]
    assert foo.name == "foo"
    assert list(foo.parameters) == ["a", "b"]
    assert foo._repid_signal_emitter is None

    # Check that the middleware_wrapper decorator correctly sets the name argument
    @middleware_wrapper(name="bar")
    async def foo2(a: int, b: int) -> int:
        return a + b

    assert foo2.name == "bar"

    # Test that the middleware_wrapper decorator can be used without brackets
    @middleware_wrapper
    async def foo3(a: int, b: int) -> int:
        return a + b

    assert foo3.name == "foo3"

    # Test that the middleware_wrapper decorator can be used with brackets
    @middleware_wrapper()
    async def foo4(a: int, b: int) -> int:
        return a + b

    assert foo4.name == "foo4"

    # Test that the middleware_wrapper decorator correctly sets the fn and parameters attributes
    @middleware_wrapper
    async def foo5(a: int, b: int, *, c: int = 0, d: int = 0) -> int:
        return a + b + c + d

    assert foo5.fn == foo5.__wrapped__  # type: ignore[attr-defined]
    assert list(foo5.parameters) == ["a", "b", "c", "d"]


async def test_middleware_wrapper_call() -> None:
    @middleware_wrapper
    async def foo(a: int, b: int) -> int:
        return a + b

    # Test that the middleware_wrapper decorator doesn't emit signals if IsInsideMiddleware is True
    foo._repid_signal_emitter = mock.AsyncMock()
    IsInsideMiddleware.set(True)
    result = await foo(1, 2)
    assert result == 3
    assert foo._repid_signal_emitter.call_count == 0

    # Test that the middleware_wrapper decorator properly emits signals if everything is set up
    foo._repid_signal_emitter = mock.AsyncMock()
    IsInsideMiddleware.set(False)
    result = await foo(1, 2)
    assert result == 3
    assert foo._repid_signal_emitter.call_count == 2
    foo._repid_signal_emitter.assert_called_with("after_foo", {"a": 1, "b": 2, "result": 3})
