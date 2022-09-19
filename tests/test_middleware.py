from inspect import isfunction

import pytest

from repid import Connection, Queue, Repid
from repid.connections.abc import BucketBrokerT, MessageBrokerT
from repid.main import DEFAULT_CONNECTION
from repid.middlewares import AVAILABLE_FUNCTIONS, InjectMiddleware


@pytest.fixture()
def dummy_recursive_connection():
    @InjectMiddleware
    class DummyConnection:
        async def queue_flush(self, queue_name: str) -> None:
            pass

        async def queue_delete(self, queue_name: str) -> None:
            await self.queue_flush(queue_name)

    repid = Repid(Connection(DummyConnection()))
    contextvar_token = DEFAULT_CONNECTION.set(repid._conn)
    yield repid._conn
    DEFAULT_CONNECTION.reset(contextvar_token)


def test_available_functions():
    for name in dir(MessageBrokerT) + dir(BucketBrokerT):
        if name.startswith("__"):
            continue
        if name.endswith("connect"):
            continue
        if isfunction(getattr(MessageBrokerT, name, None) or getattr(BucketBrokerT, name, None)):
            assert name in AVAILABLE_FUNCTIONS


async def test_middleware_double_call(dummy_recursive_connection: Connection):
    counter = 0

    class TestMiddleware:
        async def before_queue_flush(self, queue_name: str):
            raise Exception("This should not be called")

        async def after_queue_flush(self):
            raise Exception("This should not be called")

        @staticmethod
        async def before_queue_delete(queue_name: str):
            nonlocal counter
            counter += 1

        async def after_queue_delete(self):
            nonlocal counter
            counter += 1

    dummy_recursive_connection.middleware.add_middleware(TestMiddleware())
    await Queue("test_queue_name").delete()
    assert counter == 2
    await Queue("test_another_queue_name").delete()
    assert counter == 4
    await Queue("test_queue_name").flush()
    assert counter == 4


async def test_error_in_middleware(caplog, dummy_recursive_connection: Connection):
    class TestMiddleware:
        async def before_queue_flush(self, queue_name: str):
            raise Exception("Some random exception")

    dummy_recursive_connection.middleware.add_middleware(TestMiddleware())
    await Queue("test_queue_name").flush()
    assert any(
        map(
            lambda x: all(
                (
                    "ERROR" in x,
                    "Event 'before_queue_flush'" in x,
                )
            ),
            caplog.text.splitlines(),
        )
    )
