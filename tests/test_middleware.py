import pytest

from repid import Queue, Repid
from repid.connection import CONNECTIONS_MAPPING
from repid.middlewares import AVAILABLE_FUNCTIONS, InjectMiddleware
from repid.protocols import Bucketing, Messaging


@pytest.fixture()
def dummy_recursive_connection():
    @InjectMiddleware
    class DummyConnection:
        def __init__(self, dsn: str) -> None:
            pass

        async def queue_flush(self, queue_name: str) -> None:
            pass

        async def queue_delete(self, queue_name: str) -> None:
            await self.queue_flush(queue_name)

    CONNECTIONS_MAPPING["test://"] = DummyConnection
    return DummyConnection


def test_available_functions():
    for name in dir(Messaging) + dir(Bucketing):
        if name.startswith("__"):
            continue
        if callable(getattr(Messaging, name, None) or getattr(Bucketing, name, None)):
            assert name in AVAILABLE_FUNCTIONS


async def test_middleware_double_call(dummy_recursive_connection):
    r = Repid("test://test")

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

    r.middleware.add_middleware(TestMiddleware())
    Repid("test://test")
    await Queue("test_queue_name").delete()
    assert counter == 2
    await Queue("test_another_queue_name").delete()
    assert counter == 4
    await Queue("test_queue_name").flush()
    assert counter == 4


async def test_error_in_middleware(caplog, dummy_recursive_connection):
    r = Repid("test://test")

    class TestMiddleware:
        async def before_queue_flush(self, queue_name: str):
            raise Exception("Some random exception")

    r.middleware.add_middleware(TestMiddleware())
    Repid("test://test")
    await Queue("test_queue_name").flush()
    assert any(
        map(
            lambda x: all(
                (
                    "ERROR" in x,
                    "Event before_queue_flush" in x,
                )
            ),
            caplog.text.splitlines(),
        )
    )
