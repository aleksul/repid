from __future__ import annotations

import asyncio
from typing import AsyncIterator

import pytest

from repid.connections.abc import ConsumerT
from repid.data import ParametersT, RoutingKeyT


async def fake_emitter(_a: str, _b: dict) -> None:
    return None


class ConcreteConsumer(ConsumerT):
    def __init__(self) -> None:
        self._is_consuming = False
        self._messages: list[tuple[RoutingKeyT, str, ParametersT]] = []

    async def start(self) -> None:
        self._is_consuming = True

    async def pause(self) -> None:
        self._is_consuming = False

    async def unpause(self) -> None:
        self._is_consuming = True

    async def finish(self) -> None:
        self._is_consuming = False
        self._messages = []

    async def consume(self) -> tuple[RoutingKeyT, str, ParametersT]:
        if not self._is_consuming:
            raise ValueError("Consumer is not started")
        while not self._messages:
            await asyncio.sleep(0.1)
        return self._messages.pop(0)

    @property
    def is_consuming(self) -> bool:
        return self._is_consuming


@pytest.fixture()
async def consumer() -> AsyncIterator[ConcreteConsumer]:
    c = ConcreteConsumer()
    c._messages.append(("", "", ""))  # type: ignore[arg-type]
    yield c
    await c.finish()


async def test_consumer_start(consumer: ConcreteConsumer) -> None:
    await consumer.start()
    assert consumer.is_consuming is True


async def test_consumer_pause(consumer: ConcreteConsumer) -> None:
    await consumer.start()
    await consumer.pause()
    assert consumer.is_consuming is False


async def test_consumer_unpause(consumer: ConcreteConsumer) -> None:
    await consumer.start()
    await consumer.pause()
    await consumer.unpause()
    assert consumer.is_consuming is True


async def test_consumer_finish(consumer: ConcreteConsumer) -> None:
    await consumer.start()
    await consumer.finish()
    assert consumer.is_consuming is False


async def test_consumer_consume(consumer: ConcreteConsumer) -> None:
    await consumer.start()
    message = await consumer.consume()
    assert message is not None


async def test_consumer_enter(consumer: ConcreteConsumer) -> None:
    async with consumer as c:
        assert c.is_consuming is True  # type: ignore[attr-defined]


async def test_consumer_exit(consumer: ConcreteConsumer) -> None:
    async with consumer:
        pass
    assert consumer.is_consuming is False


async def test_consumer_iter(consumer: ConcreteConsumer) -> None:
    await consumer.start()
    async for message in consumer:
        assert message is not None
        break


def test_consumer_emitter(consumer: ConcreteConsumer) -> None:
    assert consumer._signal_emitter is None
    consumer._signal_emitter = fake_emitter
    assert consumer._signal_emitter is fake_emitter
