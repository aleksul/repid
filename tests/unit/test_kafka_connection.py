from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from repid.connections.kafka import message_broker
from repid.connections.kafka.message_broker import KafkaServer


@patch.object(message_broker, "AIOKafkaProducer")
async def test_kafka_connect_failed_start_is_stopped_and_can_retry(
    mock_producer_cls: MagicMock,
) -> None:
    failed_producer = MagicMock(
        start=AsyncMock(side_effect=ConnectionError("offline")),
        stop=AsyncMock(),
    )
    connected_producer = MagicMock(start=AsyncMock(), stop=AsyncMock())
    mock_producer_cls.side_effect = [failed_producer, connected_producer]
    server = KafkaServer("localhost:9092")

    with pytest.raises(ConnectionError, match="offline"):
        await server.connect()

    assert not server.is_connected
    failed_producer.stop.assert_awaited_once()

    await server.connect()

    assert server.is_connected
    assert mock_producer_cls.call_count == 2
    connected_producer.start.assert_awaited_once()


@patch.object(message_broker, "AIOKafkaProducer")
async def test_kafka_connect_cancelled_start_is_stopped_and_not_published(
    mock_producer_cls: MagicMock,
) -> None:
    producer = MagicMock(start=AsyncMock(side_effect=asyncio.CancelledError), stop=AsyncMock())
    mock_producer_cls.return_value = producer
    server = KafkaServer("localhost:9092")

    with pytest.raises(asyncio.CancelledError):
        await server.connect()

    assert not server.is_connected
    producer.stop.assert_awaited_once()


@patch.object(message_broker, "AIOKafkaProducer")
async def test_kafka_connect_preserves_start_error_when_cleanup_fails(
    mock_producer_cls: MagicMock,
) -> None:
    producer = MagicMock(
        start=AsyncMock(side_effect=ConnectionError("startup failed")),
        stop=AsyncMock(side_effect=RuntimeError("cleanup failed")),
    )
    mock_producer_cls.return_value = producer
    server = KafkaServer("localhost:9092")

    with pytest.raises(ConnectionError, match="startup failed"):
        await server.connect()

    assert not server.is_connected
    producer.stop.assert_awaited_once()
