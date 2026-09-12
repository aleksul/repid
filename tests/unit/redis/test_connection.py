from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from repid.connections.redis import message_broker
from repid.connections.redis.message_broker import RedisServer


@patch.object(message_broker, "Redis")
async def test_redis_connect_failed_ping_is_closed_and_can_retry(mock_redis_cls: MagicMock) -> None:
    failed_client = MagicMock(
        ping=AsyncMock(side_effect=ConnectionError("offline")),
        aclose=AsyncMock(),
    )
    connected_client = MagicMock(ping=AsyncMock(), aclose=AsyncMock())
    mock_redis_cls.from_url.side_effect = [failed_client, connected_client]
    server = RedisServer("redis://localhost")

    with pytest.raises(ConnectionError, match="offline"):
        await server.connect()

    assert not server.is_connected
    failed_client.aclose.assert_awaited_once()

    await server.connect()

    assert server.is_connected
    assert mock_redis_cls.from_url.call_count == 2
    connected_client.ping.assert_awaited_once()


@patch.object(message_broker, "Redis")
async def test_redis_connect_cancelled_ping_is_closed_and_not_published(
    mock_redis_cls: MagicMock,
) -> None:
    client = MagicMock(ping=AsyncMock(side_effect=asyncio.CancelledError), aclose=AsyncMock())
    mock_redis_cls.from_url.return_value = client
    server = RedisServer("redis://localhost")

    with pytest.raises(asyncio.CancelledError):
        await server.connect()

    assert not server.is_connected
    client.aclose.assert_awaited_once()


@patch.object(message_broker, "Redis")
async def test_redis_connect_preserves_ping_error_when_cleanup_fails(
    mock_redis_cls: MagicMock,
) -> None:
    client = MagicMock(
        ping=AsyncMock(side_effect=ConnectionError("startup failed")),
        aclose=AsyncMock(side_effect=RuntimeError("cleanup failed")),
    )
    mock_redis_cls.from_url.return_value = client
    server = RedisServer("redis://localhost")

    with pytest.raises(ConnectionError, match="startup failed"):
        await server.connect()

    assert not server.is_connected
    client.aclose.assert_awaited_once()
