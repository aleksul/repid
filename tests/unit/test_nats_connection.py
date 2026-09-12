from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from repid.connections.nats.message_broker import NatsReceivedMessage, NatsServer


@patch("nats.connect", new_callable=AsyncMock)
async def test_nats_connect_failure_clears_client_and_can_retry(
    mock_connect: AsyncMock,
) -> None:
    connected_client = MagicMock(is_connected=True)
    mock_connect.side_effect = [ConnectionError("offline"), connected_client]
    server = NatsServer("nats://localhost:4222")

    with pytest.raises(ConnectionError, match="offline"):
        await server.connect()

    assert not server.is_connected
    assert server._nc is None
    assert server._js is None

    await server.connect()

    assert server.is_connected
    assert server._nc is connected_client
    assert server._js is connected_client.jetstream.return_value
    assert mock_connect.call_count == 2


@patch("nats.connect", new_callable=AsyncMock)
async def test_nats_connect_closes_stale_client_before_reconnect(
    mock_connect: AsyncMock,
) -> None:
    stale_client = MagicMock(is_connected=False)
    stale_client.close = AsyncMock()
    server = NatsServer("nats://localhost:4222")
    server._nc = stale_client

    connected_client = MagicMock(is_connected=True)
    mock_connect.return_value = connected_client

    await server.connect()

    stale_client.close.assert_awaited_once()
    assert server._nc is connected_client
    assert server._js is connected_client.jetstream.return_value


@patch("nats.connect", new_callable=AsyncMock)
async def test_nats_connect_failure_with_stale_client_closes_it(
    mock_connect: AsyncMock,
) -> None:
    stale_client = MagicMock(is_connected=False)
    stale_client.close = AsyncMock()
    server = NatsServer("nats://localhost:4222")
    server._nc = stale_client
    mock_connect.side_effect = ConnectionError("offline")

    with pytest.raises(ConnectionError, match="offline"):
        await server.connect()

    stale_client.close.assert_awaited_once()
    assert server._nc is None
    assert server._js is None
    assert not server.is_connected


def _make_nats_received_message(msg: MagicMock) -> NatsReceivedMessage:
    return NatsReceivedMessage(msg, NatsServer("nats://localhost:4222"), "chan")


async def test_nats_received_message_ack_failure_resets_action() -> None:
    msg = MagicMock()
    msg.ack = AsyncMock(side_effect=RuntimeError("ack failed"))
    received = _make_nats_received_message(msg)

    with pytest.raises(RuntimeError, match="ack failed"):
        await received.ack()

    assert received.action is None
    assert not received.is_acted_on


async def test_nats_received_message_reject_failure_resets_action() -> None:
    msg = MagicMock()
    msg.nak = AsyncMock(side_effect=RuntimeError("nak failed"))
    received = _make_nats_received_message(msg)

    with pytest.raises(RuntimeError, match="nak failed"):
        await received.reject()

    assert received.action is None
    assert not received.is_acted_on
