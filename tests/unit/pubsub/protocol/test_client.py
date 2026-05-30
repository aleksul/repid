from unittest.mock import ANY, AsyncMock, MagicMock

import grpc.aio

from repid.connections.pubsub.protocol import InsecureCredentials, ResilienceConfig, ResilienceState
from repid.connections.pubsub.protocol.client import (
    ACKNOWLEDGE_METHOD,
    MODIFY_ACK_DEADLINE_METHOD,
    PUBLISH_METHOD,
    PubsubProtocolClient,
)
from repid.connections.pubsub.protocol.proto import PublishRequest, PubsubMessage


def _make_client() -> tuple[PubsubProtocolClient, MagicMock, AsyncMock]:
    channel = MagicMock(spec=grpc.aio.Channel)
    mock_unary = AsyncMock(return_value=MagicMock(message_ids=["msg1"]))
    channel.unary_unary.return_value = mock_unary
    client = PubsubProtocolClient(
        channel=channel,
        credentials_provider=InsecureCredentials(),
        resilience_state=ResilienceState(ResilienceConfig()),
    )
    return client, channel, mock_unary


async def test_publish_calls_unary_with_correct_method_and_timeout() -> None:
    client, channel, mock_unary = _make_client()
    request = PublishRequest(
        topic="projects/p/topics/t",
        messages=[PubsubMessage(data=b"payload")],
    )

    await client.publish(request, timeout=3)

    channel.unary_unary.assert_called_with(
        PUBLISH_METHOD,
        request_serializer=ANY,
        response_deserializer=ANY,
    )
    mock_unary.assert_awaited_once_with(request, timeout=3)


async def test_acknowledge_with_empty_ids_is_noop() -> None:
    client, channel, _ = _make_client()

    await client.acknowledge("projects/p/subscriptions/s", [])

    channel.unary_unary.assert_not_called()


async def test_acknowledge_with_ids_serializes_correct_request() -> None:
    client, channel, mock_unary = _make_client()

    await client.acknowledge("projects/p/subscriptions/s", ["ack1", "ack2"])

    channel.unary_unary.assert_called_with(
        ACKNOWLEDGE_METHOD,
        request_serializer=ANY,
        response_deserializer=ANY,
    )
    mock_unary.assert_awaited_once()
    request = mock_unary.call_args[0][0]
    assert request.subscription == "projects/p/subscriptions/s"
    assert request.ack_ids == ["ack1", "ack2"]


async def test_modify_ack_deadline_with_empty_ids_is_noop() -> None:
    client, channel, _ = _make_client()

    await client.modify_ack_deadline("projects/p/subscriptions/s", [], 60)

    channel.unary_unary.assert_not_called()


async def test_modify_ack_deadline_with_ids_serializes_correct_request() -> None:
    client, channel, mock_unary = _make_client()

    await client.modify_ack_deadline("projects/p/subscriptions/s", ["ack1"], 60)

    channel.unary_unary.assert_called_with(
        MODIFY_ACK_DEADLINE_METHOD,
        request_serializer=ANY,
        response_deserializer=ANY,
    )
    mock_unary.assert_awaited_once()
    request = mock_unary.call_args[0][0]
    assert request.subscription == "projects/p/subscriptions/s"
    assert request.ack_ids == ["ack1"]
    assert request.ack_deadline_seconds == 60


async def test_modify_ack_deadline_clamps_seconds_to_valid_range() -> None:
    client, _, mock_unary = _make_client()

    await client.modify_ack_deadline("projects/p/subscriptions/s", ["ack1"], -1)
    request = mock_unary.call_args[0][0]
    assert request.ack_deadline_seconds == 0

    mock_unary.reset_mock()
    await client.modify_ack_deadline("projects/p/subscriptions/s", ["ack1"], 1000)
    request = mock_unary.call_args[0][0]
    assert request.ack_deadline_seconds == 600
