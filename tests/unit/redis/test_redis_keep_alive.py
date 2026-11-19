from unittest.mock import AsyncMock, MagicMock

import pytest

from repid.connections.abc import MessageAction
from repid.connections.redis.message_broker import RedisReceivedMessage, RedisServer


async def test_redis_received_message_keep_alive_action_not_none() -> None:
    mock_redis = AsyncMock()
    mock_server = MagicMock(spec=RedisServer, _redis=mock_redis)

    msg = RedisReceivedMessage(
        payload=b"test",
        headers=None,
        content_type=None,
        reply_to=None,
        message_id="test_id",
        channel="test_channel",
        stream_name="test_stream",
        consumer_group="test_group",
        consumer_name="test_consumer",
        server=mock_server,
        redis_client=mock_redis,
        dlq_stream="test_dlq",
    )

    msg._action = MessageAction.acked
    await msg.keep_alive()
    mock_redis.xclaim.assert_not_called()


@pytest.mark.parametrize(
    ("renewed", "expected"),
    [
        pytest.param([b"test_id"], None, id="delivery_renewed"),
        pytest.param([], False, id="delivery_missing"),
    ],
)
async def test_redis_received_message_keep_alive_result(
    renewed: list[bytes],
    expected: bool | None,
) -> None:
    mock_redis = AsyncMock(xclaim=AsyncMock(return_value=renewed))
    mock_server = MagicMock(spec=RedisServer, _redis=mock_redis)

    msg = RedisReceivedMessage(
        payload=b"test",
        headers=None,
        content_type=None,
        reply_to=None,
        message_id="test_id",
        channel="test_channel",
        stream_name="test_stream",
        consumer_group="test_group",
        consumer_name="test_consumer",
        server=mock_server,
        redis_client=mock_redis,
        dlq_stream="test_dlq",
    )

    assert await msg.keep_alive() is expected
    mock_redis.xclaim.assert_awaited_once_with(
        "test_stream",
        "test_group",
        "test_consumer",
        min_idle_time=0,
        message_ids=["test_id"],
        justid=True,
    )


@pytest.mark.parametrize(
    ("min_idle_ms", "interval"),
    [
        pytest.param(0, None, id="renewal_disabled"),
        pytest.param(1000, 1 / 3, id="short_interval"),
        pytest.param(60_000, 20, id="long_interval"),
    ],
)
def test_redis_keep_alive_interval(min_idle_ms: int, interval: float | None) -> None:
    mock_redis = AsyncMock()
    mock_server = MagicMock(spec=RedisServer, _redis=mock_redis)

    msg = RedisReceivedMessage(
        payload=b"test",
        headers=None,
        content_type=None,
        reply_to=None,
        message_id="test_id",
        channel="test_channel",
        stream_name="test_stream",
        consumer_group="test_group",
        consumer_name="test_consumer",
        server=mock_server,
        redis_client=mock_redis,
        dlq_stream="test_dlq",
        min_idle_ms=min_idle_ms,
    )
    assert msg.keep_alive_interval == interval
