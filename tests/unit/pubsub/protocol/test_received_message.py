from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from repid.connections.abc import MessageAction
from repid.connections.pubsub.protocol import proto, received_message


class _FakeBatcher:
    def __init__(self) -> None:
        self.add_ack = AsyncMock()
        self.add_modify_deadline = AsyncMock()


@pytest.fixture
def msg() -> received_message.PubsubReceivedMessage:
    raw = proto.PubsubMessage(
        data=b"data",
        attributes={"key": "val", "content_type": "json"},
        message_id="msg1",
    )
    server = MagicMock()
    server._control_batcher = _FakeBatcher()

    return received_message.PubsubReceivedMessage(
        raw_message=raw,
        ack_id="ack1",
        delivery_attempt=1,
        subscription_path="sub1",
        channel_name="chan1",
        server=server,
        stream_ack_deadline_seconds=300,
    )


def _batcher(m: received_message.PubsubReceivedMessage) -> _FakeBatcher:
    return cast(_FakeBatcher, m._server._control_batcher)


def test_basic_properties(msg: received_message.PubsubReceivedMessage) -> None:
    assert msg.payload == b"data"
    assert msg.headers == {"key": "val", "content_type": "json"}
    assert msg.content_type == "json"
    assert msg.reply_to is None
    assert msg.channel == "chan1"
    assert not msg.is_acted_on
    assert msg.message_id == "msg1"
    assert msg.delivery_attempt == 1


async def test_ack_calls_batcher_add_ack(msg: received_message.PubsubReceivedMessage) -> None:
    await msg.ack()

    assert msg.is_acted_on
    assert msg.action == MessageAction.acked
    _batcher(msg).add_ack.assert_awaited_once_with("sub1", "ack1")


async def test_second_ack_is_noop(msg: received_message.PubsubReceivedMessage) -> None:
    await msg.ack()
    _batcher(msg).add_ack.reset_mock()

    await msg.ack()

    _batcher(msg).add_ack.assert_not_called()


async def test_nack_calls_batcher_add_modify_with_zero(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    await msg.nack()

    assert msg.is_acted_on
    _batcher(msg).add_modify_deadline.assert_awaited_once_with("sub1", "ack1", 0)


async def test_reject_calls_batcher_add_modify_with_one(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    await msg.reject()

    assert msg.is_acted_on
    _batcher(msg).add_modify_deadline.assert_awaited_once_with("sub1", "ack1", 1)


async def test_second_nack_is_noop(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    await msg.nack()
    _batcher(msg).add_modify_deadline.reset_mock()

    await msg.nack()

    _batcher(msg).add_modify_deadline.assert_not_called()


async def test_second_reject_is_noop(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    await msg.reject()
    _batcher(msg).add_modify_deadline.reset_mock()

    await msg.reject()

    _batcher(msg).add_modify_deadline.assert_not_called()


async def test_extend_deadline_clamps_and_calls_batcher(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    await msg.extend_deadline(30)

    assert not msg.is_acted_on
    _batcher(msg).add_modify_deadline.assert_awaited_once_with("sub1", "ack1", 30)

    _batcher(msg).add_modify_deadline.reset_mock()
    await msg.extend_deadline(1)
    _batcher(msg).add_modify_deadline.assert_awaited_once_with("sub1", "ack1", 10)

    _batcher(msg).add_modify_deadline.reset_mock()
    await msg.extend_deadline(1000)
    _batcher(msg).add_modify_deadline.assert_awaited_once_with("sub1", "ack1", 600)


async def test_extend_deadline_is_noop_after_action(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    msg._action = MessageAction.acked
    await msg.extend_deadline(30)
    _batcher(msg).add_modify_deadline.assert_not_called()


async def test_reply_raises_not_implemented(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    with pytest.raises(NotImplementedError, match=r"PubSub does not support native replies\."):
        await msg.reply(payload=b"response")


def test_headers_none_when_attributes_empty(msg: received_message.PubsubReceivedMessage) -> None:
    msg._raw_message.attributes = {}
    assert msg.headers is None
    assert msg.content_type is None


async def test_reply_after_ack_is_noop(msg: received_message.PubsubReceivedMessage) -> None:
    await msg.ack()
    _batcher(msg).add_ack.reset_mock()

    await msg.reply(payload=b"ignored")

    _batcher(msg).add_ack.assert_not_called()
    _batcher(msg).add_modify_deadline.assert_not_called()


def test_keep_alive_interval_is_one_third_of_stream_deadline(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    assert msg.keep_alive_interval == 100


async def test_keep_alive_calls_batcher_add_modify_with_stream_deadline(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    await msg.keep_alive()

    assert not msg.is_acted_on
    _batcher(msg).add_modify_deadline.assert_awaited_once_with("sub1", "ack1", 300)


async def test_keep_alive_noop_after_ack(msg: received_message.PubsubReceivedMessage) -> None:
    await msg.ack()

    _batcher(msg).add_modify_deadline.reset_mock()
    await msg.keep_alive()

    _batcher(msg).add_modify_deadline.assert_not_called()


async def test_ack_failure_does_not_set_action(msg: received_message.PubsubReceivedMessage) -> None:
    _batcher(msg).add_ack.side_effect = RuntimeError("RPC failed")

    with pytest.raises(RuntimeError):
        await msg.ack()

    assert not msg.is_acted_on


async def test_nack_failure_does_not_set_action(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    _batcher(msg).add_modify_deadline.side_effect = RuntimeError("RPC failed")

    with pytest.raises(RuntimeError):
        await msg.nack()

    assert not msg.is_acted_on


async def test_ack_raises_connection_error_when_batcher_missing() -> None:
    raw = proto.PubsubMessage(data=b"x")
    server = MagicMock()
    server._control_batcher = None
    msg = received_message.PubsubReceivedMessage(
        raw_message=raw,
        ack_id="ack1",
        delivery_attempt=1,
        subscription_path="sub1",
        channel_name="chan1",
        server=server,
        stream_ack_deadline_seconds=300,
    )

    with pytest.raises(ConnectionError, match=r"Control batcher not available\."):
        await msg.ack()

    assert not msg.is_acted_on
