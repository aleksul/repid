import asyncio
from unittest.mock import MagicMock

import pytest

from repid.connections.abc import MessageAction
from repid.connections.pubsub.protocol import proto, received_message


@pytest.fixture
def msg_fixture() -> received_message.PubsubReceivedMessage:
    raw = proto.PubsubMessage(
        data=b"data",
        attributes={"key": "val", "content_type": "json"},
        message_id="msg1",
    )
    server = MagicMock()
    write_queue: asyncio.Queue[proto.StreamingPullRequest] = asyncio.Queue()

    return received_message.PubsubReceivedMessage(
        raw_message=raw,
        ack_id="ack1",
        delivery_attempt=1,
        subscription_path="sub1",
        channel_name="chan1",
        write_queue=write_queue,
        server=server,
    )


def test_received_message_properties(msg_fixture: received_message.PubsubReceivedMessage) -> None:
    assert msg_fixture.payload == b"data"
    assert msg_fixture.headers == {"key": "val", "content_type": "json"}
    assert msg_fixture.content_type == "json"
    assert msg_fixture.reply_to is None
    assert msg_fixture.channel == "chan1"
    assert not msg_fixture.is_acted_on
    assert msg_fixture.message_id == "msg1"
    assert msg_fixture.delivery_attempt == 1


async def test_received_message_ack(msg_fixture: received_message.PubsubReceivedMessage) -> None:
    await msg_fixture.ack()

    assert msg_fixture.is_acted_on
    assert msg_fixture.action == MessageAction.acked

    req = msg_fixture._write_queue.get_nowait()
    assert isinstance(req, proto.StreamingPullRequest)
    assert req.ack_ids == ["ack1"]

    # Second ack should be ignored
    await msg_fixture.ack()
    assert msg_fixture._write_queue.empty()


async def test_received_message_nack(msg_fixture: received_message.PubsubReceivedMessage) -> None:
    await msg_fixture.nack()

    assert msg_fixture.is_acted_on

    req = msg_fixture._write_queue.get_nowait()
    assert isinstance(req, proto.StreamingPullRequest)
    assert req.modify_deadline_seconds == [0]
    assert req.modify_deadline_ack_ids == ["ack1"]

    # Second nack should be ignored
    await msg_fixture.nack()
    assert msg_fixture._write_queue.empty()


async def test_received_message_reject(msg_fixture: received_message.PubsubReceivedMessage) -> None:
    await msg_fixture.reject()

    assert msg_fixture.is_acted_on

    req = msg_fixture._write_queue.get_nowait()
    assert isinstance(req, proto.StreamingPullRequest)
    assert req.modify_deadline_seconds == [1]
    assert req.modify_deadline_ack_ids == ["ack1"]

    # Second reject should be ignored
    await msg_fixture.reject()
    assert msg_fixture._write_queue.empty()


async def test_received_message_extend_deadline(
    msg_fixture: received_message.PubsubReceivedMessage,
) -> None:
    await msg_fixture.extend_deadline(30)

    assert not msg_fixture.is_acted_on  # Should not be marked as acted on

    req = msg_fixture._write_queue.get_nowait()
    assert isinstance(req, proto.StreamingPullRequest)
    assert req.modify_deadline_seconds == [30]

    # Check clamping
    await msg_fixture.extend_deadline(1)
    req = msg_fixture._write_queue.get_nowait()
    assert req.modify_deadline_seconds == [10]

    await msg_fixture.extend_deadline(1000)
    req = msg_fixture._write_queue.get_nowait()
    assert req.modify_deadline_seconds == [600]

    # If acted on, should do nothing
    msg_fixture._action = MessageAction.acked
    await msg_fixture.extend_deadline(30)
    assert msg_fixture._write_queue.empty()


async def test_received_message_reply(msg_fixture: received_message.PubsubReceivedMessage) -> None:
    with pytest.raises(NotImplementedError, match=r"PubSub does not support native replies\."):
        await msg_fixture.reply(payload=b"response")


def test_received_message_no_attributes(
    msg_fixture: received_message.PubsubReceivedMessage,
) -> None:
    msg_fixture._raw_message.attributes = {}
    assert msg_fixture.headers is None
    assert msg_fixture.content_type is None


async def test_received_message_reply_after_ack_is_noop(
    msg_fixture: received_message.PubsubReceivedMessage,
) -> None:
    await msg_fixture.ack()
    await msg_fixture.reply(payload=b"ignored")
    assert msg_fixture._write_queue.qsize() == 1  # Only the ack request
