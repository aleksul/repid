import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from repid.connections.abc import MessageAction
from repid.connections.pubsub.protocol import proto, received_message
from repid.connections.pubsub.protocol.control_batcher import PubsubControlBatcher


class _FakeBatcher:
    def __init__(self) -> None:
        self.add_ack = AsyncMock()
        self.add_modify_deadline = AsyncMock()


class _FakeControlClient:
    def __init__(self) -> None:
        self.acknowledge = AsyncMock()
        self.modify_ack_deadline = AsyncMock()


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


async def test_ack_allows_keep_alive_but_blocks_other_settlements(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    ack_started = asyncio.Event()
    release_ack = asyncio.Event()

    async def block_ack(*_: str) -> None:
        ack_started.set()
        await release_ack.wait()

    _batcher(msg).add_ack.side_effect = block_ack
    ack_task = asyncio.create_task(msg.ack())
    await ack_started.wait()
    other_tasks = [
        asyncio.create_task(msg.nack()),
        asyncio.create_task(msg.reject()),
        asyncio.create_task(msg.keep_alive()),
    ]
    await asyncio.gather(*other_tasks)
    release_ack.set()
    await ack_task

    assert msg.action is MessageAction.acked
    _batcher(msg).add_ack.assert_awaited_once_with("sub1", "ack1")
    _batcher(msg).add_modify_deadline.assert_awaited_once_with("sub1", "ack1", 300)


async def test_timer_flushes_keep_alive_during_a_blocked_ack(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def acknowledge(*_: object) -> None:
        started.set()
        await release.wait()

    client = _FakeControlClient()
    client.acknowledge.side_effect = acknowledge
    batcher = PubsubControlBatcher(client, flush_interval=0.001)
    msg._server._control_batcher = batcher
    await batcher.start()
    ack = asyncio.create_task(msg.ack())
    try:
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.wait_for(msg.keep_alive(), 1)
        client.modify_ack_deadline.assert_awaited_once_with("sub1", ["ack1"], 300)
        assert not ack.done()
    finally:
        release.set()
        await asyncio.wait_for(ack, 1)
        await batcher.stop()


async def test_cancelled_settlement_waits_for_broker_and_blocks_retry(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    ack_started = asyncio.Event()
    release_ack = asyncio.Event()

    async def block_ack(*_: str) -> None:
        ack_started.set()
        await release_ack.wait()

    _batcher(msg).add_ack.side_effect = block_ack
    task = asyncio.create_task(msg.ack())
    await ack_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    assert msg.action is MessageAction.acked
    await msg.ack()
    _batcher(msg).add_ack.assert_awaited_once_with("sub1", "ack1")
    release_ack.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert msg.action is MessageAction.acked


@pytest.mark.parametrize("acknowledge", [False, True])
async def test_cancelled_queued_settlement_is_still_flushed(acknowledge: bool) -> None:
    client = _FakeControlClient()
    batcher = PubsubControlBatcher(client, flush_interval=10)
    server = MagicMock()
    server._control_batcher = batcher
    message = received_message.PubsubReceivedMessage(
        raw_message=proto.PubsubMessage(data=b"data"),
        ack_id="ack1",
        delivery_attempt=1,
        subscription_path="sub1",
        channel_name="chan1",
        server=server,
        stream_ack_deadline_seconds=300,
    )
    await batcher.start()

    task = asyncio.create_task(message.ack() if acknowledge else message.reject())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    await message.nack()
    await batcher.stop()
    with pytest.raises(asyncio.CancelledError):
        await task

    if acknowledge:
        assert message.action is MessageAction.acked
        client.acknowledge.assert_awaited_once_with("sub1", ["ack1"])
        client.modify_ack_deadline.assert_not_awaited()
    else:
        assert message.action is MessageAction.rejected
        client.acknowledge.assert_not_awaited()
        client.modify_ack_deadline.assert_awaited_once_with("sub1", ["ack1"], 1)


async def test_cancelled_flushing_settlement_reserves_its_disposition() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    client = _FakeControlClient()

    async def acknowledge(*_: object) -> None:
        started.set()
        await release.wait()

    client.acknowledge.side_effect = acknowledge
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=1)
    server = MagicMock()
    server._control_batcher = batcher
    message = received_message.PubsubReceivedMessage(
        raw_message=proto.PubsubMessage(data=b"data"),
        ack_id="ack1",
        delivery_attempt=1,
        subscription_path="sub1",
        channel_name="chan1",
        server=server,
        stream_ack_deadline_seconds=300,
    )
    await batcher.start()

    task = asyncio.create_task(message.ack())
    await started.wait()
    task.cancel()
    assert message.action is MessageAction.acked
    await message.nack()
    client.modify_ack_deadline.assert_not_awaited()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await batcher.stop()


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
    assert await msg.keep_alive() is False

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


async def test_reject_failure_does_not_set_action(
    msg: received_message.PubsubReceivedMessage,
) -> None:
    _batcher(msg).add_modify_deadline.side_effect = RuntimeError("RPC failed")

    with pytest.raises(RuntimeError):
        await msg.reject()

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


@pytest.mark.parametrize(("action", "delay"), [("nack", 0), ("reject", 1)])
@pytest.mark.parametrize("cancel_keep_alive", [False, True])
@pytest.mark.parametrize("race", ["none", "cancel", "ack"])
async def test_redelivery_waits_for_in_flight_deadline_extension(
    msg: received_message.PubsubReceivedMessage,
    action: str,
    delay: int,
    cancel_keep_alive: bool,
    race: str,
) -> None:
    extending = asyncio.Event()
    release = asyncio.Event()
    applied = []

    async def modify(_subscription: str, _ack_id: str, seconds: int) -> None:
        if seconds == 300:
            extending.set()
            await release.wait()
        applied.append(seconds)

    client = _FakeControlClient()
    client.modify_ack_deadline.side_effect = modify
    batcher = PubsubControlBatcher(client, max_batch_ids=1)
    msg._server._control_batcher = batcher
    keep_alive_task = asyncio.create_task(msg.keep_alive())
    await asyncio.wait_for(extending.wait(), 1)
    if cancel_keep_alive:
        keep_alive_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(keep_alive_task, 1)
    settlement = asyncio.create_task(getattr(msg, action)())
    try:
        done, _ = await asyncio.wait({settlement}, timeout=0.01)
        assert not done
        client.modify_ack_deadline.assert_awaited_once_with("sub1", ["ack1"], 300)
        if race == "cancel":
            settlement.cancel()
            with pytest.raises(asyncio.CancelledError):
                await settlement
            assert not msg.is_acted_on
            settlement = asyncio.create_task(getattr(msg, action)())
        elif race == "ack":
            await msg.ack()
    finally:
        release.set()
        await asyncio.wait_for(settlement, 1)
        if not cancel_keep_alive:
            await keep_alive_task
        await batcher.stop()
    assert applied == ([300] if race == "ack" else [300, delay])
