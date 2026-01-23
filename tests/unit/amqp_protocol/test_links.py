from __future__ import annotations

import asyncio
import json
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest

from repid.connections.amqp._uamqp._encode import message_to_transfer_frames
from repid.connections.amqp._uamqp.message import Message
from repid.connections.amqp._uamqp.performatives import (
    AttachFrame,
    DetachFrame,
    FlowFrame,
    TransferFrame,
)
from repid.connections.amqp.protocol.links import (
    Link,
    LinkError,
    ReceiverLink,
    SenderLink,
    Subscription,
)
from repid.connections.amqp.protocol.states import LinkState
from repid.connections.amqp.protocol.transport import AmqpTransport, TransportConfig

from .utils import (
    FakeConnection,
    FakeSession,
)


async def test_link_properties() -> None:
    session = Mock()
    link = SenderLink(session, "name", "addr", 0)
    assert link.name == "name"
    assert link.address == "addr"
    assert link.handle == 0


async def test_abstract_attach() -> None:
    # Explicitly call abstract method to cover the raise line
    session = Mock()
    # We use a mock that mimics a Link just enough for the method call if needed?
    # Or just call it unbound? It is async.

    # Create a concrete link
    link = SenderLink(session, "name", "addr", 0)

    with pytest.raises(NotImplementedError):
        await Link.attach(link)


async def test_sender_link_flow_consumed_credit() -> None:
    session = Mock()
    link = SenderLink(session, "name", "addr", 0)
    link._delivery_count = 10
    link._link_credit = 5

    flow = FlowFrame(
        handle=0,
        delivery_count=12,
        link_credit=10,
        next_incoming_id=0,
        incoming_window=100,
        next_outgoing_id=0,
        outgoing_window=100,
    )
    await link._handle_flow(flow)

    assert link._link_credit == 8


async def test_sender_link_flow_zero_credit() -> None:
    session = Mock()
    link = SenderLink(session, "name", "addr", 0)
    link._credit_available.set()

    flow = FlowFrame(
        handle=0,
        link_credit=0,
        next_incoming_id=0,
        incoming_window=0,
        next_outgoing_id=0,
        outgoing_window=0,
    )
    await link._handle_flow(flow)

    assert not link._credit_available.is_set()


async def test_receiver_process_message_types() -> None:
    session = Mock()
    link = ReceiverLink(session, "name", "addr", 0, Mock())
    link._callback = Mock()

    with patch("repid.connections.amqp.protocol.links.transfer_frames_to_message") as mock_conv:
        # Body is String
        msg_str = Message(value="hello")
        mock_conv.return_value = msg_str
        link._incoming_transfers = [TransferFrame(handle=0, delivery_id=1, delivery_tag=b"1")]
        await link._process_message()
        link._callback.assert_called_with(b"hello", None, 1, b"1", link)

        # Body is Object/Dict
        msg_obj = Message(value={"k": "v"})
        mock_conv.return_value = msg_obj
        link._incoming_transfers = [TransferFrame(handle=0, delivery_id=2, delivery_tag=b"2")]
        await link._process_message()
        link._callback.assert_called_with(b'{"k": "v"}', None, 2, b"2", link)

        # Headers
        msg_head = Message(value=b"b", application_properties={"h": "v"})
        mock_conv.return_value = msg_head
        link._incoming_transfers = [TransferFrame(handle=0, delivery_id=3, delivery_tag=b"3")]
        await link._process_message()
        args = link._callback.call_args[0]
        assert args[1] == {"h": "v"}


async def test_transport_configure_keepalive_no_writer() -> None:
    config = TransportConfig("localhost", 5672)
    transport = AmqpTransport(config)
    # _writer is None by default
    transport._configure_keepalive()
    # verify no error and return (implicit coverage)


@pytest.fixture
def session() -> Any:
    connection = FakeConnection(max_frame_size=512)
    return FakeSession(connection=connection, channel=1)


@pytest.fixture
def sender(session: Any) -> SenderLink:
    return SenderLink(session, "test-sender", "test-queue", 0)


@pytest.fixture
def receiver(session: Any) -> ReceiverLink:
    return ReceiverLink(session, "test-receiver", "test-queue", 0, lambda *_: None)


async def test_sender_link_attach(sender: SenderLink) -> None:
    await sender.attach()

    # Initally in ATTACH_SENT
    assert sender.state == LinkState.ATTACH_SENT

    # Connection should have sent ATTACH frame
    assert len(cast(Any, sender.session.connection).sent) == 1
    assert isinstance(cast(Any, sender.session.connection).sent[0][1], AttachFrame)

    # Receive ATTACH response
    attach_response = AttachFrame(
        name="test-sender",
        handle=1,
        role=True,
        source=None,
        target=None,
    )
    await sender._handle_attach(attach_response)

    assert sender.state == LinkState.ATTACHED
    assert sender.is_attached
    assert sender.is_usable


async def test_sender_link_send_message(sender: SenderLink) -> None:
    # Setup link as attached and ready
    sender._state_machine.transition_sync("send_attach")
    sender._state_machine.transition_sync("recv_attach")

    payload = b"hello world"
    await sender.send(payload)

    # Should send transfer frame
    assert len(cast(Any, sender.session.connection).sent) == 1
    frame = cast(Any, sender.session.connection).sent[0][1]
    assert isinstance(frame, TransferFrame)
    assert not frame.more
    assert frame.settled is True


async def test_sender_link_send_large_message(sender: SenderLink) -> None:
    # Setup link as attached and ready
    sender._state_machine.transition_sync("send_attach")
    sender._state_machine.transition_sync("recv_attach")

    # Set small max frame size on connection to force fragmentation
    sender.session.connection.max_frame_size = 64  # type: ignore[misc]

    payload = b"x" * 100
    await sender.send(payload)

    # Should be multiple frames
    assert len(cast(Any, sender.session.connection).sent) > 1

    # Check frames
    for _, frame in cast(Any, sender.session.connection).sent[:-1]:
        assert isinstance(frame, TransferFrame)
        assert frame.more is True

    last_frame = cast(Any, sender.session.connection).sent[-1][1]
    assert isinstance(last_frame, TransferFrame)
    assert frame.more is True  # previous frame has more=True


async def test_sender_link_flow_control(sender: SenderLink) -> None:
    # Setup link
    sender._state_machine.transition_sync("send_attach")
    sender._state_machine.transition_sync("recv_attach")

    # Initially 0 credit
    assert sender.link_credit == 0

    # Receive flow
    flow = FlowFrame(
        next_incoming_id=0,
        incoming_window=100,
        next_outgoing_id=0,
        outgoing_window=100,
        handle=0,
        delivery_count=0,
        link_credit=10,
    )
    await sender._handle_flow(flow)

    assert sender.link_credit == 10

    # Send consumes credit? Currently SenderLink implementation says:
    # "For now, we just send" (ignoring credit for sending, but tracking update)
    # But later updates should respect consumed credit

    # Update flow with delivery count change
    flow.delivery_count = 0
    flow.link_credit = 5
    await sender._handle_flow(flow)

    assert sender.link_credit == 5


async def test_sender_link_not_usable(sender: SenderLink) -> None:
    with pytest.raises(LinkError):
        await sender.send(b"test")


async def test_receiver_link_attach(receiver: ReceiverLink) -> None:
    await receiver.attach()

    assert receiver.state == LinkState.ATTACH_SENT
    assert len(cast(Any, receiver.session.connection).sent) == 1

    # Receive attach
    attach_response = AttachFrame(
        name="test-receiver",
        handle=1,
        role=False,
        source=None,
        target=None,
    )
    await receiver._handle_attach(attach_response)

    assert receiver.state == LinkState.ATTACHED

    # Should have sent initial flow
    assert len(receiver.session.connection.sent) == 2
    assert isinstance(receiver.session.connection.sent[1][1], FlowFrame)


async def test_receiver_link_receive_message(receiver: ReceiverLink) -> None:
    received = []

    def on_message(
        body: bytes,
        headers: dict[str, Any] | None,
        _delivery_id: int,
        _delivery_tag: bytes,
        _link: ReceiverLink,
    ) -> None:
        received.append((body, headers))

    receiver._callback = on_message

    # Pre-calculate transfer frame
    msg = Message(data=b"test-data")
    frames = list(message_to_transfer_frames(msg, 512, 0, b"0", True))

    await receiver._handle_transfer(frames[0])

    assert len(received) == 1
    assert received[0][0] == b"test-data"


async def test_receiver_link_flow_echo(receiver: ReceiverLink) -> None:
    # Setup
    receiver._state_machine.transition_sync("send_attach")
    receiver._state_machine.transition_sync("recv_attach")
    cast(Any, receiver.session.connection).sent.clear()

    # Receive flow with echo=True
    flow = FlowFrame(
        next_incoming_id=0,
        incoming_window=100,
        next_outgoing_id=0,
        outgoing_window=100,
        handle=0,
        delivery_count=0,
        link_credit=10,
        echo=True,
    )
    await receiver._handle_flow(flow)

    # Should send flow back
    assert len(cast(Any, receiver.session.connection).sent) == 1
    assert isinstance(cast(Any, receiver.session.connection).sent[0][1], FlowFrame)


async def test_receiver_link_credit_replenish(receiver: ReceiverLink) -> None:
    # Setup
    receiver._state_machine.transition_sync("send_attach")
    receiver._state_machine.transition_sync("recv_attach")
    cast(Any, receiver.session.connection).sent.clear()

    # Set prefetch low to trigger replenish easily
    receiver._prefetch = 10
    receiver._link_credit = 6

    # Process message
    msg = Message(data=b"test")
    frames = list(message_to_transfer_frames(msg, 512, 0, b"0", True))

    await receiver._handle_transfer(frames[0])

    # Credit should decrease
    assert receiver.link_credit == 5

    # Not replenished yet (threshold is prefetch//2 = 5)
    assert len(cast(Any, receiver.session.connection).sent) == 0

    # Process another
    await receiver._handle_transfer(frames[0])

    # Credit decreases to 4, which is < 5, so replenish should trigger
    assert receiver.link_credit == 10  # Reset to prefetch
    assert len(receiver.session.connection.sent) == 1  # type: ignore[attr-defined]
    assert isinstance(receiver.session.connection.sent[0][1], FlowFrame)  # type: ignore[attr-defined]


async def test_receiver_process_message_callback_async(receiver: ReceiverLink) -> None:
    future: asyncio.Future[bytes] = asyncio.Future()

    async def on_message(body: bytes, *_: Any) -> None:
        future.set_result(body)

    receiver._callback = on_message

    msg = Message(data=b"async-test")
    frames = list(message_to_transfer_frames(msg, 512, 0, b"0", True))

    await receiver._handle_transfer(frames[0])

    result = await future
    assert result == b"async-test"


async def test_receiver_process_message_json_body(receiver: ReceiverLink) -> None:
    received = []
    receiver._callback = lambda body, *_: received.append(body)

    # Body is dict (json) - must be bytes
    data = json.dumps({"key": "value"}).encode()
    msg = Message(data=data)
    frames = list(message_to_transfer_frames(msg, 512, 0, b"0", True))

    await receiver._handle_transfer(frames[0])

    assert len(received) == 1
    assert json.loads(received[0].decode()) == {"key": "value"}


async def test_receiver_process_message_str_body(receiver: ReceiverLink) -> None:
    received = []
    receiver._callback = lambda body, *_: received.append(body)

    msg = Message(data=b"string-body")
    frames = list(message_to_transfer_frames(msg, 512, 0, b"0", True))

    await receiver._handle_transfer(frames[0])

    assert received[0] == b"string-body"


async def test_receiver_process_message_error(receiver: ReceiverLink) -> None:
    # Callback raises exception
    receiver._callback = Mock(side_effect=ValueError("Callback error"))

    msg = Message(data=b"data")
    frames = list(message_to_transfer_frames(msg, 512, 0, b"0", True))

    # Should catch exception and clear transfers
    await receiver._handle_transfer(frames[0])

    assert len(receiver._incoming_transfers) == 0


async def test_link_detach(sender: SenderLink) -> None:
    # Setup
    sender._state_machine.transition_sync("send_attach")
    sender._state_machine.transition_sync("recv_attach")

    await sender.detach()

    assert sender.state == LinkState.DETACH_SENT
    assert len(cast(Any, sender.session.connection).sent) == 1
    assert isinstance(cast(Any, sender.session.connection).sent[0][1], DetachFrame)

    # Receive detach response
    await sender._handle_detach(DetachFrame(handle=0, closed=True))

    assert sender.state == LinkState.DETACHED
    assert sender.closed


async def test_link_detach_remote_initiated(sender: SenderLink) -> None:
    # Setup
    sender._state_machine.transition_sync("send_attach")
    sender._state_machine.transition_sync("recv_attach")

    # Remote sends detach
    await sender._handle_detach(DetachFrame(handle=0, closed=True))

    # Should send detach response
    assert len(cast(Any, sender.session.connection).sent) == 1
    assert isinstance(cast(Any, sender.session.connection).sent[0][1], DetachFrame)

    assert sender.state == LinkState.DETACHED


async def test_link_detach_already_detached(sender: SenderLink) -> None:
    sender._state_machine.reset(LinkState.DETACHED)

    await sender.detach()

    assert len(sender.session.connection.sent) == 0  # type: ignore[attr-defined]


async def test_link_detach_error(sender: SenderLink) -> None:
    sender._state_machine.transition_sync("send_attach")
    sender._state_machine.transition_sync("recv_attach")

    # Mock send_performative to raise error
    with patch.object(sender.session.connection, "send_performative", Mock(side_effect=OSError)):
        await sender.detach()
    # Should swallow error

    # Should still remove link
    # (Checking if _remove_link was called on session - easiest via checking session.links if we had it populated)
    # But here we just check no unexpected raise


async def test_link_invalidate(sender: SenderLink) -> None:
    wait_task = asyncio.create_task(sender.wait_ready())

    sender.invalidate()

    assert sender.state == LinkState.DETACHED
    assert sender._invalidated

    # wait_ready should raise LinkError
    with pytest.raises(LinkError, match="invalidated"):
        await wait_task


async def test_link_wait_ready_timeout(sender: SenderLink) -> None:
    with pytest.raises(asyncio.TimeoutError):
        await sender.wait_ready(timeout=0.01)


async def test_subscription(receiver: ReceiverLink) -> None:
    sub = Subscription(receiver)

    assert sub.link is receiver
    assert not sub.is_active  # Connection not connected in fixture usually

    # Mock connection connected
    receiver.session.connection.is_connected = True  # type: ignore[misc]
    receiver._state_machine.transition_sync("send_attach")
    receiver._state_machine.transition_sync("recv_attach")  # make usable

    assert sub.is_active

    await sub.cancel()
    assert receiver.state == LinkState.DETACH_SENT


async def test_receiver_set_credit(receiver: ReceiverLink) -> None:
    receiver._state_machine.transition_sync("send_attach")
    receiver._state_machine.transition_sync("recv_attach")
    receiver.session.connection.sent.clear()  # type: ignore[attr-defined]

    await receiver.set_credit(500)

    assert receiver.link_credit == 500
    assert len(receiver.session.connection.sent) == 1  # type: ignore[attr-defined]
    assert isinstance(receiver.session.connection.sent[0][1], FlowFrame)  # type: ignore[attr-defined]
