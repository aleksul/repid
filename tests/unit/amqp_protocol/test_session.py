from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from repid.connections.amqp._uamqp.performatives import (
    AttachFrame,
    BeginFrame,
    DetachFrame,
    DispositionFrame,
    EndFrame,
    FlowFrame,
    TransferFrame,
)
from repid.connections.amqp.protocol import links
from repid.connections.amqp.protocol.connection import (
    AmqpConnection,
)
from repid.connections.amqp.protocol.links import ReceiverLink, SenderLink
from repid.connections.amqp.protocol.session import Session, SessionError
from repid.connections.amqp.protocol.states import (
    SessionState,
)

from .utils import FakeConnection


async def test_session_end_already_terminal() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    session._state_machine._state = SessionState.UNMAPPED

    # Should not raise when already terminal
    await session.end()
    assert session._state_machine.is_terminal()


async def test_session_end_link_detach_error() -> None:
    @dataclass(slots=True)
    class FakeLinkWithDetachError:
        name: str = "test-link"
        _handle: int = 0
        _remote_handle: int | None = None

        async def detach(self) -> None:
            raise OSError("Detach failed")

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    await session._state_machine.transition("send_begin")
    await session._state_machine.transition("recv_begin")

    # Add a link that will fail to detach
    link = FakeLinkWithDetachError()
    session._links["test-link"] = link  # type: ignore[assignment]

    # Should handle detach error gracefully
    await session.end()
    assert len(connection.sent) > 0


async def test_session_handle_unknown_performative() -> None:
    @dataclass
    class UnknownPerformative:
        pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    # Should log warning but not raise
    await session.handle_performative(UnknownPerformative())


async def test_session_handle_attach_missing_handle() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    # Create attach frame with invalid data to test missing handle case
    attach = AttachFrame(name="test-link", handle=0, role=True)
    attach.handle = None  # type: ignore[assignment]

    # Should log error and return early
    await session._handle_attach(attach)


async def test_session_handle_attach_unknown_link() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    attach = AttachFrame(name="unknown-link", handle=5, role=True)
    # Should log warning for unknown link
    await session._handle_attach(attach)


async def test_session_handle_detach_missing_handle() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    # Create detach frame with invalid data to test missing handle case
    detach = DetachFrame(handle=0)
    detach.handle = None  # type: ignore[assignment]

    # Should return early without error
    await session._handle_detach(detach)


async def test_session_create_sender_link_alias(monkeypatch: Any) -> None:
    @dataclass(slots=True)
    class MockSenderLink:
        name: str
        _handle: int = 0
        _remote_handle: int | None = None

        async def attach(self) -> None:
            pass

        async def wait_ready(self) -> None:
            pass

        async def detach(self) -> None:
            pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    await session._state_machine.transition("send_begin")
    await session._state_machine.transition("recv_begin")

    # Monkeypatch SenderLink to avoid full initialization
    def fake_sender_link(_session: Any, name: str, _address: str, handle: int) -> MockSenderLink:
        return MockSenderLink(name=name, _handle=handle)

    monkeypatch.setattr(links, "SenderLink", fake_sender_link)

    link = await session.create_sender_link("test-address", "test-sender")
    assert link.name == "test-sender"


async def test_session_create_receiver_link_alias(monkeypatch: Any) -> None:
    @dataclass(slots=True)
    class MockReceiverLink:
        name: str
        _handle: int = 0
        _remote_handle: int | None = None

        async def attach(self) -> None:
            pass

        async def wait_ready(self) -> None:
            pass

        async def detach(self) -> None:
            pass

    async def callback(
        _body: bytes,
        _headers: dict[str, Any] | None,
        _delivery_id: int,
        _tag: bytes,
        _link: ReceiverLink,
    ) -> None:
        pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    await session._state_machine.transition("send_begin")
    await session._state_machine.transition("recv_begin")

    # Monkeypatch ReceiverLink to avoid full initialization
    def fake_receiver_link(
        _session: Any,
        name: str,
        _address: str,
        handle: int,
        _callback: Any,
    ) -> MockReceiverLink:
        return MockReceiverLink(name=name, _handle=handle)

    monkeypatch.setattr(links, "ReceiverLink", fake_receiver_link)

    link = await session.create_receiver_link("test-address", "test-receiver", callback)
    assert link.name == "test-receiver"


async def test_session_remove_link() -> None:
    @dataclass(slots=True)
    class MockLink:
        name: str = "test-link"
        _handle: int = 5
        _remote_handle: int | None = 10

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    link = MockLink()
    session._links["test-link"] = link  # type: ignore[assignment]
    session._links_by_handle[5] = link  # type: ignore[assignment]
    session._links_by_remote_handle[10] = link  # type: ignore[assignment]

    session._remove_link(link)  # type: ignore[arg-type]

    assert "test-link" not in session._links
    assert 5 not in session._links_by_handle
    assert 10 not in session._links_by_remote_handle


async def test_session_end_send_frame_error() -> None:
    @dataclass(slots=True)
    class FailingConnection:
        max_frame_size: int = 1024
        sent: list[tuple[int, Any]] = field(default_factory=list)
        is_connected: bool = True

        async def send_performative(self, channel: int, performative: Any) -> None:
            if isinstance(performative, EndFrame):
                raise OSError("Network error")
            self.sent.append((channel, performative))

        def _remove_session(self, channel: int) -> None:
            pass

    connection = FailingConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    await session._state_machine.transition("send_begin")
    await session._state_machine.transition("recv_begin")

    # Should handle send error gracefully
    await session.end()


async def test_session_handle_attach_bytes_name() -> None:
    @dataclass(slots=True)
    class MockLink:
        name: str = "test-link"
        _handle: int = 0
        _remote_handle: int | None = None

        async def _handle_attach(self, attach: AttachFrame) -> None:
            pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    # Add link to session
    link = MockLink()
    session._links["test-link"] = link  # type: ignore[assignment]

    # Create attach with bytes name (will be decoded)
    attach = AttachFrame(name="test-link", handle=5, role=True)
    attach.name = b"test-link"  # type: ignore[assignment]

    await session._handle_attach(attach)

    # Should have set remote handle
    assert 5 in session._links_by_remote_handle


async def test_session_handle_transfer_missing_handle() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    # Create transfer frame with missing handle
    transfer = TransferFrame(handle=0, delivery_id=1)
    transfer.handle = None  # type: ignore[assignment]

    # Should log error and return early
    await session._handle_transfer(transfer)


async def test_session_create_sender_not_mapped(monkeypatch: Any) -> None:
    @dataclass(slots=True)
    class MockSenderLink:
        name: str
        _handle: int = 0
        _remote_handle: int | None = None

        async def attach(self) -> None:
            pass

        async def wait_ready(self) -> None:
            pass

        async def detach(self) -> None:
            pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    # Start in BEGIN_SENT state (not yet mapped)
    await session._state_machine.transition("send_begin")

    # Monkeypatch SenderLink

    def fake_sender_link(_session: Any, name: str, _address: str, handle: int) -> MockSenderLink:
        return MockSenderLink(name=name, _handle=handle)

    monkeypatch.setattr(links, "SenderLink", fake_sender_link)

    # Start create_sender in background (it will wait for ready)
    create_task = asyncio.create_task(session.create_sender("test-address"))

    # Give it time to start waiting
    await asyncio.sleep(0.01)

    # Now complete the mapping
    await session._state_machine.transition("recv_begin")
    session._ready.set()

    # Wait for the create_sender to complete
    link = await create_task
    assert link.name.startswith("sender-test-address")


async def test_session_create_receiver_not_mapped(monkeypatch: Any) -> None:
    @dataclass(slots=True)
    class MockReceiverLink:
        name: str
        _handle: int = 0
        _remote_handle: int | None = None

        async def attach(self) -> None:
            pass

        async def wait_ready(self) -> None:
            pass

        async def detach(self) -> None:
            pass

    async def callback(
        _body: bytes,
        _headers: dict[str, Any] | None,
        _delivery_id: int,
        _tag: bytes,
        _link: ReceiverLink,
    ) -> None:
        pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    # Start in BEGIN_SENT state (not yet mapped)
    await session._state_machine.transition("send_begin")

    # Monkeypatch ReceiverLink

    def fake_receiver_link(
        _session: Any,
        name: str,
        _address: str,
        handle: int,
        _callback: Any,
    ) -> MockReceiverLink:
        return MockReceiverLink(name=name, _handle=handle)

    monkeypatch.setattr(links, "ReceiverLink", fake_receiver_link)

    # Start create_receiver in background (it will wait for ready)
    create_task = asyncio.create_task(session.create_receiver("test-address", callback))

    # Give it time to start waiting
    await asyncio.sleep(0.01)

    # Now complete the mapping
    await session._state_machine.transition("recv_begin")
    session._ready.set()

    # Wait for the create_receiver to complete
    link = await create_task
    assert link.name.startswith("receiver-test-address")


async def test_session_create_sender_default_name(monkeypatch: Any) -> None:
    @dataclass(slots=True)
    class MockSenderLink:
        name: str
        _handle: int = 0
        _remote_handle: int | None = None

        async def attach(self) -> None:
            pass

        async def wait_ready(self) -> None:
            pass

        async def detach(self) -> None:
            pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    await session._state_machine.transition("send_begin")
    await session._state_machine.transition("recv_begin")

    # Monkeypatch SenderLink

    def fake_sender_link(_session: Any, name: str, _address: str, handle: int) -> MockSenderLink:
        return MockSenderLink(name=name, _handle=handle)

    monkeypatch.setattr(links, "SenderLink", fake_sender_link)

    # Don't provide name, should generate default
    link = await session.create_sender("test-address", name=None)
    assert link.name == "sender-test-address-0"


async def test_session_create_receiver_default_name(monkeypatch: Any) -> None:
    @dataclass(slots=True)
    class MockReceiverLink:
        name: str
        _handle: int = 0
        _remote_handle: int | None = None

        async def attach(self) -> None:
            pass

        async def wait_ready(self) -> None:
            pass

        async def detach(self) -> None:
            pass

    async def callback(
        _body: bytes,
        _headers: dict[str, Any] | None,
        _delivery_id: int,
        _tag: bytes,
        _link: ReceiverLink,
    ) -> None:
        pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    await session._state_machine.transition("send_begin")
    await session._state_machine.transition("recv_begin")

    # Monkeypatch ReceiverLink

    def fake_receiver_link(
        _session: Any,
        name: str,
        _address: str,
        handle: int,
        _callback: Any,
    ) -> MockReceiverLink:
        return MockReceiverLink(name=name, _handle=handle)

    monkeypatch.setattr(links, "ReceiverLink", fake_receiver_link)

    # Don't provide name, should generate default
    link = await session.create_receiver("test-address", callback, name=None)
    assert link.name == "receiver-test-address-0"


async def test_session_create_sender_not_usable_race() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    await session._state_machine.transition("send_begin")
    await session._state_machine.transition("recv_begin")

    # Force state to UNMAPPED (simulates invalidation after mapping)
    # This makes is_mapped=False (skips wait_ready due to _ready being set)
    # and is_usable=False (triggers the error)
    session._ready.set()  # Set ready so wait_ready succeeds immediately
    session._state_machine.reset(SessionState.UNMAPPED)

    with pytest.raises(SessionError, match="not usable"):
        await session.create_sender("test-address")


async def test_session_create_receiver_not_usable_race() -> None:
    async def callback(
        _body: bytes,
        _headers: dict[str, Any] | None,
        _delivery_id: int,
        _tag: bytes,
        _link: ReceiverLink,
    ) -> None:
        pass

    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)
    await session._state_machine.transition("send_begin")
    await session._state_machine.transition("recv_begin")

    # Force state to UNMAPPED (simulates invalidation after mapping)
    session._ready.set()  # Set ready so wait_ready succeeds immediately
    session._state_machine.reset(SessionState.UNMAPPED)

    with pytest.raises(SessionError, match="not usable"):
        await session.create_receiver("test-address", callback)


async def test_session_begin_and_end() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    await session.begin()
    assert isinstance(connection.sent[0][1], BeginFrame)

    session._state_machine.transition_sync("recv_begin")

    class DummyLink:
        name = "link"
        _handle = 0
        _remote_handle: int | None = None

        async def detach(self) -> None:
            raise OSError("detach failed")

    session._links["link"] = cast(SenderLink | ReceiverLink, DummyLink())
    await session.end()


async def test_session_handle_attach_detach_flow_disposition() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    await session.begin()
    session._state_machine.transition_sync("recv_begin")

    class FakeLinkForAttach:
        name = "test-link"
        _remote_handle: int | None = None
        detached = False

        async def _handle_attach(self, _attach: AttachFrame) -> None:
            pass

        async def _handle_detach(self, _detach: DetachFrame) -> None:
            self.detached = True

        async def _handle_flow(self, _flow: FlowFrame) -> None:
            pass

        def invalidate(self) -> None:
            self.detached = True

    # Test attach handler
    attach = AttachFrame(name="test-link", handle=5, role=False, source=None, target=None)
    link = FakeLinkForAttach()
    session._links["test-link"] = cast(SenderLink | ReceiverLink, link)
    await session.handle_performative(attach)
    assert link._remote_handle == 5
    assert session._links_by_remote_handle[5] == link

    # Test flow handler with session-level flow
    flow = FlowFrame(
        next_incoming_id=10,
        incoming_window=100,
        next_outgoing_id=1,
        outgoing_window=50,
    )
    await session.handle_performative(flow)

    # Test flow handler with link-level flow
    flow_link = FlowFrame(
        handle=5,
        next_incoming_id=10,
        incoming_window=100,
        next_outgoing_id=1,
        outgoing_window=50,
    )
    await session.handle_performative(flow_link)

    # Test disposition handler
    disposition = DispositionFrame(role=True, first=1, settled=True)
    await session.handle_performative(disposition)

    # Test detach handler
    detach = DetachFrame(handle=5)
    await session.handle_performative(detach)
    assert link.detached

    # Test attach with unknown link (warning path)
    attach_unknown = AttachFrame(name="unknown", handle=10, role=False, source=None, target=None)
    await session.handle_performative(attach_unknown)

    # Test detach with unknown handle (warning path)
    detach_unknown = DetachFrame(handle=99)
    await session.handle_performative(detach_unknown)


async def test_session_handle_transfer_and_invalidate(monkeypatch: Any) -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    await session.begin()
    session._state_machine.transition_sync("recv_begin")

    # Create fake receiver link without slots to allow dynamic attributes
    transferred: list[Any] = []

    class FakeReceiverForTest:
        def __init__(self) -> None:
            self._remote_handle: int | None = 3
            self.detached = False

        async def _handle_transfer(self, frame: TransferFrame) -> None:
            transferred.append(frame)

        def invalidate(self) -> None:
            self.detached = True

    fake_receiver = FakeReceiverForTest()

    # Import to get the ReceiverLink class for mocking isinstance

    # Mock isinstance to return True for our fake receiver
    original_isinstance = isinstance

    def mock_isinstance(obj: Any, classinfo: Any) -> bool:
        if obj is fake_receiver and classinfo is ReceiverLink:
            return True
        return original_isinstance(obj, classinfo)

    monkeypatch.setattr("builtins.isinstance", mock_isinstance)

    session._links["receiver"] = cast(SenderLink | ReceiverLink, fake_receiver)
    session._links_by_remote_handle[3] = cast(SenderLink | ReceiverLink, fake_receiver)

    # Test transfer handler
    transfer = TransferFrame(handle=3, delivery_id=1, delivery_tag=b"tag1")
    await session.handle_performative(transfer)
    assert len(transferred) == 1

    # Test transfer with unknown handle (warning path)
    transfer_unknown = TransferFrame(handle=99, delivery_id=2, delivery_tag=b"tag2")
    await session.handle_performative(transfer_unknown)

    # Test transfer on sender link (warning path)
    class FakeSenderForTest:
        def __init__(self) -> None:
            self._remote_handle: int | None = 4

        def invalidate(self) -> None:
            pass

    fake_sender = FakeSenderForTest()
    session._links["sender"] = cast(SenderLink | ReceiverLink, fake_sender)
    session._links_by_remote_handle[4] = cast(SenderLink | ReceiverLink, fake_sender)
    transfer_sender = TransferFrame(handle=4, delivery_id=3, delivery_tag=b"tag3")
    await session.handle_performative(transfer_sender)

    # Test invalidate
    session.invalidate()
    assert session.state == SessionState.UNMAPPED
    assert fake_receiver.detached


async def test_session_wait_ready_timeout() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    await session.begin()

    with pytest.raises(asyncio.TimeoutError):
        await session.wait_ready(timeout=0.01)


async def test_session_handle_end_various_states() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    await session.begin()
    session._state_machine.transition_sync("recv_begin")

    # Test handle end in END_RCVD state
    end = EndFrame()
    await session.handle_performative(end)


async def test_session_create_sender_not_ready() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=1)

    # Session not yet ready - should fail or wait
    with pytest.raises((SessionError, asyncio.TimeoutError)):
        await session.create_sender("queue", name="test-sender")


async def test_session_properties() -> None:
    connection = FakeConnection()
    session = Session(cast(AmqpConnection, connection), channel=5)

    assert session.connection == connection
    assert session.channel == 5
    assert session.state == SessionState.UNMAPPED
    assert not session.is_mapped
    assert not session.is_usable
