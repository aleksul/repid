from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from repid.connections.amqp.protocol.transport import AMQP_HEADER, SASL_HEADER


@dataclass(slots=True)
class FakeConnection:
    max_frame_size: int = 1024
    sent: list[tuple[int, Any]] = field(default_factory=list)
    is_connected: bool = True

    async def send_performative(self, channel: int, performative: Any) -> None:
        self.sent.append((channel, performative))

    def _remove_session(self, _channel: int) -> None:
        pass


@dataclass(slots=True)
class FakeSession:
    connection: FakeConnection
    channel: int = 1

    def _remove_link(self, _link: Any) -> None:
        return None

    async def end(self) -> None:
        return None

    def invalidate(self) -> None:
        return None

    async def handle_performative(self, _performative: Any) -> None:
        return None


@dataclass(slots=True)
class FakeTransport:
    protocol_headers: list[bytes]
    frames: list[tuple[int, Any]]
    sent_frames: list[tuple[int, Any]] = field(default_factory=list)
    amqp_headers_sent: int = 0
    sasl_headers_sent: int = 0
    closed: bool = False

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def send_amqp_header(self) -> None:
        self.amqp_headers_sent += 1

    async def send_sasl_header(self) -> None:
        self.sasl_headers_sent += 1

    async def read_protocol_header(self) -> bytes:
        if self.protocol_headers:
            return self.protocol_headers.pop(0)
        raise RuntimeError("No protocol headers configured")

    async def send_performative(self, channel: int, performative: Any) -> None:
        self.sent_frames.append((channel, performative))

    async def read_frame(self) -> tuple[int, Any]:
        if self.frames:
            return self.frames.pop(0)
        raise RuntimeError("No frames configured")

    def validate_amqp_header(self, header: bytes) -> bool:
        return header == AMQP_HEADER

    def validate_sasl_header(self, header: bytes) -> bool:
        return header == SASL_HEADER


@dataclass(slots=True)
class FakeSenderLink:
    session: Any
    name: str = "sender"
    is_sender: bool = True
    _usable: bool = True

    def is_usable(self) -> bool:
        return self._usable


@dataclass(slots=True)
class FakeReceiverLink:
    session: Any
    name: str = "receiver"
    is_sender: bool = False
    _usable: bool = True

    def is_usable(self) -> bool:
        return self._usable


@dataclass(slots=True)
class FakeSessionForPools:
    connection: FakeConnection
    channel: int = 1
    sender_links: list[FakeSenderLink] = field(default_factory=list)
    receiver_links: list[FakeReceiverLink] = field(default_factory=list)
    is_usable_value: bool = True
    _sender_link_factory: Any = None

    def is_usable(self) -> bool:
        return self.is_usable_value

    async def create_sender(self, address: str) -> FakeSenderLink:
        if self._sender_link_factory:
            return self._sender_link_factory(address)  # type: ignore[no-any-return]
        if self.sender_links:
            return self.sender_links.pop(0)
        return FakeSenderLink(session=self, name=f"sender-{address}")

    async def create_receiver(self, address: str) -> FakeReceiverLink:
        if self.receiver_links:
            return self.receiver_links.pop(0)
        return FakeReceiverLink(session=self, name=f"receiver-{address}")

    def invalidate(self) -> None:
        self.is_usable_value = False


@dataclass(slots=True)
class FakeManagedConnection:
    is_connected: bool = True
    _sessions: dict[int, Any] = field(default_factory=dict)

    async def create_session(self) -> FakeSessionForPools:
        return FakeSessionForPools(connection=FakeConnection(), channel=1)


@pytest.fixture
def fake_connection() -> FakeConnection:
    return FakeConnection()


@pytest.fixture
def fake_session(fake_connection: FakeConnection) -> FakeSession:
    return FakeSession(connection=fake_connection, channel=1)
