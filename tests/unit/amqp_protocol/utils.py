import asyncio
import struct
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from repid.connections.amqp.protocol.connection import AmqpConnection
from repid.connections.amqp.protocol.events import EventEmitter
from repid.connections.amqp.protocol.links import LinkError, ReceiverLink
from repid.connections.amqp.protocol.transport import AMQP_HEADER, SASL_HEADER


@dataclass
class FakeConnection:
    max_frame_size: int = 1024
    sent: list[tuple[int, Any]] = field(default_factory=list)
    is_connected: bool = True

    async def send_performative(self, channel: int, performative: Any) -> None:
        self.sent.append((channel, performative))

    def _remove_session(self, channel: int) -> None:
        pass


@dataclass
class FakeSession:
    connection: FakeConnection
    channel: int = 1
    _next_incoming_id: int = 0
    _next_outgoing_id: int = 0
    _incoming_window: int = 100
    _outgoing_window: int = 100

    def _remove_link(self, _link: Any) -> None:
        return None

    async def end(self) -> None:
        return None

    def invalidate(self) -> None:
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
        return self.protocol_headers.pop(0)

    def validate_amqp_header(self, header: bytes) -> bool:
        return header == AMQP_HEADER

    def validate_sasl_header(self, header: bytes) -> bool:
        return header == SASL_HEADER

    async def read_frame(self) -> tuple[int, Any]:
        return self.frames.pop(0)

    async def send_performative(self, channel: int, performative: Any) -> None:
        self.sent_frames.append((channel, performative))


@dataclass(slots=True)
class FakeSocket:
    def setsockopt(self, *_args: Any, **_kwargs: Any) -> None:
        return None


@dataclass
class FakeStreamWriter:
    data: bytearray = field(default_factory=bytearray)
    closed: bool = False
    socket: FakeSocket = field(default_factory=FakeSocket)

    def write(self, data: bytes) -> None:
        self.data.extend(data)

    async def drain(self) -> None:
        return None

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None

    def get_extra_info(self, name: str) -> Any:
        if name == "socket":
            return self.socket
        return None


@dataclass(slots=True)
class FakeStreamReader:
    data: bytearray

    async def readexactly(self, n: int) -> bytes:
        if len(self.data) < n:
            partial = bytes(self.data)
            self.data.clear()
            raise asyncio.IncompleteReadError(partial=partial, expected=n)
        result = bytes(self.data[:n])
        del self.data[:n]
        return result


def _build_frame_bytes(body: bytes = b"", *, channel: int = 0) -> bytes:
    size = 8 + len(body)
    header = struct.pack(
        ">I B B H",
        size,
        2,
        0,
        channel,
    )
    return header + body


def _set_connection_open(connection: AmqpConnection) -> None:
    connection._state_machine.transition_sync("send_header")
    connection._state_machine.transition_sync("recv_header")
    connection._state_machine.transition_sync("send_open")
    connection._state_machine.transition_sync("recv_open")


@dataclass(slots=True)
class FakeSenderLink:
    should_fail: bool = False
    is_usable: bool = True
    sent: list[bytes] = field(default_factory=list)
    detached: bool = False

    async def send(self, payload: bytes, **_kwargs: Any) -> None:
        if self.should_fail:
            raise LinkError("send failed")
        self.sent.append(payload)

    async def detach(self) -> None:
        self.detached = True


@dataclass(slots=True)
class FakeReceiverLink:
    handle: int = 1
    is_usable: bool = True
    detached: bool = False

    async def detach(self) -> None:
        self.detached = True


@dataclass(slots=True)
class FakeSessionForPools:
    connection: Any
    sender_links: list[FakeSenderLink]
    receiver_links: list[FakeReceiverLink]
    is_usable: bool = True
    _sender_link_factory: Callable[[], FakeSenderLink] | None = None

    async def create_sender(self, _address: str, _name: str) -> FakeSenderLink:
        if self.sender_links:
            return self.sender_links.pop(0)
        if self._sender_link_factory:
            return self._sender_link_factory()
        # Fallback to default link
        return FakeSenderLink()

    async def create_receiver(
        self,
        _address: str,
        _callback: Callable[[bytes, dict[str, Any] | None, int, bytes, ReceiverLink], Any],
        _name: str,
        prefetch: int = 100,  # noqa: ARG002
    ) -> FakeReceiverLink:
        return self.receiver_links.pop(0)

    async def end(self) -> None:
        return None


@dataclass(slots=True)
class FakeManagedConnection:
    is_connected: bool
    session_factory: Callable[[], FakeSessionForPools]
    events: EventEmitter = field(default_factory=EventEmitter)

    async def create_session(self) -> FakeSessionForPools:
        return self.session_factory()
