import asyncio
import contextlib
import dataclasses
import json
import logging
import struct
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar, cast

from ._pyamqp._decode import decode_frame, decode_payload
from ._pyamqp._encode import encode_frame, encode_payload
from ._pyamqp.constants import FIELD
from ._pyamqp.message import Message
from ._pyamqp.performatives import (
    AttachFrame,
    BeginFrame,
    CloseFrame,
    DetachFrame,
    DispositionFrame,
    EndFrame,
    FlowFrame,
    OpenFrame,
    SASLChallenge,
    SASLInit,
    SASLMechanism,
    SASLOutcome,
    SASLResponse,
    TransferFrame,
)
from ._pyamqp.types import AMQPTypes

logger = logging.getLogger(__name__)

AMQP_HEADER = b"AMQP\x00\x01\x00\x00"
SASL_HEADER = b"AMQP\x03\x01\x00\x00"

FRAME_TYPE_AMQP = 0x00
FRAME_TYPE_SASL = 0x01

PERFORMATIVES = {
    16: OpenFrame,
    17: BeginFrame,
    18: AttachFrame,
    19: FlowFrame,
    20: TransferFrame,
    21: DispositionFrame,
    22: DetachFrame,
    23: EndFrame,
    24: CloseFrame,
    64: SASLMechanism,
    65: SASLInit,
    66: SASLChallenge,
    67: SASLResponse,
    68: SASLOutcome,
}


def fields_to_performative(frame_type: int, fields: list[Any]) -> Any:
    cls = PERFORMATIVES.get(frame_type)
    if not cls:
        raise ValueError(f"Unknown frame type: {frame_type}")

    # Get field names from dataclass
    field_names = [f.name for f in dataclasses.fields(cls)]

    payload = None
    if frame_type == 20:  # TransferFrame
        # The last element in fields is the payload (bytes)
        if fields:
            payload = fields.pop()

    # Map fields to kwargs
    kwargs = {}
    # For TransferFrame, 'payload' is in field_names but not in the AMQP list fields
    target_names = [n for n in field_names if n != "payload"]

    for name, value in zip(target_names, fields):
        if value is not None:
            kwargs[name] = value

    if payload is not None:
        kwargs["payload"] = payload

    return cls(**kwargs)


class AmqpError(Exception):
    pass


class ConnectionClosedError(AmqpError):
    pass


class AmqpConnection:
    def __init__(
        self, host: str, port: int, username: str | None = None, password: str | None = None
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._connected = False
        self.max_frame_size = 4096  # Default
        self.channel_max = 65535
        self.sessions: dict[int, "Session"] = {}
        self._incoming_task: asyncio.Task | None = None

    async def connect(self) -> None:
        if self.username is not None:
            await self._connect_sasl()
            return

        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self._connected = True

        # Send AMQP Header
        self.writer.write(AMQP_HEADER)
        await self.writer.drain()

        try:
            response = await self.reader.readexactly(8)
        except asyncio.IncompleteReadError:
            raise AmqpError("Connection closed during handshake") from None

        if response == SASL_HEADER:
            # Server requires SASL. Reconnect and do SASL.
            self.writer.close()
            await self.writer.wait_closed()
            await self._connect_sasl()
            return

        if response != AMQP_HEADER:
            raise AmqpError(f"Unexpected protocol header: {response!r}")

        # Start reading frames
        self._incoming_task = asyncio.create_task(self._read_loop())

        # Send Open frame
        open_frame = OpenFrame(container_id="repid-client", hostname=self.host)
        await self.send_performative(0, open_frame)

    async def _connect_sasl(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self._connected = True

        self.writer.write(SASL_HEADER)
        await self.writer.drain()

        response = await self.reader.readexactly(8)
        if response != SASL_HEADER:
            raise AmqpError(f"Unexpected SASL header response: {response!r}")

        await self._sasl_handshake()

        # Start reading frames
        self._incoming_task = asyncio.create_task(self._read_loop())

        # Send Open frame
        open_frame = OpenFrame(container_id="repid-client", hostname=self.host)
        await self.send_performative(0, open_frame)

    async def _sasl_handshake(self) -> None:
        # Wait for SASL-MECHANISMS
        _, frame_type, fields = await self._read_frame()
        if frame_type != 64:  # SASL-MECHANISMS
            raise AmqpError(f"Expected SASL-MECHANISMS, got {frame_type}")

        mechanisms_frame = fields_to_performative(frame_type, fields)
        mechanisms = mechanisms_frame.sasl_server_mechanisms

        # Choose mechanism (PLAIN or ANONYMOUS)
        chosen_mech = b"ANONYMOUS"
        initial_response = None

        mech_names = [m.decode() if isinstance(m, bytes) else m for m in mechanisms]

        if "PLAIN" in mech_names and self.username and self.password:
            chosen_mech = b"PLAIN"
            # SASL PLAIN: authorization-id \0 authentication-id \0 passwd
            initial_response = f"\0{self.username}\0{self.password}".encode()
        elif "ANONYMOUS" not in mech_names:
            raise AmqpError(f"No supported SASL mechanism found. Server offered: {mechanisms}")

        # Send SASL-INIT
        sasl_init = SASLInit(
            mechanism=chosen_mech, initial_response=initial_response, hostname=self.host
        )
        await self.send_performative(0, sasl_init, frame_type=FRAME_TYPE_SASL)

        # Wait for SASL-OUTCOME
        while True:
            _, frame_type, fields = await self._read_frame()

            if frame_type == 66:  # SASL-CHALLENGE
                # TODO: Handle challenge
                pass
            elif frame_type == 68:  # SASL-OUTCOME
                outcome = fields_to_performative(frame_type, fields)
                if outcome.code != 0:  # OK
                    raise AmqpError(f"SASL Authentication failed with code {outcome.code}")
                break
            else:
                raise AmqpError(f"Unexpected SASL frame: {frame_type}")

        # SASL Done. Now reset and start AMQP handshake.
        self.writer.write(AMQP_HEADER)
        await self.writer.drain()

        response = await self.reader.readexactly(8)
        if response != AMQP_HEADER:
            raise AmqpError(f"Unexpected protocol header after SASL: {response!r}")

    async def _read_frame(self) -> tuple[int, int, list[Any]]:
        if self.reader is None:
            raise AmqpError("Not connected")

        try:
            header_data = await self.reader.readexactly(8)
        except asyncio.IncompleteReadError:
            raise ConnectionClosedError("Connection closed by server") from None

        size, doff, frame_type, channel = struct.unpack(">IBBH", header_data)

        if size < 8:
            raise AmqpError(f"Invalid frame size: {size}")

        body_size = size - 8
        payload = await self.reader.readexactly(body_size)

        data_offset = doff * 4
        if data_offset > 8:
            extended_header_size = data_offset - 8
            if extended_header_size > len(payload):
                raise AmqpError("Invalid data offset")
            payload = payload[extended_header_size:]

        ft, fields = decode_frame(memoryview(payload))
        return channel, ft, fields

    async def _read_loop(self) -> None:
        while self._connected:
            try:
                channel, frame_type, fields = await self._read_frame()

                performative = fields_to_performative(frame_type, fields)
                await self.handle_performative(channel, performative, payload=None)

            except ConnectionClosedError:
                logger.info("Connection closed.")
                self._connected = False
                break
            except Exception as e:
                logger.error("Error in read loop", extra={"error": e})
                self._connected = False
                break

    async def send_performative(
        self,
        channel: int,
        performative: Any,
        payload: bytes | None = None,
        frame_type: int = FRAME_TYPE_AMQP,
    ) -> None:
        if self.writer is None:
            raise AmqpError("Not connected")

        if isinstance(performative, TransferFrame):
            performative.payload = payload or b""

        ft_bytes = b"\x00" if frame_type == FRAME_TYPE_AMQP else b"\x01"

        header, body = encode_frame(performative, frame_type=ft_bytes)

        # encode_frame returns 6 bytes header (Size, Doff, Type). We need to add Channel (2 bytes).
        channel_bytes = struct.pack(">H", channel)
        full_header = header + channel_bytes

        self.writer.write(full_header)
        if body:
            self.writer.write(body)
        await self.writer.drain()

    async def handle_performative(
        self,
        channel: int,
        performative: Any,
        payload: bytes | None = None,
    ) -> None:
        logger.debug("Received performative: %s", performative)
        if isinstance(performative, OpenFrame):
            # Handle Open response
            pass
        elif isinstance(performative, BeginFrame):
            # Handle Begin response
            if channel in self.sessions:
                # TODO: Handle session begin response
                pass
        elif isinstance(performative, CloseFrame):
            await self.close()
        else:
            # Dispatch to session
            if channel in self.sessions:
                await self.sessions[channel].handle_performative(performative, payload)

    async def close(self) -> None:
        if not self._connected:
            return

        try:
            await self.send_performative(0, CloseFrame())
        except Exception:
            pass

        self._connected = False
        if self._incoming_task:
            self._incoming_task.cancel()
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    def create_session(self) -> "Session":
        # Find free channel
        channel = 0
        while channel in self.sessions:
            channel += 1
        session = Session(self, channel)
        self.sessions[channel] = session
        return session


@dataclass(slots=True, kw_only=True)
class Source:
    _code: ClassVar[int] = 0x00000028
    _definition: ClassVar[tuple[FIELD, ...]] = (
        FIELD("address", AMQPTypes.string, mandatory=False, default=None, multiple=False),
        FIELD("durable", AMQPTypes.uint, mandatory=False, default=0, multiple=False),
        FIELD(
            "expiry_policy",
            AMQPTypes.symbol,
            mandatory=False,
            default="session-end",
            multiple=False,
        ),
        FIELD("timeout", AMQPTypes.uint, mandatory=False, default=0, multiple=False),
        FIELD("dynamic", AMQPTypes.boolean, mandatory=False, default=False, multiple=False),
        FIELD(
            "dynamic_node_properties", AMQPTypes.map, mandatory=False, default=None, multiple=False
        ),
        FIELD("distribution_mode", AMQPTypes.symbol, mandatory=False, default=None, multiple=False),
        FIELD("filter", AMQPTypes.map, mandatory=False, default=None, multiple=False),
        FIELD(
            "default_outcome", AMQPTypes.described, mandatory=False, default=None, multiple=False
        ),
        FIELD("outcomes", AMQPTypes.symbol, mandatory=False, default=None, multiple=True),
        FIELD("capabilities", AMQPTypes.symbol, mandatory=False, default=None, multiple=True),
    )

    address: str | None = None
    durable: int | None = 0
    expiry_policy: str | None = "session-end"
    timeout: int | None = 0
    dynamic: bool | None = False
    dynamic_node_properties: dict | None = None
    distribution_mode: str | None = None
    filter: dict | None = None
    default_outcome: Any | None = None
    outcomes: list[str] | None = None
    capabilities: list[str] | None = None


@dataclass(slots=True, kw_only=True)
class Target:
    _code: ClassVar[int] = 0x00000029
    _definition: ClassVar[tuple[FIELD, ...]] = (
        FIELD("address", AMQPTypes.string, mandatory=False, default=None, multiple=False),
        FIELD("durable", AMQPTypes.uint, mandatory=False, default=0, multiple=False),
        FIELD(
            "expiry_policy",
            AMQPTypes.symbol,
            mandatory=False,
            default="session-end",
            multiple=False,
        ),
        FIELD("timeout", AMQPTypes.uint, mandatory=False, default=0, multiple=False),
        FIELD("dynamic", AMQPTypes.boolean, mandatory=False, default=False, multiple=False),
        FIELD(
            "dynamic_node_properties", AMQPTypes.map, mandatory=False, default=None, multiple=False
        ),
        FIELD("capabilities", AMQPTypes.symbol, mandatory=False, default=None, multiple=True),
    )

    address: str | None = None
    durable: int | None = 0
    expiry_policy: str | None = "session-end"
    timeout: int | None = 0
    dynamic: bool | None = False
    dynamic_node_properties: dict | None = None
    capabilities: list[str] | None = None


class Session:
    def __init__(self, connection: AmqpConnection, channel: int):
        self.connection = connection
        self.channel = channel
        self.links: dict[str, "Link"] = {}  # Map by name
        self.links_by_handle: dict[int, "Link"] = {}
        self.links_by_remote_handle: dict[int, "Link"] = {}
        self.next_outgoing_id = 0
        self.incoming_window = 100
        self.outgoing_window = 100
        self._ready = asyncio.Event()

    async def begin(self) -> None:
        begin = BeginFrame(
            next_outgoing_id=self.next_outgoing_id,
            incoming_window=self.incoming_window,
            outgoing_window=self.outgoing_window,
        )
        await self.connection.send_performative(self.channel, begin)

    async def handle_performative(
        self,
        performative: Any,
        payload: bytes | None,
    ) -> None:
        logger.debug("Session %d handling performative: %s", self.channel, performative)
        if isinstance(performative, BeginFrame):
            self._ready.set()
        elif isinstance(performative, AttachFrame):
            # Handle Attach
            name = performative.name
            if isinstance(name, bytes):
                name = name.decode()
            remote_handle = performative.handle
            if remote_handle is None:
                raise AmqpError("Attach frame missing handle")
            if name in self.links:
                link = self.links[name]
                link.remote_handle = remote_handle
                self.links_by_remote_handle[remote_handle] = link
        elif isinstance(performative, FlowFrame):
            # Handle Flow
            pass
        elif isinstance(performative, TransferFrame):
            # Handle Transfer
            transfer_handle = performative.handle
            if transfer_handle is None:
                raise AmqpError("Transfer frame missing handle")
            if transfer_handle in self.links_by_remote_handle:
                link = self.links_by_remote_handle[transfer_handle]
                if isinstance(link, ReceiverLink):
                    await link.handle_transfer(performative, payload or performative.payload)
            else:
                logger.debug(
                    "No link found for handle %s. Known handles: %s",
                    transfer_handle,
                    list(self.links_by_remote_handle.keys()),
                )
        elif isinstance(performative, DispositionFrame):
            # Handle Disposition
            pass
        elif isinstance(performative, DetachFrame):
            # Handle Detach
            pass
        elif isinstance(performative, EndFrame):
            # Handle End
            pass

    async def create_sender_link(self, address: str, name: str) -> "SenderLink":
        link = SenderLink(self, name, address)
        await link.attach()
        return link

    async def create_receiver_link(
        self,
        address: str,
        name: str,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, "ReceiverLink"], Any],
    ) -> "ReceiverLink":
        link = ReceiverLink(self, name, address, callback)
        await link.attach()
        return link


class Link:
    def __init__(self, session: Session, name: str, address: str, role: bool):
        self.session = session
        self.name = name
        self.address = address
        self.role = role  # False=Sender, True=Receiver
        self.handle = 0  # Need to manage handles in Session
        # Assign handle
        while self.handle in self.session.links_by_handle:
            self.handle += 1
        self.session.links_by_handle[self.handle] = self
        self.session.links[name] = self
        self.remote_handle: int | None = None

    async def attach(self) -> None:
        # Abstract
        pass


class SenderLink(Link):
    def __init__(self, session: Session, name: str, address: str):
        super().__init__(session, name, address, role=False)
        self.delivery_count = 0

    async def attach(self) -> None:
        target = Target(address=self.address)
        source = Source(address="client-source")

        attach = AttachFrame(
            name=self.name,
            handle=self.handle,
            role=self.role,
            source=cast(Any, source),
            target=cast(Any, target),
            initial_delivery_count=0,
        )
        await self.session.connection.send_performative(self.session.channel, attach)

    async def send(self, message_payload: bytes, headers: dict[str, Any] | None = None) -> None:
        # Send Transfer
        transfer = TransferFrame(
            handle=self.handle,
            delivery_id=self.delivery_count,
            delivery_tag=str(self.delivery_count).encode(),
            message_format=0,
            settled=True,
        )
        self.delivery_count += 1

        # Wrap payload in Message and encode
        msg = Message(
            body=message_payload,
            application_properties=cast(dict[str | bytes, Any] | None, headers),
        )
        encoded_payload = encode_payload(bytearray(), msg)

        await self.session.connection.send_performative(
            self.session.channel,
            transfer,
            payload=encoded_payload,
        )


class ReceiverLink(Link):
    def __init__(
        self,
        session: Session,
        name: str,
        address: str,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, "ReceiverLink"], Any],
    ):
        super().__init__(session, name, address, role=True)
        self.callback = callback

    async def attach(self) -> None:
        source = Source(address=self.address)
        target = Target(address="client-target")

        attach = AttachFrame(
            name=self.name,
            handle=self.handle,
            role=self.role,
            source=cast(Any, source),
            target=cast(Any, target),
        )
        await self.session.connection.send_performative(self.session.channel, attach)

        # Send Flow to grant credit
        flow = FlowFrame(
            next_incoming_id=0,
            incoming_window=100,
            next_outgoing_id=0,
            outgoing_window=100,
            handle=self.handle,
            delivery_count=0,
            link_credit=100,
            available=0,
            drain=False,
            echo=False,
            properties=None,
        )
        await self.session.connection.send_performative(self.session.channel, flow)

    async def handle_transfer(self, transfer: TransferFrame, payload: Any) -> None:
        if payload:
            buf = payload if isinstance(payload, memoryview) else memoryview(payload)
            msg = decode_payload(buf)

            # Extract body/value
            body = msg.body or msg.value

            if isinstance(body, str):
                body = body.encode()
            elif not isinstance(body, bytes) and body is not None:
                body = json.dumps(body).encode()

            # Cast application_properties to dict[str, Any] as that's what callback expects
            headers = cast(dict[str, Any] | None, msg.application_properties)

            await self.callback(
                body or b"",
                headers,
                transfer.delivery_id or 0,
                transfer.delivery_tag or b"",
                self,
            )
