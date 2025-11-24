import abc
import asyncio
import contextlib
import json
import logging
import socket
import ssl
import struct
import uuid
from collections.abc import Callable
from typing import Any, Literal, cast

from repid.connections.amqp._uamqp._decode import (
    bytes_to_performative,
    transfer_frames_to_message,
)
from repid.connections.amqp._uamqp._encode import (
    message_to_transfer_frames,
    performative_to_bytes,
)
from repid.connections.amqp._uamqp.constants import MAX_FRAME_SIZE_BYTES, MIN_MAX_FRAME_SIZE
from repid.connections.amqp._uamqp.endpoints import Source, Target
from repid.connections.amqp._uamqp.message import Message
from repid.connections.amqp._uamqp.performatives import (
    AttachFrame,
    BeginFrame,
    CloseFrame,
    DetachFrame,
    DispositionFrame,
    EndFrame,
    FlowFrame,
    OpenFrame,
    Performative,
    SASLChallenge,
    SASLInit,
    SASLMechanism,
    SASLOutcome,
    TransferFrame,
)

logger = logging.getLogger(__name__)

AMQP_HEADER = b"AMQP\x00\x01\x00\x00"
SASL_HEADER = b"AMQP\x03\x01\x00\x00"
TLS_HEADER = b"AMQP\x02\x01\x00\x00"

FRAME_TYPE_AMQP = 0x00
FRAME_TYPE_SASL = 0x01
FRAME_TYPE_TLS = 0x02

FRAME_HEADER_SIZE = 8


class AmqpError(Exception):
    pass


class ConnectionClosedError(AmqpError):
    pass


class AmqpTransport:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        ssl_context: ssl.SSLContext | None = None,
        connect_timeout: float = 10.0,
        tcp_keepalive: bool = True,
    ):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.connect_timeout = connect_timeout
        self.tcp_keepalive = tcp_keepalive
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port, ssl=self.ssl_context),
                timeout=self.connect_timeout,
            )
        except asyncio.TimeoutError:
            raise AmqpError(f"Connection timed out after {self.connect_timeout}s") from None
        except OSError as exc:
            raise AmqpError(f"Could not connect to {self.host}:{self.port}: {exc}") from exc

        if self.tcp_keepalive and self.writer:
            sock = self.writer.get_extra_info("socket")
            if sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                # Platform specific keepalive settings could go here (e.g. TCP_KEEPIDLE)

    async def read_exactly(self, n: int) -> bytes:
        if self.reader is None:
            raise ConnectionClosedError("Not connected")
        try:
            return await self.reader.readexactly(n)
        except asyncio.IncompleteReadError:
            raise ConnectionClosedError("Connection closed by server") from None

    async def read_frame(self) -> tuple[int, Performative]:
        if self.reader is None:
            raise ConnectionClosedError("Not connected")

        try:
            header_data = await self.reader.readexactly(8)
        except asyncio.IncompleteReadError:
            raise ConnectionClosedError("Connection closed by server") from None

        size, doff, _, channel = struct.unpack(">IBBH", header_data)

        if size < FRAME_HEADER_SIZE:
            raise AmqpError(f"Invalid frame size: {size}")

        body_size = size - FRAME_HEADER_SIZE
        payload = await self.reader.readexactly(body_size)

        data_offset = doff * 4
        if data_offset > FRAME_HEADER_SIZE:
            extended_header_size = data_offset - FRAME_HEADER_SIZE
            if extended_header_size > len(payload):
                raise AmqpError("Invalid data offset")
            # We don't strip extended header here because bytes_to_performative expects full frame
            # But bytes_to_performative expects header + body.
            # So we reconstruct the full frame.

        full_frame = header_data + payload
        performative = bytes_to_performative(full_frame)
        return channel, performative

    async def write(self, data: bytes) -> None:
        if self.writer is None:
            raise ConnectionClosedError("Not connected")
        self.writer.write(data)
        await self.writer.drain()

    async def close(self) -> None:
        if self.writer:
            self.writer.close()
            with contextlib.suppress(Exception):
                await self.writer.wait_closed()
        self.reader = None
        self.writer = None


class Subscription:
    def __init__(self, link: "ReceiverLink"):
        self.link = link

    @property
    def is_active(self) -> bool:
        return not self.link.closed and self.link.session.connection.is_connected

    async def cancel(self) -> None:
        await self.link.detach()


class AmqpConnection:
    def __init__(
        self,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ):
        self.transport = AmqpTransport(host=host, port=port, ssl_context=ssl_context)
        self.username = username
        self.password = password
        self._connected = False
        self.sessions: dict[int, Session] = {}
        self._incoming_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self.max_frame_size = MAX_FRAME_SIZE_BYTES
        self.remote_max_frame_size = MIN_MAX_FRAME_SIZE
        self._waiters: list[tuple[Callable[[int, Any], bool], asyncio.Future]] = []

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        backoff = 1.0
        max_backoff = 60.0

        while not self._stop_event.is_set():
            try:
                logger.info("Connecting to %s:%d...", self.transport.host, self.transport.port)
                await self.transport.connect()

                if self.username is not None:
                    await self._connect_sasl()
                else:
                    await self._connect_plain()

                self._connected = True
                logger.info("Connected to AMQP broker.")

                # Start read loop
                self._incoming_task = asyncio.create_task(self._read_loop())

                # Send Open frame
                open_frame = OpenFrame(
                    container_id="repid-client",
                    hostname=self.transport.host,
                    max_frame_size=self.max_frame_size,
                )
                await self.send_performative(
                    0,
                    open_frame,
                    wait_for_response=lambda _, p: isinstance(p, OpenFrame),
                )

                return

            except Exception as exc:  # noqa: BLE001
                logger.error("Connection failed: %s. Retrying in %.1fs...", exc, backoff)
                await self.transport.close()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_sasl(self) -> None:
        await self.transport.write(SASL_HEADER)
        response = await self.transport.read_exactly(8)
        if response != SASL_HEADER:
            raise AmqpError(f"Unexpected SASL header response: {response!r}")

        await self._sasl_handshake()

        # SASL Done. Now send AMQP header.
        await self.transport.write(AMQP_HEADER)
        response = await self.transport.read_exactly(8)
        if response != AMQP_HEADER:
            raise AmqpError(f"Unexpected protocol header after SASL: {response!r}")

    async def _connect_plain(self) -> None:
        await self.transport.write(AMQP_HEADER)
        response = await self.transport.read_exactly(8)
        if response == SASL_HEADER:
            raise AmqpError("Server requires SASL, but no credentials provided.")
        if response != AMQP_HEADER:
            raise AmqpError(f"Unexpected protocol header: {response!r}")

    async def _sasl_handshake(self) -> None:
        # Wait for SASL-MECHANISMS
        _, performative = await self.transport.read_frame()
        if not isinstance(performative, SASLMechanism):
            raise AmqpError(f"Expected SASL-MECHANISMS, got {performative}")

        mechanisms = performative.sasl_server_mechanisms

        # Choose mechanism (PLAIN, ANONYMOUS, or EXTERNAL)
        chosen_mech = "ANONYMOUS"
        initial_response = None

        mech_names = [m.decode() if isinstance(m, bytes) else m for m in mechanisms]

        if "EXTERNAL" in mech_names and self.transport.ssl_context:
            chosen_mech = "EXTERNAL"
            initial_response = b""
        elif "PLAIN" in mech_names and self.username and self.password:
            chosen_mech = "PLAIN"
            # SASL PLAIN: authorization-id \0 authentication-id \0 passwd
            initial_response = f"\0{self.username}\0{self.password}".encode()
        elif "ANONYMOUS" not in mech_names:
            raise AmqpError(f"No supported SASL mechanism found. Server offered: {mechanisms}")

        # Send SASL-INIT
        sasl_init = SASLInit(
            mechanism=chosen_mech,
            initial_response=initial_response,
            hostname=self.transport.host,
        )
        await self.send_performative(0, sasl_init, frame_type=FRAME_TYPE_SASL)

        # Wait for SASL-OUTCOME
        while True:
            _, performative = await self.transport.read_frame()

            if isinstance(performative, SASLChallenge):
                raise AmqpError("SASL Challenge received but not supported.")
            if isinstance(performative, SASLOutcome):
                if performative.code != 0:  # OK
                    raise AmqpError(f"SASL Authentication failed with code {performative.code}")
                break
            raise AmqpError(f"Unexpected SASL frame: {performative}")

    async def _read_loop(self) -> None:
        while self._connected:
            try:
                channel, performative = await self.transport.read_frame()
                await self.handle_performative(channel, performative)

            except ConnectionClosedError:
                logger.info("Connection closed.")
                self._connected = False
                break
            except Exception:
                logger.exception("Error in read loop")
                self._connected = False
                break

    async def publish(
        self,
        topic: str,
        payload: bytes,
        headers: dict[str, Any] | None = None,
    ) -> None:
        if not self.sessions:
            # Create default session if none exists
            session = self.create_session()
            await session.begin()
            await session._ready.wait()
        else:
            session = next(iter(self.sessions.values()))

        link_name = f"sender-{topic}"
        if link_name in session.links:
            link = cast(SenderLink, session.links[link_name])
        else:
            link = await session.create_sender_link(topic, link_name)

        await link.send(payload, headers)

    async def subscribe(
        self,
        topic: str,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, "ReceiverLink"], Any],
    ) -> "Subscription":
        if not self.sessions:
            # Create default session if none exists
            session = self.create_session()
            await session.begin()
            await session._ready.wait()
        else:
            session = next(iter(self.sessions.values()))

        link_name = f"receiver-{topic}-{uuid.uuid4()}"
        link = await session.create_receiver_link(topic, link_name, callback)

        return Subscription(link)

    async def send_performative(
        self,
        channel: int,
        performative: Any,
        payload: bytes | None = None,
        frame_type: int = FRAME_TYPE_AMQP,  # noqa: ARG002
        wait_for_response: Callable[[int, Any], bool] | None = None,
        timeout: float = 10.0,
    ) -> None:
        if self.transport.writer is None:
            raise AmqpError("Not connected")

        if isinstance(performative, TransferFrame) and payload is not None:
            performative.payload = payload

        full_frame = performative_to_bytes(performative, channel)
        await self.transport.write(full_frame)

        if wait_for_response:
            future = asyncio.get_running_loop().create_future()
            self._waiters.append((wait_for_response, future))
            try:
                await asyncio.wait_for(future, timeout)
                return
            finally:
                if (wait_for_response, future) in self._waiters:
                    self._waiters.remove((wait_for_response, future))

    async def handle_performative(
        self,
        channel: int,
        performative: Any,
        payload: bytes | None = None,
    ) -> None:
        logger.debug("Received performative: %s", performative)

        # Check waiters
        for predicate, future in self._waiters[:]:
            if predicate(channel, performative):
                if not future.done():
                    future.set_result(performative)
                self._waiters.remove((predicate, future))

        if isinstance(performative, OpenFrame):
            # Handle Open response
            if performative.max_frame_size:
                self.remote_max_frame_size = performative.max_frame_size
        elif isinstance(performative, CloseFrame):
            await self.close()
        # Dispatch to session
        elif channel in self.sessions:
            await self.sessions[channel].handle_performative(performative, payload)

    async def close(self) -> None:
        if not self._connected:
            return

        with contextlib.suppress(Exception):
            await self.send_performative(0, CloseFrame())

        self._connected = False
        self._stop_event.set()
        if self._incoming_task:
            self._incoming_task.cancel()
        await self.transport.close()

    def create_session(self) -> "Session":
        # Find free channel
        channel = 0
        while channel in self.sessions:
            channel += 1
        session = Session(self, channel)
        self.sessions[channel] = session
        return session


class Session:
    def __init__(self, connection: AmqpConnection, channel: int):
        self.connection = connection
        self.channel = channel
        self.links: dict[str, Link] = {}  # Map by name
        self.links_by_handle: dict[int, Link] = {}
        self.links_by_remote_handle: dict[int, Link] = {}
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

    async def handle_performative(  # noqa: C901, PLR0912
        self,
        performative: Any,
        payload: bytes | None,  # noqa: ARG002
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
                    await link.handle_transfer(performative)
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


class Link(abc.ABC):
    def __init__(
        self,
        session: Session,
        name: str,
        address: str,
        role: Literal["Sender", "Receiver"],
    ):
        self.session = session
        self.name = name
        self.address = address
        self.role = role == "Receiver"
        self.handle = len(self.session.links_by_handle)
        self.session.links_by_handle[self.handle] = self
        self.session.links[name] = self
        self.remote_handle: int | None = None
        self.closed = False

    @abc.abstractmethod
    async def attach(self) -> None:
        raise NotImplementedError()

    async def detach(self) -> None:
        if self.closed:
            return
        detach = DetachFrame(handle=self.handle, closed=True)
        await self.session.connection.send_performative(self.session.channel, detach)
        self.closed = True


class SenderLink(Link):
    def __init__(self, session: Session, name: str, address: str):
        super().__init__(session, name, address, role="Sender")
        self.delivery_count = 0

    async def attach(self) -> None:
        target = Target(address=self.address)
        source = Source(address="client-source")

        attach = AttachFrame(
            name=self.name,
            handle=self.handle,
            role=self.role,
            source=source,
            target=target,
            initial_delivery_count=0,
        )
        await self.session.connection.send_performative(self.session.channel, attach)

    async def send(
        self,
        message_payload: bytes,
        headers: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        # Wrap payload in Message and encode
        msg = Message(
            data=message_payload,
            application_properties=headers,
            **kwargs,
        )

        max_frame_size = min(
            self.session.connection.max_frame_size,
            self.session.connection.remote_max_frame_size,
        )

        frames = message_to_transfer_frames(
            message=msg,
            handle=self.handle,
            delivery_id=self.delivery_count,
            delivery_tag=str(self.delivery_count).encode(),
            settled=True,
            max_frame_size=max_frame_size,
        )

        for frame in frames:
            await self.session.connection.send_performative(self.session.channel, frame)

        self.delivery_count += 1


class ReceiverLink(Link):
    def __init__(
        self,
        session: Session,
        name: str,
        address: str,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, "ReceiverLink"], Any],
    ):
        super().__init__(session, name, address, role="Receiver")
        self.callback = callback
        self._incoming_transfers: list[TransferFrame] = []

    async def attach(self) -> None:
        source = Source(address=self.address)
        target = Target(address="client-target")

        attach = AttachFrame(
            name=self.name,
            handle=self.handle,
            role=self.role,
            source=source,
            target=target,
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

    async def handle_transfer(self, transfer: TransferFrame) -> None:
        self._incoming_transfers.append(transfer)
        if not transfer.more:
            msg = transfer_frames_to_message(self._incoming_transfers)
            self._incoming_transfers.clear()

            # Extract body/value
            body = msg.body

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
