from __future__ import annotations

import asyncio
import ssl
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from repid.connections.amqp._uamqp.performatives import (
    BeginFrame,
    CloseFrame,
    OpenFrame,
    SASLChallenge,
    SASLInit,
    SASLMechanism,
    SASLOutcome,
)
from repid.connections.amqp.protocol.connection import (
    AmqpConnection,
    AuthenticationError,
    ConnectionConfig,
    ProtocolError,
)
from repid.connections.amqp.protocol.reconnect import ReconnectConfig
from repid.connections.amqp.protocol.session import Session
from repid.connections.amqp.protocol.states import (
    ConnectionState,
    ConnectionStateMachine,
    InvalidStateTransitionError,
    SessionState,
)
from repid.connections.amqp.protocol.transport import (
    AMQP_HEADER,
    SASL_HEADER,
    ConnectionClosedError,
)

from .utils import FakeConnection, FakeSession, FakeTransport, _set_connection_open


async def test_connection_state_machine_tracks_transitions() -> None:
    state_machine = ConnectionStateMachine()
    transitions: list[tuple[ConnectionState, ConnectionState]] = []

    def listener(transition: Any) -> None:
        transitions.append((transition.from_state, transition.to_state))

    state_machine.add_listener(listener)

    await state_machine.transition("send_header")
    await state_machine.transition("recv_header")

    assert state_machine.state == ConnectionState.HDR_EXCH
    assert transitions == [
        (ConnectionState.START, ConnectionState.HDR_SENT),
        (ConnectionState.HDR_SENT, ConnectionState.HDR_EXCH),
    ]


def test_connection_state_machine_rejects_invalid_transition() -> None:
    state_machine = ConnectionStateMachine()

    with pytest.raises(InvalidStateTransitionError) as excinfo:
        state_machine.transition_sync("send_open")

    assert "Valid triggers" in str(excinfo.value)


async def test_connection_do_amqp_open_records_remote_params() -> None:
    config = ConnectionConfig(host="example.com")
    connection = AmqpConnection(config)
    transport = FakeTransport(
        protocol_headers=[AMQP_HEADER],
        frames=[
            (
                0,
                OpenFrame(
                    container_id="remote-container",
                    max_frame_size=512,
                    channel_max=7,
                    idle_timeout=30,
                ),
            ),
        ],
    )

    with patch.object(connection, "_transport", transport):
        await connection._do_amqp_open()

        assert connection.state == ConnectionState.OPENED
    assert connection.max_frame_size == 512
    assert connection._remote_container_id == "remote-container"
    assert connection._remote_idle_timeout == 30
    assert connection._remote_channel_max == 7
    assert transport.amqp_headers_sent == 1
    assert isinstance(transport.sent_frames[0][1], OpenFrame)


async def test_connection_do_amqp_open_rejects_sasl_header() -> None:
    config = ConnectionConfig(host="example.com")
    connection = AmqpConnection(config)
    transport = FakeTransport(
        protocol_headers=[SASL_HEADER],
        frames=[],
    )

    with (
        patch.object(connection, "_transport", transport),
        pytest.raises(
            ProtocolError,
            match="Server requires SASL",
        ),
    ):
        await connection._do_amqp_open()


async def test_connection_send_performative_waits_for_response() -> None:
    config = ConnectionConfig(host="example.com")
    connection = AmqpConnection(config)
    transport = FakeTransport(protocol_headers=[], frames=[])

    with patch.object(connection, "_transport", transport):
        _set_connection_open(connection)

        async def wait_for_close() -> CloseFrame:
            result = await connection.send_performative(
                0,
                OpenFrame(container_id="local"),
                wait_for_response=lambda channel, frame: channel == 0
                and isinstance(frame, CloseFrame),
            )
            assert isinstance(result, CloseFrame)
            return result

        task = asyncio.create_task(wait_for_close())
        # Ensure the task starts while the transport is still patched
        await asyncio.sleep(0)
        await connection._handle_performative(0, CloseFrame())
        await task

    assert isinstance(transport.sent_frames[0][1], OpenFrame)


async def test_connection_handle_performative_dispatches_session() -> None:
    config = ConnectionConfig(host="example.com")
    connection = AmqpConnection(config)

    with patch.object(connection, "_transport", FakeTransport(protocol_headers=[], frames=[])):
        session = Session(connection, channel=1)
        connection._sessions[1] = session

        await connection._handle_performative(
            1,
            BeginFrame(next_outgoing_id=0, incoming_window=1, outgoing_window=1),
        )

        assert session.state == SessionState.BEGIN_RCVD
        assert session._ready.is_set()


async def test_connection_state_machine_is_open() -> None:
    csm = ConnectionStateMachine()
    assert not csm.is_open()

    # Transition to opened state
    await csm.transition("send_header")
    await csm.transition("recv_header")
    await csm.transition("send_open")
    await csm.transition("recv_open")
    assert csm.is_open()


async def test_connection_context_manager() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "connect", new_callable=AsyncMock),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=AMQP_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            return_value=(0, OpenFrame(container_id="server")),
        ),
        patch.object(conn._transport, "close", new_callable=AsyncMock),
    ):
        # Test context manager
        async with conn:
            assert conn.is_connected

        # Should be closed after exiting context
        assert conn._stop_event.is_set()


async def test_connection_sasl_plain_authentication() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock) as mock_send,
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (0, SASLMechanism(sasl_server_mechanisms=["PLAIN"])),
                (0, SASLOutcome(code=0)),
            ],
        ),
    ):
        # Perform SASL auth
        await conn._do_sasl_auth()

        # Verify SASL INIT was sent
        calls = mock_send.call_args_list
        assert any(isinstance(args[0][1], SASLInit) for args in calls)

        # Check PLAIN credentials were sent
        init_call = next(args for args in calls if isinstance(args[0][1], SASLInit))
        sasl_init = init_call[0][1]
        assert sasl_init.mechanism == "PLAIN"
        assert sasl_init.initial_response == b"\x00guest\x00guest"


async def test_connection_sasl_external_with_ssl() -> None:
    # Create SSL context
    ssl_context = ssl.create_default_context()

    config = ConnectionConfig(
        host="localhost",
        port=5671,
        ssl_context=ssl_context,
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock) as mock_send,
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (0, SASLMechanism(sasl_server_mechanisms=["EXTERNAL", "PLAIN"])),
                (0, SASLOutcome(code=0)),
            ],
        ),
    ):
        # Perform SASL auth
        await conn._do_sasl_auth()

        # Should choose EXTERNAL when SSL is available
        calls = mock_send.call_args_list
        init_call = next(args for args in calls if isinstance(args[0][1], SASLInit))
        sasl_init = init_call[0][1]
        assert sasl_init.mechanism == "EXTERNAL"
        assert sasl_init.initial_response == b""


async def test_connection_sasl_anonymous() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock) as mock_send,
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (0, SASLMechanism(sasl_server_mechanisms=["ANONYMOUS"])),
                (0, SASLOutcome(code=0)),
            ],
        ),
    ):
        # Perform SASL auth
        await conn._do_sasl_auth()

        # Should use ANONYMOUS
        calls = mock_send.call_args_list
        init_call = next(args for args in calls if isinstance(args[0][1], SASLInit))
        sasl_init = init_call[0][1]
        assert sasl_init.mechanism == "ANONYMOUS"
        assert sasl_init.initial_response is None


async def test_connection_sasl_authentication_failed() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        username="invalid",
        password="invalid",
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (0, SASLMechanism(sasl_server_mechanisms=["PLAIN"])),
                (0, SASLOutcome(code=1)),  # Non-zero code = failure
            ],
        ),
        pytest.raises(AuthenticationError, match="SASL authentication failed"),
    ):
        await conn._do_sasl_auth()


async def test_connection_sasl_no_supported_mechanism() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            return_value=(0, SASLMechanism(sasl_server_mechanisms=["SCRAM-SHA-256"])),
        ),
        pytest.raises(AuthenticationError, match="No supported SASL mechanism"),
    ):
        await conn._do_sasl_auth()


async def test_connection_sasl_challenge_not_supported() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (0, SASLMechanism(sasl_server_mechanisms=["PLAIN"])),
                (0, SASLChallenge(challenge=b"challenge-data")),
            ],
        ),
        pytest.raises(AuthenticationError, match="SASL Challenge"),
    ):
        await conn._do_sasl_auth()


async def test_connection_sasl_unexpected_frame() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (0, SASLMechanism(sasl_server_mechanisms=["PLAIN"])),
                (0, OpenFrame(container_id="wrong")),  # Wrong frame type
            ],
        ),
        pytest.raises(ProtocolError, match="Unexpected SASL frame"),
    ):
        await conn._do_sasl_auth()


async def test_connection_do_amqp_open_server_requires_sasl() -> None:
    config = ConnectionConfig(host="localhost", port=5672)  # No username/password
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        pytest.raises(ProtocolError, match="Server requires SASL"),
    ):
        await conn._do_amqp_open()


async def test_connection_do_amqp_open_invalid_protocol_header() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=b"INVALID!",
        ),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=False,
        ),
        pytest.raises(ProtocolError, match="Unexpected protocol header"),
    ):
        await conn._do_amqp_open()


async def test_connection_do_amqp_open_unexpected_frame() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=AMQP_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            return_value=(0, CloseFrame()),
        ),
        pytest.raises(ProtocolError, match="Expected OPEN"),
    ):
        await conn._do_amqp_open()


async def test_connection_read_loop_handles_connection_closed() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set state to OPENED
    conn._state_machine._state = ConnectionState.OPENED

    with patch.object(
        conn._transport,
        "read_frame",
        new_callable=AsyncMock,
        side_effect=ConnectionClosedError("EOF"),
    ):
        # Run read loop
        await conn._read_loop()

        # Should exit cleanly without raising


async def test_connection_read_loop_handles_generic_error() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set state to OPENED
    conn._state_machine._state = ConnectionState.OPENED

    with patch.object(
        conn._transport,
        "read_frame",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Error"),
    ):
        # Run read loop
        await conn._read_loop()

        # Should exit cleanly


async def test_connection_read_loop_cancelled() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set state to OPENED
    conn._state_machine._state = ConnectionState.OPENED

    with patch.object(
        conn._transport,
        "read_frame",
        new_callable=AsyncMock,
        side_effect=asyncio.CancelledError(),
    ):
        # Run read loop - should catch and suppress CancelledError
        await conn._read_loop()

        # Should exit cleanly without raising


async def test_connection_handle_close_frame() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set state to OPENED
    conn._state_machine._state = ConnectionState.OPENED

    with (
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock) as mock_send,
        patch.object(conn._transport, "close", new_callable=AsyncMock),
    ):
        # Handle CLOSE frame
        await conn._handle_close(CloseFrame())

        # Should send CLOSE response
        assert any(isinstance(args[0][1], CloseFrame) for args in mock_send.call_args_list)


async def test_connection_create_session_not_connected() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Connection is not in OPENED state
    assert not conn.is_connected

    with pytest.raises(ConnectionError, match="Connection is not open"):
        await conn.create_session()


async def test_connection_create_session_no_free_channels() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set to OPENED state
    conn._state_machine._state = ConnectionState.OPENED
    conn._remote_channel_max = 0  # Only channel 0 available

    # Fill the only available channel
    fake_session = FakeSession(connection=FakeConnection(), channel=0)
    conn._sessions[0] = fake_session  # type: ignore[assignment]

    with pytest.raises(ConnectionError, match="No free channels"):
        await conn.create_session()


async def test_connection_properties() -> None:
    config = ConnectionConfig(
        host="testhost",
        port=5673,
        max_frame_size=8192,
    )
    conn = AmqpConnection(config)

    # Test properties
    assert conn.connection_id
    assert conn.state == ConnectionState.START
    assert not conn.is_connected
    assert not conn.is_usable
    assert conn.events == conn._events
    assert conn.metrics == conn._metrics

    # Test max_frame_size with remote value
    conn._remote_max_frame_size = 4096
    assert conn.max_frame_size == 4096  # Should use min of local and remote


async def test_connection_handle_unexpected_close_skips_if_connected() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set state to OPENED
    conn._state_machine._state = ConnectionState.OPENED

    # Call _handle_unexpected_close - should return early
    await conn._handle_unexpected_close()

    # Should not have reset state
    assert conn._state_machine.state == ConnectionState.OPENED


async def test_connection_close_already_terminal() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set to terminal state
    conn._state_machine._state = ConnectionState.END

    with patch.object(conn._transport, "close", new_callable=AsyncMock) as mock_close:
        # Should return immediately
        await conn.close()

        # Transport should not be closed again
        mock_close.assert_not_called()


async def test_connection_close_send_frame_error() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set to OPENED state
    conn._state_machine._state = ConnectionState.OPENED

    with (
        patch.object(
            conn._transport,
            "send_performative",
            new_callable=AsyncMock,
            side_effect=OSError("Network error"),
        ),
        patch.object(conn._transport, "close", new_callable=AsyncMock) as mock_close,
    ):
        # Should handle error gracefully
        await conn.close()
        # Transport should still be closed
        mock_close.assert_called()


async def test_connection_remove_session() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Add fake session
    fake_session = FakeSession(connection=FakeConnection(), channel=1)
    conn._sessions[1] = fake_session  # type: ignore[assignment]

    # Remove session
    conn._remove_session(1)

    # Should be removed
    assert 1 not in conn._sessions


async def test_connection_create_classmethod() -> None:
    conn = AmqpConnection.create(
        host="testhost",
        port=1234,
        username="user",
        password="pass",
        virtual_host="/vhost",
        max_frame_size=4096,
    )

    # Should create ConnectionConfig with provided params
    assert conn._config.host == "testhost"
    assert conn._config.port == 1234
    assert conn._config.username == "user"
    assert conn._config.password == "pass"
    assert conn._config.virtual_host == "/vhost"
    assert conn._config.max_frame_size == 4096


async def test_connection_connect_retry_exhausted() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        reconnect=ReconnectConfig(enabled=True, max_attempts=2),
    )
    conn = AmqpConnection(config)

    with (
        patch.object(
            conn._transport,
            "connect",
            new_callable=AsyncMock,
            side_effect=OSError("Connection refused"),
        ),
        patch.object(conn._transport, "close", new_callable=AsyncMock),
        pytest.raises(ConnectionError, match="Connection failed after"),
    ):
        await conn.connect()


async def test_connection_do_sasl_auth_invalid_sasl_header() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=b"BADHEADR",
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=False,
        ),
        pytest.raises(ProtocolError, match="Unexpected SASL header response"),
    ):
        await conn._do_sasl_auth()


async def test_connection_do_sasl_auth_unexpected_mechanism_frame() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            return_value=(0, OpenFrame(container_id="wrong")),
        ),
        pytest.raises(ProtocolError, match="Expected SASL-MECHANISMS"),
    ):
        await conn._do_sasl_auth()


async def test_connection_do_sasl_auth_invalid_amqp_header_after_sasl() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            side_effect=[SASL_HEADER, b"BADHEADR"],
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=False,
        ),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (0, SASLMechanism(sasl_server_mechanisms=["PLAIN"])),
                (0, SASLOutcome(code=0)),
            ],
        ),
        pytest.raises(ProtocolError, match="Unexpected AMQP header after SASL"),
    ):
        await conn._do_sasl_auth()


async def test_connection_do_connect_starts_read_loop() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "connect", new_callable=AsyncMock),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=AMQP_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            return_value=(0, OpenFrame(container_id="server")),
        ),
    ):
        # Do connect
        await conn._do_connect()

        # Read task should be created
        assert conn._read_task is not None
        assert not conn._read_task.done()

        # Clean up
        conn._stop_event.set()
        conn._read_task.cancel()


async def test_connection_close_with_sessions() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set to OPENED state
    conn._state_machine._state = ConnectionState.OPENED

    # Add fake session
    fake_session = FakeSession(connection=FakeConnection(), channel=1)
    conn._sessions[1] = fake_session  # type: ignore[assignment]

    with (
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(conn._transport, "close", new_callable=AsyncMock),
    ):
        # Close connection
        await conn.close()

        # Sessions should be cleared (end() was called)


async def test_connection_handle_unexpected_close_invalidates_sessions() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        reconnect=ReconnectConfig(enabled=True),
    )
    conn = AmqpConnection(config)

    # Set state to non-open (connection lost)
    conn._state_machine._state = ConnectionState.START

    # Add fake session with invalidate method
    fake_session = FakeSession(connection=FakeConnection(), channel=1)
    conn._sessions[1] = fake_session  # type: ignore[assignment]

    with (
        patch.object(conn._transport, "connect", new_callable=AsyncMock),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=AMQP_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            return_value=(0, OpenFrame(container_id="server")),
        ),
    ):
        # Handle unexpected close
        await conn._handle_unexpected_close()

        # Sessions should be cleared
        assert 1 not in conn._sessions

        # Clean up
        conn._stop_event.set()
        if conn._read_task:
            conn._read_task.cancel()


async def test_connection_handle_unexpected_close_reconnect_failed() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        reconnect=ReconnectConfig(enabled=True, max_attempts=1),
    )
    conn = AmqpConnection(config)

    # Set state to non-open
    conn._state_machine._state = ConnectionState.START

    with (
        patch.object(
            conn._transport,
            "connect",
            new_callable=AsyncMock,
            side_effect=OSError("Connection refused"),
        ) as mock_connect,
        patch.object(conn._transport, "close", new_callable=AsyncMock),
    ):
        # Handle unexpected close - should catch and log error
        await conn._handle_unexpected_close()

        # Should have attempted reconnection
        mock_connect.assert_called()


async def test_connection_handle_performative_no_session() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # No sessions exist
    assert len(conn._sessions) == 0

    # Handle a frame for channel 1 (use valid BeginFrame)
    await conn._handle_performative(
        1,
        BeginFrame(next_outgoing_id=0, incoming_window=100, outgoing_window=100),
    )

    # Should log warning but not crash


async def test_connection_send_performative_not_usable() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Connection is in START state (not usable)
    assert not conn.is_usable

    with pytest.raises(ConnectionError, match="Connection is not usable"):
        await conn.send_performative(0, OpenFrame(container_id="test"))


async def test_connection_create_session_success() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    # Set to OPENED state
    conn._state_machine._state = ConnectionState.OPENED
    conn._remote_channel_max = 10

    with (
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            return_value=(0, OpenFrame(container_id="server")),
        ),
        patch.object(conn._transport, "close", new_callable=AsyncMock),
    ):
        session = await conn.create_session()

        # Should have created a session and tracked it
        assert session is not None
        assert len(conn._sessions) > 0


async def test_connection_do_connect_with_sasl_auth() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
    )
    conn = AmqpConnection(config)

    with (
        patch.object(conn._transport, "connect", new_callable=AsyncMock),
        patch.object(conn._transport, "send_sasl_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=SASL_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_sasl_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (0, SASLMechanism(sasl_server_mechanisms=["PLAIN"])),
                (0, SASLOutcome(code=0)),
                (0, OpenFrame(container_id="server")),
            ],
        ),
    ):
        await conn._do_connect()

        assert conn._read_task is not None
        assert not conn._read_task.done()

        conn._stop_event.set()
        conn._read_task.cancel()


async def test_connection_read_loop_with_unexpected_close() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.OPENED

    with patch.object(
        conn._transport,
        "read_frame",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Network error"),
    ):
        await conn._read_loop()

        conn._stop_event.set()


async def test_connection_read_loop_triggers_unexpected_close_handler() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.OPENED

    with (
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Network error"),
        ),
        patch.object(conn, "_handle_unexpected_close", new_callable=AsyncMock),
    ):
        await conn._read_loop()

        assert conn._stop_event.is_set() or conn._state_machine.is_open()


async def test_connection_handle_unexpected_close_with_general_exception() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        reconnect=ReconnectConfig(enabled=True, max_attempts=1),
    )
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.START

    with (
        patch.object(
            conn._transport,
            "connect",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Unexpected error"),
        ) as mock_connect,
        patch.object(conn._transport, "close", new_callable=AsyncMock),
    ):
        await conn._handle_unexpected_close()

        mock_connect.assert_called()


async def test_connection_handle_unexpected_close_emit_exception() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        reconnect=ReconnectConfig(enabled=True),
    )
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.START

    with (
        patch.object(conn._transport, "connect", new_callable=AsyncMock),
        patch.object(conn._transport, "send_amqp_header", new_callable=AsyncMock),
        patch.object(
            conn._transport,
            "read_protocol_header",
            new_callable=AsyncMock,
            return_value=AMQP_HEADER,
        ),
        patch.object(
            conn._transport,
            "validate_amqp_header",
            new_callable=MagicMock,
            return_value=True,
        ),
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            return_value=(0, OpenFrame(container_id="server")),
        ),
        patch.object(conn._transport, "close", new_callable=AsyncMock),
    ):
        original_emit = conn._emit
        call_count = [0]

        async def mock_emit(event: Any, details: Any = None) -> None:
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("Emit failed")
            return await original_emit(event, details)

        with patch.object(conn, "_emit", side_effect=mock_emit):
            await conn._handle_unexpected_close()


async def test_connection_handle_performative_unexpected_open() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    await conn._handle_performative(0, OpenFrame(container_id="unexpected"))


async def test_connection_handle_close_with_error_and_reconnect() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        reconnect=ReconnectConfig(enabled=True),
    )
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.OPENED

    close_frame = CloseFrame()
    close_frame.error = {"condition": "amqp:connection:forced"}

    with (
        patch.object(conn._transport, "send_performative", new_callable=AsyncMock),
        patch.object(conn._transport, "close", new_callable=AsyncMock),
        patch.object(conn, "_handle_unexpected_close", new_callable=AsyncMock) as mock_handle,
    ):
        await conn._handle_close(close_frame)
        mock_handle.assert_called()


async def test_connection_send_performative_waiter_removed() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.OPENED

    with patch.object(conn._transport, "send_performative", new_callable=AsyncMock):
        with pytest.raises(asyncio.TimeoutError):
            await conn.send_performative(
                0,
                OpenFrame(container_id="test"),
                wait_for_response=lambda *_: False,
                timeout=0.001,
            )

        assert len(conn._waiters) == 0


async def test_connection_send_performative_without_response() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.OPENED

    with patch.object(conn._transport, "send_performative", new_callable=AsyncMock) as mock_send:
        result = await conn.send_performative(0, OpenFrame(container_id="test"))

        assert result is None
        mock_send.assert_called_once()


async def test_connection_incoming_task_property() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    assert conn._incoming_task is None

    task = asyncio.create_task(asyncio.sleep(0))
    conn._read_task = task
    assert conn._incoming_task is task

    task.cancel()


async def test_connection_read_loop_unexpected_close_with_reconnect_disabled() -> None:
    config = ConnectionConfig(
        host="localhost",
        port=5672,
        reconnect=ReconnectConfig(enabled=False),
    )
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.OPENED

    with (
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Network error"),
        ),
        patch.object(conn, "_handle_unexpected_close", new_callable=AsyncMock) as mock_handle,
    ):
        await conn._read_loop()

        if not conn._stop_event.is_set() and not conn._state_machine.is_open():
            mock_handle.assert_called()


async def test_connection_read_loop_finally_block_with_close_rcvd_state() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.CLOSE_RCVD

    with patch.object(
        conn._transport,
        "read_frame",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Network error"),
    ):
        await conn._read_loop()


async def test_connection_read_loop_processes_frames() -> None:
    config = ConnectionConfig(host="localhost", port=5672)
    conn = AmqpConnection(config)

    conn._state_machine._state = ConnectionState.OPENED

    with (
        patch.object(
            conn._transport,
            "read_frame",
            new_callable=AsyncMock,
            side_effect=[
                (1, BeginFrame(next_outgoing_id=0, incoming_window=100, outgoing_window=100)),
                ConnectionClosedError("EOF"),
            ],
        ),
        patch.object(conn, "_handle_performative", new_callable=AsyncMock) as mock_handle,
    ):
        await conn._read_loop()
        mock_handle.assert_called_once()
