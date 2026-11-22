from __future__ import annotations

from typing import Any

from repid.connections.amqp.protocol.states import (
    ConnectionState,
    ConnectionStateMachine,
    LinkStateMachine,
    StateError,
    StateTransition,
)


async def test_state_error_formats_required_states() -> None:
    error = StateError(ConnectionState.START, [ConnectionState.OPENED, ConnectionState.OPEN_SENT])
    error_msg = str(error)
    assert "START" in error_msg
    assert "OPENED" in error_msg


async def test_state_machine_listeners() -> None:
    sm = ConnectionStateMachine()
    transitions: list[StateTransition[Any]] = []

    def listener(t: StateTransition[Any]) -> None:
        transitions.append(t)

    sm.add_listener(listener)
    await sm.transition("send_header")
    assert len(transitions) == 1

    sm.remove_listener(listener)
    await sm.transition("recv_header")
    # Should still be 1 since listener was removed
    assert len(transitions) == 1


async def test_state_machine_history() -> None:
    sm = ConnectionStateMachine()
    assert len(sm.history) == 0

    await sm.transition("send_header")
    history = sm.history
    assert len(history) == 1
    assert repr(history) == "[START -> HDR_SENT (send_header)]"
    await sm.transition("recv_header")
    assert repr(sm.history) == (
        "[START -> HDR_SENT (send_header), HDR_SENT -> HDR_EXCH (recv_header)]"
    )
    assert repr(history) == "[START -> HDR_SENT (send_header)]"


async def test_state_machine_listener_exception() -> None:
    sm = ConnectionStateMachine()

    def bad_listener(_t: StateTransition[Any]) -> None:
        raise ValueError("Test error")

    sm.add_listener(bad_listener)
    # Should not raise despite listener error
    await sm.transition("send_header")


async def test_state_machine_remove_nonexistent_listener() -> None:
    sm = ConnectionStateMachine()

    def listener(_t: StateTransition[Any]) -> None:
        pass

    # Should not raise when removing non-existent listener
    sm.remove_listener(listener)


async def test_state_machine_can_transition() -> None:
    sm = ConnectionStateMachine()

    # Valid transition from START
    assert sm.can_transition("send_header")

    # Invalid transition from START
    assert not sm.can_transition("send_close")


async def test_link_state_machine_is_usable() -> None:
    lsm = LinkStateMachine()
    # In DETACHED state initially
    assert not lsm.is_usable()

    # Transition to ATTACHED
    await lsm.transition("send_attach")
    await lsm.transition("recv_attach")
    assert lsm.is_usable()
    assert lsm.is_attached()

    # Detach
    await lsm.transition("send_detach")
    assert not lsm.is_usable()
    assert not lsm.is_attached()
