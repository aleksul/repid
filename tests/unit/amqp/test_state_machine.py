from __future__ import annotations

from typing import Any

from repid.connections.amqp.protocol.states import (
    ConnectionState,
    ConnectionStateMachine,
    LinkStateMachine,
    StateError,
    StateTransition,
)


async def test_state_transition_repr() -> None:
    transition = StateTransition(
        from_state=ConnectionState.START,
        to_state=ConnectionState.OPENED,
        trigger="open",
    )
    rep = repr(transition)
    assert "START" in rep
    assert "OPENED" in rep
    assert "open" in rep


async def test_state_error() -> None:
    error = StateError(ConnectionState.START, [ConnectionState.OPENED, ConnectionState.OPEN_SENT])
    assert error.current_state == ConnectionState.START
    assert len(error.required_states) == 2
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
    assert len(transitions) == 1


async def test_state_machine_history() -> None:
    sm = ConnectionStateMachine()
    assert len(sm.history) == 0

    await sm.transition("send_header")
    history = sm.history
    assert len(history) == 1
    assert history[0].trigger == "send_header"


async def test_state_machine_listener_exception() -> None:
    sm = ConnectionStateMachine()

    def bad_listener(_t: StateTransition[Any]) -> None:
        raise ValueError("Test error")

    sm.add_listener(bad_listener)
    await sm.transition("send_header")


async def test_state_machine_remove_nonexistent_listener() -> None:
    sm = ConnectionStateMachine()

    def listener(_t: StateTransition[Any]) -> None:
        pass

    sm.remove_listener(listener)


async def test_state_machine_can_transition() -> None:
    sm = ConnectionStateMachine()

    assert sm.can_transition("send_header")

    assert not sm.can_transition("send_close")


async def test_link_state_machine_is_usable() -> None:
    lsm = LinkStateMachine()
    assert not lsm.is_usable()

    await lsm.transition("send_attach")
    await lsm.transition("recv_attach")
    assert lsm.is_usable()
    assert lsm.is_attached()

    await lsm.transition("send_detach")
    assert not lsm.is_usable()
    assert not lsm.is_attached()
