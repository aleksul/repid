"""
AMQP 1.0 State Machines for Connection, Session, and Link states.

This module implements explicit state management following the AMQP 1.0 specification.
Each state machine validates transitions and maintains history for debugging.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import ClassVar, Generic, TypeVar

logger = logging.getLogger(__name__)

# Type variable for state enums
S = TypeVar("S", bound=Enum)


# =============================================================================
# Connection States
# =============================================================================


class ConnectionState(Enum):
    """Connection states matching AMQP 1.0 specification."""

    # Initial states
    START = auto()
    HDR_SENT = auto()
    HDR_RCVD = auto()
    HDR_EXCH = auto()

    # Opening states
    OPEN_PIPE = auto()
    OPEN_SENT = auto()
    OPEN_RCVD = auto()
    OPENED = auto()

    # Closing states
    CLOSE_PIPE = auto()
    CLOSE_SENT = auto()
    CLOSE_RCVD = auto()
    DISCARDING = auto()
    END = auto()

    # Error state
    ERROR = auto()


# =============================================================================
# Session States
# =============================================================================


class SessionState(Enum):
    """Session states matching AMQP 1.0 specification."""

    UNMAPPED = auto()
    BEGIN_SENT = auto()
    BEGIN_RCVD = auto()
    MAPPED = auto()
    END_SENT = auto()
    END_RCVD = auto()
    DISCARDING = auto()
    ERROR = auto()


# =============================================================================
# Link States
# =============================================================================


class LinkState(Enum):
    """Link states matching AMQP 1.0 specification."""

    DETACHED = auto()
    ATTACH_SENT = auto()
    ATTACH_RCVD = auto()
    ATTACHED = auto()
    DETACH_SENT = auto()
    DETACH_RCVD = auto()
    ERROR = auto()


# =============================================================================
# State Transition Record
# =============================================================================


@dataclass(frozen=True, slots=True)
class StateTransition(Generic[S]):
    """Immutable record of a state transition."""

    from_state: S
    to_state: S
    trigger: str
    timestamp: float = field(default_factory=time.monotonic)

    def __repr__(self) -> str:
        return f"{self.from_state.name} -> {self.to_state.name} ({self.trigger})"


# =============================================================================
# Exceptions
# =============================================================================


class InvalidStateTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""

    def __init__(
        self,
        current_state: Enum,
        trigger: str,
        valid_triggers: list[str] | None = None,
    ):
        self.current_state = current_state
        self.trigger = trigger
        self.valid_triggers = valid_triggers or []
        msg = f"Invalid transition from {current_state.name} via '{trigger}'"
        if valid_triggers:
            msg += f". Valid triggers: {valid_triggers}"
        super().__init__(msg)


class StateError(Exception):
    """Raised when an operation is attempted in an invalid state."""

    def __init__(self, current_state: Enum, required_states: list[Enum]):
        self.current_state = current_state
        self.required_states = required_states
        required_names = [s.name for s in required_states]
        super().__init__(
            f"Operation not allowed in state {current_state.name}. "
            f"Required states: {required_names}",
        )


# =============================================================================
# Base State Machine
# =============================================================================


class StateMachine(Generic[S]):
    """
    Generic Finite State Machine with explicit transition validation.

    Pattern: State Machine
    - Explicit states prevent invalid operations
    - Transitions are validated and logged
    - State history enables debugging and monitoring

    Type Parameters:
        S: The state enum type
    """

    def __init__(
        self,
        initial_state: S,
        transitions: dict[tuple[S, str], S],
        terminal_states: frozenset[S],
    ):
        """
        Initialize the state machine.

        Args:
            initial_state: The starting state
            transitions: Map of (from_state, trigger) -> to_state
            terminal_states: Set of states that are terminal
        """
        self._state = initial_state
        self._transitions = transitions
        self._terminal_states = terminal_states
        self._lock = asyncio.Lock()
        self._history: list[StateTransition[S]] = []
        self._listeners: list[Callable[[StateTransition[S]], None]] = []

    @property
    def state(self) -> S:
        """Current state."""
        return self._state

    @property
    def history(self) -> list[StateTransition[S]]:
        """Copy of transition history."""
        return self._history.copy()

    def add_listener(self, callback: Callable[[StateTransition[S]], None]) -> None:
        """Add a listener for state transitions."""
        self._listeners.append(callback)

    def remove_listener(self, callback: Callable[[StateTransition[S]], None]) -> None:
        """Remove a state transition listener."""
        if callback in self._listeners:
            self._listeners.remove(callback)

    def can_transition(self, trigger: str) -> bool:
        """Check if a transition is valid without performing it."""
        return (self._state, trigger) in self._transitions

    def get_valid_triggers(self) -> list[str]:
        """Get list of valid triggers from current state."""
        return [trigger for (state, trigger) in self._transitions if state == self._state]

    async def transition(self, trigger: str) -> S:
        """
        Attempt a state transition.

        Thread-safe via asyncio.Lock.

        Args:
            trigger: The trigger causing the transition

        Returns:
            The new state after transition

        Raises:
            InvalidStateTransitionError: If the transition is not valid
        """
        async with self._lock:
            return self._transition_unlocked(trigger)

    def transition_sync(self, trigger: str) -> S:
        """
        Synchronous state transition (not thread-safe).

        Use only when you're sure no concurrent access is possible.
        """
        return self._transition_unlocked(trigger)

    def _transition_unlocked(self, trigger: str) -> S:
        """Perform transition without lock."""
        key = (self._state, trigger)
        if key not in self._transitions:
            raise InvalidStateTransitionError(
                self._state,
                trigger,
                self.get_valid_triggers(),
            )

        old_state = self._state
        new_state = self._transitions[key]
        self._state = new_state

        transition = StateTransition(old_state, new_state, trigger)
        self._history.append(transition)

        # Notify listeners synchronously
        for listener in self._listeners:
            try:
                listener(transition)
            except Exception:
                logger.exception("Error in state transition listener")

        logger.debug(
            "State transition: %s -> %s (trigger: %s)",
            old_state.name,
            new_state.name,
            trigger,
        )

        return new_state

    def is_terminal(self) -> bool:
        """Check if current state is terminal."""
        return self._state in self._terminal_states

    def reset(self, initial_state: S) -> None:
        """Reset to initial state (clears history)."""
        self._state = initial_state
        self._history.clear()


# =============================================================================
# Connection State Machine
# =============================================================================


class ConnectionStateMachine(StateMachine[ConnectionState]):
    """
    State machine for AMQP connection lifecycle.

    Implements the connection state diagram from AMQP 1.0 spec.
    """

    # Valid transitions: (from_state, trigger) -> to_state
    _TRANSITIONS: ClassVar[dict[tuple[ConnectionState, str], ConnectionState]] = {
        # From START
        (ConnectionState.START, "send_header"): ConnectionState.HDR_SENT,
        (ConnectionState.START, "recv_header"): ConnectionState.HDR_RCVD,
        # From HDR_SENT
        (ConnectionState.HDR_SENT, "recv_header"): ConnectionState.HDR_EXCH,
        (ConnectionState.HDR_SENT, "send_open"): ConnectionState.OPEN_PIPE,
        # From HDR_RCVD
        (ConnectionState.HDR_RCVD, "send_header"): ConnectionState.HDR_EXCH,
        # From HDR_EXCH
        (ConnectionState.HDR_EXCH, "send_open"): ConnectionState.OPEN_SENT,
        (ConnectionState.HDR_EXCH, "recv_open"): ConnectionState.OPEN_RCVD,
        # From OPEN_PIPE
        (ConnectionState.OPEN_PIPE, "recv_header"): ConnectionState.OPEN_SENT,
        # From OPEN_SENT
        (ConnectionState.OPEN_SENT, "recv_open"): ConnectionState.OPENED,
        # From OPEN_RCVD
        (ConnectionState.OPEN_RCVD, "send_open"): ConnectionState.OPENED,
        # From OPENED
        (ConnectionState.OPENED, "send_close"): ConnectionState.CLOSE_SENT,
        (ConnectionState.OPENED, "recv_close"): ConnectionState.CLOSE_RCVD,
        (ConnectionState.OPENED, "error"): ConnectionState.DISCARDING,
        # From CLOSE_SENT
        (ConnectionState.CLOSE_SENT, "recv_close"): ConnectionState.END,
        (ConnectionState.CLOSE_SENT, "error"): ConnectionState.END,
        # From CLOSE_RCVD
        (ConnectionState.CLOSE_RCVD, "send_close"): ConnectionState.END,
        # From DISCARDING
        (ConnectionState.DISCARDING, "recv_close"): ConnectionState.END,
        (ConnectionState.DISCARDING, "error"): ConnectionState.END,
        # Fatal error from any state
        **{
            (state, "fatal_error"): ConnectionState.ERROR
            for state in ConnectionState
            if state not in (ConnectionState.ERROR, ConnectionState.END)
        },
    }

    _TERMINAL_STATES: ClassVar[frozenset[ConnectionState]] = frozenset(
        {
            ConnectionState.END,
            ConnectionState.ERROR,
        },
    )

    def __init__(self) -> None:
        super().__init__(
            initial_state=ConnectionState.START,
            transitions=self._TRANSITIONS,
            terminal_states=self._TERMINAL_STATES,
        )

    def is_open(self) -> bool:
        """Check if connection is in OPENED state."""
        return self._state == ConnectionState.OPENED

    def is_usable(self) -> bool:
        """Check if connection can be used for operations."""
        return self._state in (
            ConnectionState.OPENED,
            ConnectionState.CLOSE_RCVD,  # Can still send close
        )


# =============================================================================
# Session State Machine
# =============================================================================


class SessionStateMachine(StateMachine[SessionState]):
    """
    State machine for AMQP session lifecycle.

    Implements the session state diagram from AMQP 1.0 spec.
    """

    _TRANSITIONS: ClassVar[dict[tuple[SessionState, str], SessionState]] = {
        # From UNMAPPED
        (SessionState.UNMAPPED, "send_begin"): SessionState.BEGIN_SENT,
        (SessionState.UNMAPPED, "recv_begin"): SessionState.BEGIN_RCVD,
        # From BEGIN_SENT
        (SessionState.BEGIN_SENT, "recv_begin"): SessionState.MAPPED,
        # From BEGIN_RCVD
        (SessionState.BEGIN_RCVD, "send_begin"): SessionState.MAPPED,
        # From MAPPED
        (SessionState.MAPPED, "send_end"): SessionState.END_SENT,
        (SessionState.MAPPED, "recv_end"): SessionState.END_RCVD,
        (SessionState.MAPPED, "error"): SessionState.DISCARDING,
        # From END_SENT
        (SessionState.END_SENT, "recv_end"): SessionState.UNMAPPED,
        # From END_RCVD
        (SessionState.END_RCVD, "send_end"): SessionState.UNMAPPED,
        # From DISCARDING
        (SessionState.DISCARDING, "recv_end"): SessionState.UNMAPPED,
        # Fatal error from any state
        **{
            (state, "fatal_error"): SessionState.ERROR
            for state in SessionState
            if state not in (SessionState.ERROR, SessionState.UNMAPPED)
        },
    }

    _TERMINAL_STATES: ClassVar[frozenset[SessionState]] = frozenset(
        {
            SessionState.UNMAPPED,
            SessionState.ERROR,
        },
    )

    def __init__(self) -> None:
        super().__init__(
            initial_state=SessionState.UNMAPPED,
            transitions=self._TRANSITIONS,
            terminal_states=self._TERMINAL_STATES,
        )

    def is_mapped(self) -> bool:
        """Check if session is in MAPPED state."""
        return self._state == SessionState.MAPPED

    def is_usable(self) -> bool:
        """Check if session can be used for operations."""
        return self._state in (
            SessionState.MAPPED,
            SessionState.END_RCVD,  # Can still send end
        )


# =============================================================================
# Link State Machine
# =============================================================================


class LinkStateMachine(StateMachine[LinkState]):
    """
    State machine for AMQP link lifecycle.

    Implements the link state diagram from AMQP 1.0 spec.
    """

    _TRANSITIONS: ClassVar[dict[tuple[LinkState, str], LinkState]] = {
        # From DETACHED
        (LinkState.DETACHED, "send_attach"): LinkState.ATTACH_SENT,
        (LinkState.DETACHED, "recv_attach"): LinkState.ATTACH_RCVD,
        # From ATTACH_SENT
        (LinkState.ATTACH_SENT, "recv_attach"): LinkState.ATTACHED,
        # From ATTACH_RCVD
        (LinkState.ATTACH_RCVD, "send_attach"): LinkState.ATTACHED,
        # From ATTACHED
        (LinkState.ATTACHED, "send_detach"): LinkState.DETACH_SENT,
        (LinkState.ATTACHED, "recv_detach"): LinkState.DETACH_RCVD,
        # From DETACH_SENT
        (LinkState.DETACH_SENT, "recv_detach"): LinkState.DETACHED,
        # From DETACH_RCVD
        (LinkState.DETACH_RCVD, "send_detach"): LinkState.DETACHED,
        # Fatal error from any state
        **{
            (state, "fatal_error"): LinkState.ERROR
            for state in LinkState
            if state not in (LinkState.ERROR, LinkState.DETACHED)
        },
    }

    _TERMINAL_STATES: ClassVar[frozenset[LinkState]] = frozenset(
        {
            LinkState.DETACHED,
            LinkState.ERROR,
        },
    )

    def __init__(self) -> None:
        super().__init__(
            initial_state=LinkState.DETACHED,
            transitions=self._TRANSITIONS,
            terminal_states=self._TERMINAL_STATES,
        )

    def is_attached(self) -> bool:
        """Check if link is in ATTACHED state."""
        return self._state == LinkState.ATTACHED

    def is_usable(self) -> bool:
        """Check if link can be used for operations."""
        return self._state in (
            LinkState.ATTACHED,
            LinkState.DETACH_RCVD,  # Can still send detach
        )
