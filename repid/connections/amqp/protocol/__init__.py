"""
AMQP 1.0 Protocol Implementation.

This package provides a redesigned AMQP 1.0 client with:
- Explicit state machine management (Connection, Session, Link states)
- Clean separation of transport, connection, session, and link layers
- Event-driven monitoring and metrics
- Automatic reconnection with configurable backoff
- TCP keepalive for connection health (no application-level heartbeats)

Architecture:
-----------
The implementation follows a layered architecture:

1. Transport Layer (`transport.py`)
   - Raw TCP/SSL connection handling
   - TCP keepalive configuration
   - Frame reading/writing

2. State Machines (`states.py`)
   - ConnectionStateMachine: Manages connection lifecycle
   - SessionStateMachine: Manages session lifecycle
   - LinkStateMachine: Manages link lifecycle

3. Connection Layer (`connection.py`)
   - SASL authentication
   - Protocol negotiation
   - Automatic reconnection

4. Session Layer (`session.py`)
   - Session multiplexing over connection
   - Link management
   - Flow control

5. Link Layer (`links.py`)
   - SenderLink: For sending messages
   - ReceiverLink: For receiving messages

6. Events & Metrics (`events.py`)
   - Observer pattern for monitoring
   - Metrics collection for observability

Usage Example:
-------------
```python
from repid.connections.amqp.protocol import (
    AmqpConnection,
    ConnectionConfig,
)

# Create connection
config = ConnectionConfig(
    host="localhost",
    port=5672,
    username="guest",
    password="guest",
)

async with AmqpConnection(config) as conn:
    # Create a session
    session = await conn.create_session()

    # Create a sender
    sender = await session.create_sender("my-queue")
    await sender.send(b"Hello, World!")

    # Create a receiver
    async def on_message(body, headers, delivery_id, tag, link):
        print(f"Received: {body}")

    receiver = await session.create_receiver("my-queue", on_message)
```

For backward compatibility with existing code, the old `protocol.py` module
is still available but deprecated. New code should use this package.
"""

from .connection import AmqpConnection as AmqpConnection
from .connection import AmqpError as AmqpError
from .connection import AuthenticationError as AuthenticationError
from .connection import ConnectionConfig as ConnectionConfig
from .connection import ProtocolError as ProtocolError
from .events import ConnectionEvent as ConnectionEvent
from .events import ConnectionMetrics as ConnectionMetrics
from .events import EventData as EventData
from .events import EventEmitter as EventEmitter
from .events import MetricsCollector as MetricsCollector
from .links import Link as Link
from .links import LinkError as LinkError
from .links import ReceiverLink as ReceiverLink
from .links import SenderLink as SenderLink
from .links import Subscription as Subscription
from .managed import ManagedSession as ManagedSession
from .managed import ManagedSessionError as ManagedSessionError
from .managed import ReceiverPool as ReceiverPool
from .managed import SenderPool as SenderPool
from .reconnect import ReconnectConfig as ReconnectConfig
from .reconnect import ReconnectStrategy as ReconnectStrategy
from .session import Session as Session
from .session import SessionError as SessionError
from .states import ConnectionState as ConnectionState
from .states import ConnectionStateMachine as ConnectionStateMachine
from .states import InvalidStateTransitionError as InvalidStateTransitionError
from .states import LinkState as LinkState
from .states import LinkStateMachine as LinkStateMachine
from .states import SessionState as SessionState
from .states import SessionStateMachine as SessionStateMachine
from .states import StateError as StateError
from .states import StateTransition as StateTransition
from .transport import AMQP_HEADER as AMQP_HEADER
from .transport import SASL_HEADER as SASL_HEADER
from .transport import AmqpTransport as AmqpTransport
from .transport import ConnectionClosedError as ConnectionClosedError
from .transport import ConnectionTimeoutError as ConnectionTimeoutError
from .transport import FrameBuffer as FrameBuffer
from .transport import FrameError as FrameError
from .transport import TransportConfig as TransportConfig
from .transport import TransportError as TransportError
