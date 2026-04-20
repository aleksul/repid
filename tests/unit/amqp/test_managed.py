from __future__ import annotations

from typing import cast

import pytest
from typing_extensions import override

from repid.connections.amqp.protocol import ManagedSession
from repid.connections.amqp.protocol.connection import (
    AmqpConnection,
)
from repid.connections.amqp.protocol.events import (
    EventEmitter,
)
from repid.connections.amqp.protocol.managed import (
    ManagedSessionError,
    SenderPool,
)

from .utils import (
    FakeManagedConnection,
    FakeReceiverLink,
    FakeSenderLink,
    FakeSessionForPools,
)


async def test_managed_session_and_pools() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[FakeSenderLink(should_fail=True), FakeSenderLink()],
            receiver_links=[FakeReceiverLink(handle=9)],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))

    session = await managed.get_session()
    assert session.is_usable is True

    managed.invalidate()
    sender_pool = managed.sender_pool
    receiver_pool = managed.receiver_pool

    class TestSenderPool(SenderPool):
        @override
        async def _wait_for_connection(self, timeout: float = 5.0) -> None:
            return None

    managed._sender_pool = TestSenderPool(managed)
    sender_pool = managed.sender_pool

    await sender_pool.send("/queues/a", b"payload", max_retries=2)

    link = await receiver_pool.subscribe(
        "/queues/b",
        lambda *_args: None,
        "receiver-b",
    )
    assert link.handle == 9

    await receiver_pool.unsubscribe("/queues/b")
    await managed.close()


async def test_managed_session_connection_error() -> None:
    class DisconnectedConnection:
        is_connected = False
        events = EventEmitter()

    managed = ManagedSession(cast(AmqpConnection, DisconnectedConnection()))

    with pytest.raises(ConnectionError, match="Not connected"):
        await managed.get_session()


async def test_managed_session_create_session_error() -> None:
    class FailingConnection:
        is_connected = True
        events = EventEmitter()

        async def create_session(self) -> None:
            raise RuntimeError("Session creation failed")

    managed = ManagedSession(cast(AmqpConnection, FailingConnection()))

    with pytest.raises(ManagedSessionError, match="Failed to create session"):
        await managed.get_session()


async def test_managed_session_on_reconnected() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))

    # Call the handler directly
    await managed._on_reconnected()

    # Session should still be None until get_session is called
    assert managed._session is None


async def test_managed_session_invalidate_with_pools() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))

    # Access pools to create them
    _ = managed.sender_pool
    _ = managed.receiver_pool

    # Invalidate should clear pools
    managed.invalidate()

    assert managed._session is None


async def test_managed_session_on_disconnected() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))

    # Create a session
    session = await managed.get_session()
    assert session is not None

    # Trigger disconnected event
    await managed._on_disconnected()

    # Session should be invalidated
    assert managed._session is None


async def test_managed_session_close_with_pools() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))

    # Access pools to create them
    _ = managed.sender_pool
    _ = managed.receiver_pool

    # Close should clean up event handlers and pools
    await managed.close()


async def test_managed_session_close_with_sender_pool() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))

    # Access only sender pool
    _ = managed.sender_pool

    # Close should clean up
    await managed.close()
