from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast  # Still needed for function annotations
from unittest.mock import patch

import pytest

from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp.protocol import ManagedSession
from repid.connections.amqp.protocol.connection import (
    AmqpConnection,
)
from repid.connections.amqp.protocol.events import (
    ConnectionEvent,
    EventData,
    EventEmitter,
)
from repid.connections.amqp.protocol.links import LinkError, ReceiverLink
from repid.connections.amqp.protocol.managed import (
    ReceiverPool,
    SenderPool,
)
from repid.connections.amqp.protocol.session import Session

from .utils import (
    FakeManagedConnection,
    FakeReceiverLink,
    FakeSenderLink,
    FakeSessionForPools,
)


async def test_sender_pool_wait_for_connection_timeout() -> None:
    connection = FakeManagedConnection(
        is_connected=False,
        session_factory=lambda: FakeSessionForPools(
            connection=None,
            sender_links=[FakeSenderLink()],
            receiver_links=[FakeReceiverLink()],
        ),
    )
    managed = ManagedSession(cast(AmqpConnection, connection))
    pool = managed.sender_pool

    with pytest.raises(ConnectionError, match="Connection not available"):
        await pool._wait_for_connection(timeout=0.0)


async def test_sender_pool_stale_link_removal() -> None:
    class StaleLink:
        is_usable = False

    session = FakeSessionForPools(
        connection=None,
        sender_links=[FakeSenderLink(), FakeSenderLink()],
        receiver_links=[],
    )

    connection = FakeManagedConnection(is_connected=True, session_factory=lambda: session)
    managed = ManagedSession(cast(AmqpConnection, connection))
    pool = managed.sender_pool

    # Add a stale link manually
    pool._links["/queues/test"] = cast(Any, StaleLink())

    # Send should create a new link instead of using stale one
    await pool.send("/queues/test", b"data")
    # Should have created new link
    assert pool._links["/queues/test"].is_usable


async def test_sender_pool_max_retries_exceeded() -> None:
    session = FakeSessionForPools(
        connection=None,
        sender_links=[FakeSenderLink(should_fail=True), FakeSenderLink(should_fail=True)],
        receiver_links=[],
    )

    class SlowConnection:
        is_connected = True
        events = EventEmitter()

        async def create_session(self) -> Session:
            return cast(Session, session)

    managed = ManagedSession(cast(AmqpConnection, SlowConnection()))
    pool = managed.sender_pool

    # Override to return immediately
    async def no_wait(timeout: float = 5.0) -> None:  # noqa: ARG001
        return None

    with (
        patch.object(pool, "_wait_for_connection", no_wait),
        pytest.raises(
            LinkError,
            match="send failed",
        ),
    ):
        await pool.send("/queues/test", b"data", max_retries=2)


async def test_sender_pool_wait_for_connection_timeout_error() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    # Connection that stays disconnected
    connection = FakeManagedConnection(is_connected=False, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    sender_pool = SenderPool(managed)

    # Should timeout
    with pytest.raises(ConnectionError, match="Connection not available"):
        await sender_pool._wait_for_connection(timeout=0.2)


async def test_sender_pool_removes_stale_link() -> None:
    @dataclass(slots=True)
    class StaleLink:
        _handle: int = 1
        _remote_handle: int | None = None
        name: str = "sender-test"
        is_usable: bool = False

        async def detach(self) -> None:
            pass

    sender_link = FakeSenderLink()

    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[sender_link],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    sender_pool = SenderPool(managed)

    # Add a stale link
    sender_pool._links["test-address"] = StaleLink()  # type: ignore[assignment]

    # Get should remove stale link and create new one
    link = await sender_pool.get("test-address")
    assert link is not None


async def test_sender_pool_send_retries_on_link_error() -> None:
    @dataclass(slots=True)
    class AlwaysFailingLink:
        is_usable: bool = True
        sent: list[bytes] = field(default_factory=list)
        detached: bool = False

        async def send(self, _payload: bytes, **_kwargs: Any) -> None:
            # Always fail to trigger retry logic
            raise LinkError("send always fails for testing")

        async def detach(self) -> None:
            self.detached = True

    def link_factory() -> FakeSenderLink:
        return cast(FakeSenderLink, AlwaysFailingLink())

    def session_factory() -> FakeSessionForPools:
        # Return a session that creates failing links via factory
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
            _sender_link_factory=link_factory,
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    sender_pool = SenderPool(managed)

    # Send should retry max_retries times and fail
    with pytest.raises(LinkError, match="send always fails"):
        await sender_pool.send("test-address", b"test-payload", max_retries=2)

    # The important part: verify that retry logic was executed (covered line 248)


async def test_sender_pool_get_with_stale_link() -> None:
    @dataclass(slots=True)
    class StaleLink:
        _handle: int = 1
        _remote_handle: int | None = None
        name: str = "sender-test"
        is_usable: bool = False

        async def detach(self) -> None:
            pass

    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    sender_pool = SenderPool(managed)

    # Add a stale link manually
    stale = StaleLink()
    sender_pool._links["test-address"] = cast(Any, stale)

    # Verify stale link is there
    assert "test-address" in sender_pool._links
    assert sender_pool._links["test-address"] is stale

    # Get should remove stale link and create new one
    link = await sender_pool.get("test-address")

    # Verify we got a different link
    assert link is not None
    assert link is not stale
    # The new link should be in the pool
    assert "test-address" in sender_pool._links
    assert sender_pool._links["test-address"] is link


async def test_sender_pool_get_reuses_usable_link() -> None:
    @dataclass(slots=True)
    class UsableLink:
        _handle: int = 1
        _remote_handle: int | None = None
        name: str = "sender-test"
        is_usable: bool = True

        async def detach(self) -> None:
            pass

    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    sender_pool = SenderPool(managed)

    # Add a usable link manually
    usable = UsableLink()
    sender_pool._links["test-address"] = cast(Any, usable)

    # Get should return the same usable link
    link = await sender_pool.get("test-address")
    assert link is usable


async def test_receiver_pool_reconnect_and_close() -> None:
    session = FakeSessionForPools(
        connection=None,
        sender_links=[FakeSenderLink()],
        receiver_links=[FakeReceiverLink(handle=3), FakeReceiverLink(handle=4)],
    )

    connection = FakeManagedConnection(is_connected=True, session_factory=lambda: session)
    managed = ManagedSession(cast(AmqpConnection, connection))
    pool = managed.receiver_pool

    await pool.subscribe("/queues/one", lambda *_args: None, "receiver-1")
    await pool.subscribe("/queues/two", lambda *_args: None, "receiver-2")

    await pool._on_reconnected()
    pool._stop_event.set()
    await pool._on_reconnected()

    await pool.close()


async def test_receiver_pool_on_reconnected_error_handling() -> None:
    class BadLink:
        async def detach(self) -> None:
            raise RuntimeError("Detach failed")

    receiver_link = FakeReceiverLink(handle=1)

    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[receiver_link],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    pool = managed.receiver_pool

    # Add a bad link
    pool._links["/queues/test"] = cast(Any, BadLink())

    # Trigger reconnection - should handle detach error
    await pool._on_reconnected(EventData(ConnectionEvent.RECONNECTED))

    # Links should be cleared despite error
    assert len(pool._links) == 0


async def test_receiver_pool_close_with_link_error() -> None:
    class BadLink:
        async def detach(self) -> None:
            raise RuntimeError("Detach error")

    receiver_link = FakeReceiverLink(handle=1)

    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[receiver_link],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    pool = managed.receiver_pool

    # Add bad link and a callback
    pool._links["/queues/test"] = cast(Any, BadLink())
    pool._callbacks["/queues/test"] = lambda *_: None

    # Should handle error without raising
    await pool.close()

    # Should clear both links and callbacks
    assert len(pool._links) == 0
    assert len(pool._callbacks) == 0


async def test_receiver_pool_on_reconnected_re_subscribe() -> None:
    async def callback(
        _body: bytes,
        _headers: dict[str, Any] | None,
        _delivery_id: int,
        _tag: bytes,
        _link: ReceiverLink,
        _properties: Properties | None,
    ) -> None:
        pass

    receiver_link = FakeReceiverLink(handle=1)

    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[receiver_link],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    receiver_pool = ReceiverPool(managed)

    # Subscribe to an address
    receiver_pool._callbacks["test-queue"] = callback
    receiver_pool._link_names["test-queue"] = "test-receiver"

    # Call _on_reconnected to trigger re-subscription
    await receiver_pool._on_reconnected()

    # Link should be created
    assert "test-queue" in receiver_pool._links


async def test_receiver_pool_invalidate() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))
    receiver_pool = ReceiverPool(managed)

    # Add a fake link
    receiver_pool._links["test"] = cast(Any, object())

    # Invalidate should clear
    receiver_pool.invalidate()

    assert len(receiver_pool._links) == 0
