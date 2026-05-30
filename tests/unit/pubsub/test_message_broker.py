from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import grpc.aio
import pytest

from repid.connections.pubsub import message_broker
from repid.connections.pubsub.helpers import ChannelOverride
from repid.connections.pubsub.protocol import (
    GoogleDefaultCredentials,
    InsecureCredentials,
    ResilienceConfig,
    ResilienceState,
)
from repid.connections.pubsub.protocol.client import PubsubProtocolClient


def test_init_defaults() -> None:
    server = message_broker.PubsubServer(default_project="my-project")

    assert server._default_project == "my-project"
    assert isinstance(server._credentials_provider, GoogleDefaultCredentials)
    assert server.host == "pubsub.googleapis.com:443"
    assert not server.is_connected
    assert server.protocol == "googlepubsub"


def test_init_with_dsn_uses_insecure_credentials() -> None:
    server = message_broker.PubsubServer(
        dsn="http://localhost:8085",
        default_project="my-project",
    )
    assert isinstance(server._credentials_provider, InsecureCredentials)
    assert server.host == "localhost:8085"


def test_init_explicit_auth_flags() -> None:
    dsn_auth = message_broker.PubsubServer(
        dsn="http://localhost:8085",
        default_project="p",
        use_google_auth=True,
    )
    assert isinstance(dsn_auth._credentials_provider, GoogleDefaultCredentials)

    no_dsn_no_auth = message_broker.PubsubServer(default_project="p", use_google_auth=False)
    assert isinstance(no_dsn_no_auth._credentials_provider, InsecureCredentials)


def test_init_custom_credentials_provider() -> None:
    provider = InsecureCredentials()
    server = message_broker.PubsubServer(
        default_project="p",
        credentials_provider=provider,
    )
    assert server._credentials_provider is provider


def test_all_properties() -> None:
    server = message_broker.PubsubServer(
        default_project="p",
        title="T",
        summary="S",
        description="D",
    )

    assert server.host == "pubsub.googleapis.com:443"
    assert server.pathname is None
    assert server.title == "T"
    assert server.summary == "S"
    assert server.description == "D"
    assert server.protocol_version == "v1"
    assert server.variables is None
    assert server.security is None
    assert server.tags is None
    assert server.external_docs is None
    assert server.bindings is None
    assert server.capabilities["supports_native_reply"] is False
    assert server.capabilities["supports_lightweight_pause"] is False
    assert server.resilience_config is not None
    assert server.resilience_state is not None

    with_dsn = message_broker.PubsubServer(dsn="http://localhost:8085", default_project="p")
    assert with_dsn.host == "localhost:8085"


async def test_connect_is_idempotent() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)

    with patch(
        "repid.connections.pubsub.message_broker.create_channel",
        new_callable=AsyncMock,
        return_value=MagicMock(spec=grpc.aio.Channel),
    ):
        await server.connect()
        await server.connect()


async def test_disconnect_closes_subscribers_and_channel() -> None:
    server = message_broker.PubsubServer(default_project="p")
    mock_channel = MagicMock(spec=grpc.aio.Channel)
    server._channel = mock_channel

    mock_sub = AsyncMock()
    server._active_subscribers.append(mock_sub)

    await server.disconnect()

    mock_sub.close.assert_called_once()
    mock_channel.close.assert_called_once()
    assert server._channel is None


async def test_connection_context_calls_connect_on_entry_and_disconnect_on_exit() -> None:
    server = message_broker.PubsubServer(default_project="p")

    with (
        patch.object(server, "connect", new_callable=AsyncMock) as mock_connect,
        patch.object(server, "disconnect", new_callable=AsyncMock) as mock_disconnect,
    ):
        async with server.connection() as s:
            assert s is server
            mock_connect.assert_called_once()

        mock_disconnect.assert_called_once()


async def test_publish_delegates_to_protocol_client() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)
    server._channel = MagicMock(spec=grpc.aio.Channel)
    mock_unary = AsyncMock()
    server._channel.unary_unary.return_value = mock_unary
    server._protocol_client = PubsubProtocolClient(
        channel=server._channel,
        credentials_provider=InsecureCredentials(),
        resilience_state=ResilienceState(ResilienceConfig(max_attempts=1)),
    )

    message = MagicMock()
    message.payload = b"payload"

    await server.publish(channel="topic1", message=message)

    mock_unary.assert_awaited_once()
    request = mock_unary.call_args[0][0]
    assert request.topic == "projects/p/topics/topic1"
    assert request.messages[0].data == b"payload"


async def test_publish_raises_error_when_disconnected() -> None:
    server = message_broker.PubsubServer(default_project="p")
    message = MagicMock()

    with pytest.raises(ConnectionError, match="not connected"):
        await server.publish(channel="t", message=message)


async def test_subscribe_creates_subscriber_via_factory() -> None:
    server = message_broker.PubsubServer(default_project="p")
    server._channel = MagicMock(spec=grpc.aio.Channel)

    with patch(
        "repid.connections.pubsub.message_broker.PubsubSubscriber.create",
        new_callable=AsyncMock,
    ) as mock_create:
        ret = await server.subscribe(channels_to_callbacks={"chan1": MagicMock()})

        assert ret is mock_create.return_value
        mock_create.assert_called_once()
        assert server._active_subscribers == [mock_create.return_value]

        assert mock_create.call_args.kwargs["channel_configs"][0].channel == "chan1"
        assert (
            mock_create.call_args.kwargs["channel_configs"][0].subscription_path
            == "projects/p/subscriptions/chan1"
        )


async def test_subscribe_raises_error_when_disconnected() -> None:
    server = message_broker.PubsubServer(default_project="p")

    with pytest.raises(Exception, match=r"PubSub server is not connected\."):
        await server.subscribe(channels_to_callbacks={"c": MagicMock()})


def test_resolve_topic_path() -> None:
    server = message_broker.PubsubServer(
        default_project="def-proj",
        channel_overrides={
            "overridden": ChannelOverride(topic="new-topic", project="new-proj"),
            "partial": ChannelOverride(topic="partial-topic"),
        },
    )

    assert server._resolve_topic_path("my-chan", None) == "projects/def-proj/topics/my-chan"

    params = {"topic": "param-topic", "project": "param-proj"}
    assert server._resolve_topic_path("my-chan", params) == "projects/param-proj/topics/param-topic"

    assert server._resolve_topic_path("overridden", None) == "projects/new-proj/topics/new-topic"
    assert server._resolve_topic_path("partial", None) == "projects/def-proj/topics/partial-topic"


def test_resolve_subscription_path() -> None:
    server = message_broker.PubsubServer(
        default_project="def-proj",
        channel_overrides={
            "overridden": ChannelOverride(subscription="new-sub", project="new-proj"),
            "partial_sub": ChannelOverride(subscription="s2"),
        },
    )

    assert server._resolve_subscription_path("my-chan") == "projects/def-proj/subscriptions/my-chan"
    assert (
        server._resolve_subscription_path("overridden") == "projects/new-proj/subscriptions/new-sub"
    )
    assert server._resolve_subscription_path("partial_sub") == "projects/def-proj/subscriptions/s2"


def test_extract_timeout() -> None:
    server = message_broker.PubsubServer(default_project="p")

    assert server._extract_timeout(None) == 10
    assert server._extract_timeout({}) == 10
    assert server._extract_timeout({"timeout": 5}) == 5
    assert server._extract_timeout({"timeout": 10.5}) == 10
    assert server._extract_timeout({"timeout": timedelta(seconds=20)}) == 20
    assert server._extract_timeout({"timeout": None}) == 10


def test_extract_ordering_key() -> None:
    server = message_broker.PubsubServer(default_project="p")

    assert server._extract_ordering_key(None) is None
    assert server._extract_ordering_key({}) is None
    assert server._extract_ordering_key({"ordering_key": "key"}) == "key"
    assert server._extract_ordering_key({"ordering_key": 123}) == "123"


def test_build_attributes() -> None:
    server = message_broker.PubsubServer(default_project="p")

    msg = MagicMock()
    msg.headers = {"h1": "v1"}
    msg.content_type = "application/json"
    msg.reply_to = None

    assert server._build_attributes(msg, None) == {"h1": "v1", "content_type": "application/json"}

    msg2 = MagicMock()
    msg2.headers = None
    msg2.content_type = None
    msg2.reply_to = None
    assert server._build_attributes(msg2, None) is None

    msg3 = MagicMock()
    msg3.headers = {"h1": "v1"}
    msg3.content_type = "application/json"
    msg3.reply_to = None
    attrs3 = server._build_attributes(msg3, {"attributes": {"extra": "val"}})
    assert attrs3 == {"h1": "v1", "content_type": "application/json", "extra": "val"}
