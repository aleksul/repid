from datetime import timedelta
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import grpc.aio
import pytest

from repid.connections.pubsub import message_broker
from repid.connections.pubsub.helpers import ChannelOverride
from repid.connections.pubsub.message_broker import PUBLISH_METHOD
from repid.connections.pubsub.protocol import GoogleDefaultCredentials, InsecureCredentials


def test_pubsub_server_init_defaults() -> None:
    server = message_broker.PubsubServer(default_project="my-project")

    assert server._default_project == "my-project"
    assert isinstance(server._credentials_provider, GoogleDefaultCredentials)
    assert server.host == "pubsub.googleapis.com:443"
    assert not server.is_connected
    assert server.protocol == "googlepubsub"


def test_pubsub_server_init_dsn_insecure() -> None:
    server = message_broker.PubsubServer(
        dsn="http://localhost:8085",
        default_project="my-project",
    )
    assert isinstance(server._credentials_provider, InsecureCredentials)
    assert server.host == "localhost:8085"


def test_pubsub_server_init_explicit_auth_flags() -> None:
    server = message_broker.PubsubServer(
        dsn="http://localhost:8085",
        default_project="p",
        use_google_auth=True,
    )
    assert isinstance(server._credentials_provider, GoogleDefaultCredentials)

    server2 = message_broker.PubsubServer(
        default_project="p",
        use_google_auth=False,
    )
    assert isinstance(server2._credentials_provider, InsecureCredentials)


def test_pubsub_server_init_custom_provider() -> None:
    provider = InsecureCredentials()
    server = message_broker.PubsubServer(
        default_project="p",
        credentials_provider=provider,
    )
    assert server._credentials_provider == provider


async def test_pubsub_server_connect_idempotent() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)

    with patch(
        "repid.connections.pubsub.message_broker.create_channel",
        new_callable=AsyncMock,
        return_value=MagicMock(spec=grpc.aio.Channel),
    ):
        await server.connect()
        await server.connect()


async def test_pubsub_server_publish() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)
    server._channel = MagicMock(spec=grpc.aio.Channel)
    mock_unary = AsyncMock(return_value=MagicMock(message_ids=["123"]))
    server._channel.unary_unary.return_value = mock_unary

    message = MagicMock()
    message.payload = b"payload"

    await server.publish(channel="topic1", message=message)

    server._channel.unary_unary.assert_called_with(
        PUBLISH_METHOD,
        request_serializer=ANY,
        response_deserializer=ANY,
    )
    mock_unary.assert_called_once()
    assert mock_unary.call_args[0][0].topic == "projects/p/topics/topic1"
    assert mock_unary.call_args[0][0].messages[0].data == b"payload"


async def test_pubsub_server_publish_not_connected() -> None:
    server = message_broker.PubsubServer(default_project="p")
    message = MagicMock()

    with pytest.raises(ConnectionError, match="not connected"):
        await server.publish(channel="t", message=message)


def test_pubsub_server_properties_basic() -> None:
    server = message_broker.PubsubServer(
        default_project="p",
        title="Title",
        summary="Summary",
    )
    assert server.title == "Title"
    assert server.summary == "Summary"
    assert server.capabilities["supports_acknowledgments"] is True


async def test_pubsub_server_disconnect() -> None:
    server = message_broker.PubsubServer(default_project="p")
    mock_channel = MagicMock(spec=grpc.aio.Channel)
    server._channel = mock_channel

    mock_sub = AsyncMock()
    server._active_subscribers.append(mock_sub)

    await server.disconnect()

    mock_sub.close.assert_called_once()
    mock_channel.close.assert_called_once()
    assert server._channel is None


async def test_pubsub_server_context_manager() -> None:
    server = message_broker.PubsubServer(default_project="p")

    with (
        patch.object(server, "connect", new_callable=AsyncMock) as mock_connect,
        patch.object(server, "disconnect", new_callable=AsyncMock) as mock_disconnect,
    ):
        async with server.connection() as s:
            assert s is server
            mock_connect.assert_called_once()

        mock_disconnect.assert_called_once()


async def test_pubsub_server_subscribe() -> None:
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
        },
    )

    assert server._resolve_subscription_path("my-chan") == "projects/def-proj/subscriptions/my-chan"
    assert (
        server._resolve_subscription_path("overridden") == "projects/new-proj/subscriptions/new-sub"
    )


def test_extract_timeout() -> None:
    server = message_broker.PubsubServer(default_project="p")

    assert server._extract_timeout(None) == 10
    assert server._extract_timeout({"timeout": 5}) == 5
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

    attrs2 = server._build_attributes(msg, {"attributes": {"extra": "val"}})
    assert attrs2 == {"h1": "v1", "content_type": "application/json", "extra": "val"}

    msg_empty = MagicMock()
    msg_empty.headers = None
    msg_empty.content_type = None
    msg_empty.reply_to = None
    assert server._build_attributes(msg_empty, None) is None


def test_pubsub_server_all_properties() -> None:
    server = message_broker.PubsubServer(default_project="p")

    assert server.pathname is None
    assert server.title is None
    assert server.summary is None
    assert server.description is None
    assert server.protocol_version == "v1"
    assert server.variables is None
    assert server.security is None
    assert server.tags is None
    assert server.external_docs is None
    assert server.bindings is None

    caps = server.capabilities
    assert caps["supports_acknowledgments"] is True
    assert caps["supports_persistence"] is True

    assert server.resilience_config is not None
    assert server.resilience_state is not None


def test_pubsub_server_custom_dsn_properties() -> None:
    server = message_broker.PubsubServer(dsn="http://localhost:8085", default_project="p")
    assert server.host == "localhost:8085"
    assert server.pathname is None


async def test_pubsub_server_all_properties_coverage() -> None:
    server = message_broker.PubsubServer(default_project="p")
    # Coverage for properties
    assert server.protocol_version == "v1"
    assert server.title is None
    assert server.summary is None
    assert server.description is None
    assert server.variables is None
    assert server.security is None
    assert server.tags is None
    assert server.external_docs is None
    assert server.bindings is None
    assert server.capabilities
    assert server.resilience_config
    assert server.resilience_state


def test_extract_timeout_variants() -> None:
    server = message_broker.PubsubServer(default_project="p")

    assert server._extract_timeout({"timeout": 10}) == 10
    assert server._extract_timeout({"timeout": 10.5}) == 10
    assert server._extract_timeout({"timeout": timedelta(seconds=20)}) == 20
    assert server._extract_timeout({}) == message_broker.PubsubServer.DEFAULT_PUBLISH_TIMEOUT
    assert server._extract_timeout(None) == message_broker.PubsubServer.DEFAULT_PUBLISH_TIMEOUT


def test_resolve_topic_path_logic() -> None:
    server = message_broker.PubsubServer(
        default_project="default_p",
        channel_overrides={
            "my_channel": ChannelOverride(topic="t1", project="p1"),
            "partial_channel": ChannelOverride(topic="t2"),
        },
    )

    path = server._resolve_topic_path("any", {"topic": "custom_t", "project": "custom_p"})
    assert path == "projects/custom_p/topics/custom_t"

    path = server._resolve_topic_path("my_channel", None)
    assert path == "projects/p1/topics/t1"

    path = server._resolve_topic_path("partial_channel", None)
    assert path == "projects/default_p/topics/t2"

    path = server._resolve_topic_path("fresh", None)
    assert path == "projects/default_p/topics/fresh"


def test_resolve_subscription_path_logic() -> None:
    server = message_broker.PubsubServer(
        default_project="default_p",
        channel_overrides={
            "sub_channel": ChannelOverride(subscription="s1", project="p1"),
            "partial_sub": ChannelOverride(subscription="s2"),
        },
    )

    path = server._resolve_subscription_path("sub_channel")
    assert path == "projects/p1/subscriptions/s1"

    path = server._resolve_subscription_path("partial_sub")
    assert path == "projects/default_p/subscriptions/s2"

    path = server._resolve_subscription_path("fresh")
    assert path == "projects/default_p/subscriptions/fresh"


def test_build_attributes_variants() -> None:
    server = message_broker.PubsubServer(default_project="p")
    msg = MagicMock()
    msg.headers = {"h1": "v1"}
    msg.content_type = "json"
    msg.reply_to = None

    attrs = server._build_attributes(msg, {"attributes": {"extra": 123}})
    assert attrs == {"h1": "v1", "content_type": "json", "extra": "123"}

    msg.headers = None
    msg.content_type = None
    attrs = server._build_attributes(msg, None)
    assert attrs is None


async def test_subscribe_not_connected() -> None:
    server = message_broker.PubsubServer(default_project="p")

    with pytest.raises(Exception, match=r"PubSub server is not connected."):
        await server.subscribe(channels_to_callbacks={"c": MagicMock()})
