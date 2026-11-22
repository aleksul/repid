import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import grpc.aio
import pytest

from repid import Concurrency
from repid.connections.pubsub import message_broker
from repid.connections.pubsub.helpers import ChannelOverride
from repid.connections.pubsub.protocol import (
    GoogleDefaultCredentials,
    InsecureCredentials,
    ResilienceConfig,
    ResilienceState,
    StaticTokenCredentials,
)
from repid.connections.pubsub.protocol.client import PubsubProtocolClient
from repid.connections.pubsub.protocol.proto import StreamingPullRequest, StreamingPullResponse
from tests.intake import callback_intake


def test_init_defaults() -> None:
    server = message_broker.PubsubServer(default_project="my-project")

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


async def test_connect_is_idempotent() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)

    with patch.object(
        message_broker,
        "create_channel",
        new_callable=AsyncMock,
        return_value=MagicMock(spec=grpc.aio.Channel),
    ):
        await server.connect()
        await server.connect()


async def test_disconnect_closes_subscribers_and_channel() -> None:
    server = message_broker.PubsubServer(default_project="p")
    mock_channel = MagicMock(spec=grpc.aio.Channel)
    server._channel = mock_channel
    batcher = MagicMock(stop=AsyncMock())
    server._control_batcher = batcher

    mock_sub = AsyncMock()
    server._active_subscribers.append(mock_sub)

    await server.disconnect()

    mock_sub.close.assert_called_once()
    batcher.stop.assert_awaited_once()
    assert server._control_batcher is None
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

    message = MagicMock(payload=b"payload")

    await server.publish(channel="topic1", message=message)

    mock_unary.assert_awaited_once()
    request = mock_unary.call_args[0][0]
    assert request.topic == "projects/p/topics/topic1"
    assert request.messages[0].data == b"payload"


async def test_custom_credentials_authenticate_connection_and_publish() -> None:
    credentials = StaticTokenCredentials("custom-token")
    server = message_broker.PubsubServer(
        default_project="p",
        credentials_provider=credentials,
        use_google_auth=False,
    )
    publish = AsyncMock()
    channel = MagicMock(spec=grpc.aio.Channel)
    channel.unary_unary.return_value = publish

    with (
        patch.object(
            message_broker,
            "create_channel",
            new_callable=AsyncMock,
            return_value=channel,
        ) as create_channel,
        patch.object(
            credentials,
            "ensure_valid",
            new_callable=AsyncMock,
            wraps=credentials.ensure_valid,
        ) as ensure_valid,
    ):
        async with server.connection():
            await server.publish(channel="events", message=MagicMock(payload=b"event"))

    assert create_channel.call_args.kwargs["credentials_provider"] is credentials
    assert ensure_valid.await_count == 2
    publish.assert_awaited_once()
    request = publish.call_args.args[0]
    assert request.topic == "projects/p/topics/events"
    assert request.messages[0].data == b"event"
    channel.close.assert_awaited_once()


async def test_subscribers_share_the_circuit_breaker() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)
    server.resilience_config.failure_threshold = 2
    server.resilience_config.base_delay = 0
    channel = MagicMock(spec=grpc.aio.Channel)
    error = grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, None, None, "offline")

    async def pull(
        requests: AsyncGenerator[StreamingPullRequest, None],
    ) -> AsyncGenerator[StreamingPullResponse, None]:
        async with aclosing(requests):
            await anext(requests)
            yield StreamingPullResponse()
            raise error

    def open_stream(requests: AsyncGenerator[StreamingPullRequest, None]) -> MagicMock:
        call = MagicMock(spec=grpc.aio.StreamStreamCall)
        call.__aiter__.side_effect = lambda: pull(requests)
        return call

    channel.stream_stream.return_value = open_stream
    with patch.object(
        message_broker,
        "create_channel",
        new_callable=AsyncMock,
        return_value=channel,
    ):
        async with server.connection():
            for expected_attempts in (2, 3):
                subscriber = await server.subscribe(
                    intake=callback_intake({"events": AsyncMock()}),
                )
                try:
                    with pytest.raises(grpc.aio.AioRpcError, match="offline"):
                        await asyncio.wait_for(asyncio.shield(subscriber.task), 1)
                    assert server.resilience_state.attempt_count == expected_attempts
                    assert channel.stream_stream.call_count == expected_attempts
                finally:
                    await subscriber.close()


async def test_publish_raises_error_when_disconnected() -> None:
    server = message_broker.PubsubServer(default_project="p")
    message = MagicMock()

    with pytest.raises(ConnectionError, match="not connected"):
        await server.publish(channel="t", message=message)


async def test_subscribe_creates_subscriber_via_factory() -> None:
    server = message_broker.PubsubServer(default_project="p")
    server._channel = MagicMock(spec=grpc.aio.Channel)

    with patch.object(
        message_broker.PubsubSubscriber,
        "create",
        new_callable=AsyncMock,
    ) as mock_create:
        ret = await server.subscribe(
            intake=callback_intake({"chan1": MagicMock()}),
        )

        assert ret is mock_create.return_value
        mock_create.assert_called_once()
        assert server._active_subscribers == [mock_create.return_value]

        assert mock_create.call_args.kwargs["channel_configs"][0].channel == "chan1"
        assert (
            mock_create.call_args.kwargs["channel_configs"][0].subscription_path
            == "projects/p/subscriptions/chan1"
        )


async def test_subscribe_treats_missing_channel_limit_as_unlimited() -> None:
    server = message_broker.PubsubServer(default_project="p")
    server._channel = MagicMock(spec=grpc.aio.Channel)

    with patch.object(
        message_broker.PubsubSubscriber,
        "create",
        new_callable=AsyncMock,
    ) as mock_create:
        await server.subscribe(
            intake=callback_intake({"chan1": MagicMock()}, Concurrency(), {}),
        )

    assert mock_create.call_args.kwargs["channel_configs"][0].concurrency == Concurrency()


async def test_subscribe_raises_error_when_disconnected() -> None:
    server = message_broker.PubsubServer(default_project="p")

    with pytest.raises(Exception, match=r"PubSub server is not connected\."):
        await server.subscribe(
            intake=callback_intake({"c": MagicMock()}),
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

    msg = MagicMock(headers={"h1": "v1"}, content_type="application/json", reply_to=None)

    assert server._build_attributes(msg, None) == {"h1": "v1", "content_type": "application/json"}

    msg2 = MagicMock(headers=None, content_type=None, reply_to=None)
    assert server._build_attributes(msg2, None) is None

    msg3 = MagicMock(headers={"h1": "v1"}, content_type="application/json", reply_to=None)
    attrs3 = server._build_attributes(msg3, {"attributes": {"extra": "val"}})
    assert attrs3 == {"h1": "v1", "content_type": "application/json", "extra": "val"}


async def test_connect_failure_after_channel_creation_cleans_up_and_can_retry() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)

    mock_channel = MagicMock(spec=grpc.aio.Channel)
    with (
        patch.object(
            message_broker,
            "create_channel",
            new_callable=AsyncMock,
            return_value=mock_channel,
        ),
        patch.object(
            message_broker,
            "PubsubControlBatcher",
        ) as mock_batcher_cls,
    ):
        failing_batcher = MagicMock(
            start=AsyncMock(side_effect=ConnectionError("offline")),
            stop=AsyncMock(),
        )
        ok_batcher = MagicMock(start=AsyncMock(), stop=AsyncMock())
        mock_batcher_cls.side_effect = [failing_batcher, ok_batcher]

        with pytest.raises(ConnectionError, match="offline"):
            await server.connect()

        assert server.is_connected is False
        assert server._channel is None
        assert server._protocol_client is None
        assert server._control_batcher is None
        failing_batcher.stop.assert_awaited_once()
        mock_channel.close.assert_called_once()

        await server.connect()

        assert server.is_connected is True
        assert server._control_batcher is ok_batcher
        assert mock_batcher_cls.call_count == 2


async def test_connect_failure_preserves_error_when_cleanup_fails() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)

    mock_channel = MagicMock(spec=grpc.aio.Channel)
    with (
        patch.object(
            message_broker,
            "create_channel",
            new_callable=AsyncMock,
            return_value=mock_channel,
        ),
        patch.object(
            message_broker,
            "PubsubControlBatcher",
        ) as mock_batcher_cls,
    ):
        failing_batcher = MagicMock(
            start=AsyncMock(side_effect=ConnectionError("startup failed")),
            stop=AsyncMock(side_effect=RuntimeError("cleanup failed")),
        )
        mock_batcher_cls.return_value = failing_batcher

        with pytest.raises(ConnectionError, match="startup failed"):
            await server.connect()

        assert server.is_connected is False
        assert server._channel is None
        mock_channel.close.assert_called_once()


async def test_connect_failure_survives_channel_close_failure() -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)

    mock_channel = MagicMock(
        spec=grpc.aio.Channel,
        close=AsyncMock(side_effect=RuntimeError("close failed")),
    )
    with (
        patch.object(
            message_broker,
            "create_channel",
            new_callable=AsyncMock,
            return_value=mock_channel,
        ),
        patch.object(
            message_broker,
            "PubsubControlBatcher",
        ) as mock_batcher_cls,
    ):
        failing_batcher = MagicMock(
            start=AsyncMock(side_effect=ConnectionError("startup failed")),
            stop=AsyncMock(),
        )
        mock_batcher_cls.return_value = failing_batcher

        with pytest.raises(ConnectionError, match="startup failed"):
            await server.connect()

        assert server.is_connected is False
        assert server._channel is None
        mock_channel.close.assert_awaited_once()


def test_init_passes_circuit_breaker_and_control_timeout_settings() -> None:
    server = message_broker.PubsubServer(
        default_project="p",
        circuit_breaker_threshold=4,
        circuit_breaker_cooldown=12.5,
        control_rpc_timeout=3.0,
    )

    assert server.resilience_state.config.failure_threshold == 4
    assert server.resilience_state.config.open_duration == 12.5
    assert server._control_rpc_timeout == 3.0


@pytest.mark.parametrize(
    ("threshold", "cooldown", "timeout"),
    [
        pytest.param(0, 30.0, 10.0, id="threshold_zero"),
        pytest.param(10, -1.0, 10.0, id="negative_cooldown"),
        pytest.param(10, float("inf"), 10.0, id="infinite_cooldown"),
        pytest.param(10, 30.0, 0.0, id="zero_timeout"),
        pytest.param(10, 30.0, float("nan"), id="nan_timeout"),
    ],
)
def test_init_rejects_invalid_resilience_settings(
    threshold: int,
    cooldown: float,
    timeout: float,
) -> None:
    with pytest.raises(ValueError, match=r"circuit_breaker|control_rpc_timeout"):
        message_broker.PubsubServer(
            default_project="p",
            circuit_breaker_threshold=threshold,
            circuit_breaker_cooldown=cooldown,
            control_rpc_timeout=timeout,
        )


@pytest.mark.parametrize(
    ("concurrency", "channel_concurrency", "expected"),
    [
        (Concurrency(messages=4), {}, [(4, 0), (4, 0)]),
        (Concurrency(), {}, [(0, 0), (0, 0)]),
        (
            Concurrency(messages=1000),
            {"a": Concurrency(messages=3, payload_bytes=10 * 1024)},
            [(3, 10 * 1024), (1000, 0)],
        ),
    ],
)
async def test_subscribe_sets_streaming_pull_limits(
    concurrency: Concurrency,
    channel_concurrency: dict[str, Concurrency],
    expected: list[tuple[int, int]],
) -> None:
    server = message_broker.PubsubServer(default_project="p", use_google_auth=False)
    server._channel = MagicMock(spec=grpc.aio.Channel)
    initial_requests: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    async def pull(
        requests: AsyncGenerator[StreamingPullRequest, None],
    ) -> AsyncGenerator[StreamingPullResponse, None]:
        async with aclosing(requests):
            initial_requests.put_nowait(await anext(requests))
            async for _ in requests:
                yield StreamingPullResponse()

    def open_stream(requests: AsyncGenerator[StreamingPullRequest, None]) -> MagicMock:
        call = MagicMock(spec=grpc.aio.StreamStreamCall)
        call.__aiter__.side_effect = lambda: pull(requests)
        return call

    server._channel.stream_stream.return_value = open_stream
    subscriber = await server.subscribe(
        intake=callback_intake(
            {"a": AsyncMock(), "b": AsyncMock()},
            concurrency,
            channel_concurrency,
        ),
    )
    try:
        requests = [await asyncio.wait_for(initial_requests.get(), 1) for _ in expected]
        assert [
            (request.max_outstanding_messages, request.max_outstanding_bytes)
            for request in sorted(requests, key=lambda request: request.subscription)
        ] == expected
    finally:
        await subscriber.close()
