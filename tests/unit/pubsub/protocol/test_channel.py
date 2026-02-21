from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import grpc

from repid.connections.pubsub.protocol.channel import (
    DEFAULT_PUBSUB_TARGET,
    create_channel,
    parse_dsn,
)
from repid.connections.pubsub.protocol.credentials import CredentialsProvider


async def test_parse_dsn() -> None:
    assert parse_dsn(None) == DEFAULT_PUBSUB_TARGET
    assert parse_dsn("pubsub.googleapis.com") == "pubsub.googleapis.com:443"
    assert parse_dsn("http://localhost:8681") == "localhost:8681"
    assert parse_dsn("http://localhost:8681/v1") == "localhost:8681"

    # Scheme logic
    assert parse_dsn("http://localhost") == "localhost:80"
    assert parse_dsn("other://localhost") == "localhost:443"
    assert parse_dsn("https://localhost") == "localhost:443"

    # Missing host -> Default
    assert parse_dsn("myscheme:") == DEFAULT_PUBSUB_TARGET


async def test_create_channel_insecure() -> None:
    creds_mock = MagicMock(spec=CredentialsProvider)
    creds_mock.is_secure = False

    with patch("grpc.aio.insecure_channel") as mock_insecure:
        mock_insecure.return_value = AsyncMock(spec=grpc.aio.Channel)

        channel = await create_channel(
            dsn="http://emulator-host:8085",
            credentials_provider=creds_mock,
        )

        mock_insecure.assert_called_once_with(
            "emulator-host:8085",
            options=None,
        )
        assert isinstance(channel, grpc.aio.Channel)


async def test_create_channel_secure() -> None:
    creds_mock = MagicMock(spec=CredentialsProvider)
    creds_mock.is_secure = True
    creds_mock.ensure_valid = AsyncMock()
    creds_mock.get_token.return_value = "fake-token"

    # Mock all the grpc calls
    mock_channel = AsyncMock(spec=grpc.aio.Channel)

    with (
        patch("grpc.aio.secure_channel", return_value=mock_channel) as mock_secure,
        patch("grpc.ssl_channel_credentials") as mock_ssl,
        patch("grpc.metadata_call_credentials") as mock_meta,
        patch("grpc.composite_channel_credentials") as mock_composite,
    ):
        # We need to capture the AuthMetadataPlugin instance to test it
        auth_plugin_instance = None

        def side_effect_metadata_call_credentials(plugin: Any) -> MagicMock:
            nonlocal auth_plugin_instance
            auth_plugin_instance = plugin
            return MagicMock()

        mock_meta.side_effect = side_effect_metadata_call_credentials

        channel = await create_channel(
            dsn="pubsub.googleapis.com",
            credentials_provider=creds_mock,
        )
        assert channel is mock_channel

        # Verify call chain
        creds_mock.ensure_valid.assert_called_once()
        mock_ssl.assert_called_once()
        mock_meta.assert_called_once()
        mock_composite.assert_called_once()
        mock_secure.assert_called_once()

        # Now test the plugin callback logic
        assert auth_plugin_instance is not None
        callback_mock = MagicMock()
        auth_plugin_instance(None, callback_mock)

        creds_mock.get_token.assert_called()
        callback_mock.assert_called_once_with(
            (("authorization", "Bearer fake-token"),),
            None,
        )


async def test_create_channel_secure_no_token() -> None:
    creds_mock = MagicMock(spec=CredentialsProvider)
    creds_mock.is_secure = True
    creds_mock.ensure_valid = AsyncMock()
    creds_mock.get_token.return_value = None

    with (
        patch("grpc.aio.secure_channel"),
        patch("grpc.ssl_channel_credentials"),
        patch("grpc.metadata_call_credentials") as mock_meta,
        patch("grpc.composite_channel_credentials"),
    ):
        auth_plugin_instance = None

        def side_effect(plugin: Any) -> MagicMock:
            nonlocal auth_plugin_instance
            auth_plugin_instance = plugin
            return MagicMock()

        mock_meta.side_effect = side_effect

        await create_channel(
            dsn="pubsub.googleapis.com",
            credentials_provider=creds_mock,
        )

        assert auth_plugin_instance is not None
        callback_mock = MagicMock()
        auth_plugin_instance(None, callback_mock)

        callback_mock.assert_called_once_with((), None)


async def test_create_channel_defaults() -> None:
    # It should default to GoogleDefaultCredentials when create() is called
    with (
        patch("repid.connections.pubsub.protocol.channel.GoogleDefaultCredentials") as mock_gdc,
        patch("grpc.aio.secure_channel"),
        patch("grpc.ssl_channel_credentials"),
        patch("grpc.metadata_call_credentials"),
        patch("grpc.composite_channel_credentials"),
    ):
        mock_provider = MagicMock()
        mock_provider.is_secure = True
        mock_provider.ensure_valid = AsyncMock()
        mock_gdc.return_value = mock_provider

        await create_channel(dsn=None, credentials_provider=None)

        mock_gdc.assert_called_once()
        mock_provider.ensure_valid.assert_called_once()
