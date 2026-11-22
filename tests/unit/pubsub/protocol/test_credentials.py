from unittest.mock import MagicMock, patch

import google.auth
import grpc.aio

from repid.connections.pubsub.protocol import credentials
from repid.connections.pubsub.protocol.channel import create_channel


@patch.object(google.auth, "default")
async def test_google_default_credentials_ensure_initialized(mock_default: MagicMock) -> None:
    mock_creds = MagicMock()
    mock_default.return_value = (mock_creds, "project")

    gdc = credentials.GoogleDefaultCredentials()
    assert not gdc._initialized

    await gdc.ensure_valid()

    assert gdc._initialized
    assert gdc._credentials == mock_creds
    mock_default.assert_called_once()
    assert mock_default.call_args[1]["scopes"] == credentials.GoogleDefaultCredentials.SCOPES

    await gdc.ensure_valid()  # Should not call default again
    mock_default.assert_called_once()  # Still only called once


@patch.object(google.auth, "default")
async def test_google_default_credentials_refresh(mock_default: MagicMock) -> None:
    mock_creds = MagicMock(expired=True, valid=False)
    mock_default.return_value = (mock_creds, "project")

    gdc = credentials.GoogleDefaultCredentials()
    await gdc.ensure_valid()  # Initializes

    assert gdc._credentials == mock_creds
    mock_creds.refresh.assert_called_once()


@patch.object(google.auth, "default")
async def test_google_default_credentials_ensure_valid_no_refresh(mock_default: MagicMock) -> None:
    mock_creds = MagicMock(expired=False, valid=True)
    mock_default.return_value = (mock_creds, "project")

    gdc = credentials.GoogleDefaultCredentials()
    await gdc.ensure_valid()

    mock_creds.refresh.assert_not_called()


async def test_custom_scopes_select_credentials_for_authentication() -> None:
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    google_credentials = MagicMock(expired=False, valid=True, token="scoped-token")
    provider = credentials.GoogleDefaultCredentials(scopes=scopes)

    channel = MagicMock(spec=grpc.aio.Channel)
    with (
        patch.object(
            google.auth,
            "default",
            return_value=(google_credentials, "project"),
        ) as default,
        patch.object(grpc.aio, "secure_channel", return_value=channel) as secure_channel,
        patch.object(grpc.aio, "insecure_channel") as insecure_channel,
    ):
        assert await create_channel(credentials_provider=provider) is channel
        assert provider.get_token() == "scoped-token"
        await provider.ensure_valid()

    default.assert_called_once_with(scopes=scopes)
    secure_channel.assert_called_once()
    assert secure_channel.call_args.args[0] == "pubsub.googleapis.com:443"
    insecure_channel.assert_not_called()
    google_credentials.refresh.assert_not_called()


async def test_insecure_credentials() -> None:
    creds = credentials.InsecureCredentials()
    assert not creds.is_secure
    await creds.ensure_valid()
    assert creds.get_token() is None


async def test_static_token_credentials() -> None:
    creds = credentials.StaticTokenCredentials("my-token")
    assert creds.is_secure
    await creds.ensure_valid()
    assert creds.get_token() == "my-token"


async def test_google_credentials_get_token_none() -> None:
    gdc = credentials.GoogleDefaultCredentials()
    gdc._initialized = True
    gdc._credentials = None
    assert gdc.get_token() is None


def test_get_token_with_valid_creds() -> None:
    creds = MagicMock(token="valid_token")
    gdc = credentials.GoogleDefaultCredentials(credentials=creds)
    assert gdc.get_token() == "valid_token"
