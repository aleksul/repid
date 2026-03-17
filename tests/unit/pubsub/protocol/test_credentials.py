from unittest.mock import MagicMock, patch

from repid.connections.pubsub.protocol import credentials


def test_google_default_credentials_init() -> None:
    creds_mock = MagicMock()
    gdc = credentials.GoogleDefaultCredentials(credentials=creds_mock)
    assert gdc.is_secure
    assert gdc._credentials == creds_mock
    assert gdc._initialized


def test_google_default_credentials_scopes() -> None:
    gdc = credentials.GoogleDefaultCredentials(scopes=["scope1"])
    assert gdc._scopes == ["scope1"]


@patch("google.auth.default")
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


@patch("google.auth.default")
async def test_google_default_credentials_refresh(mock_default: MagicMock) -> None:
    mock_creds = MagicMock()
    mock_creds.expired = True
    mock_creds.valid = False
    mock_default.return_value = (mock_creds, "project")

    gdc = credentials.GoogleDefaultCredentials()
    await gdc.ensure_valid()  # Initializes

    assert gdc._credentials == mock_creds
    mock_creds.refresh.assert_called_once()


@patch("google.auth.default")
async def test_google_default_credentials_ensure_valid_no_refresh(mock_default: MagicMock) -> None:
    mock_creds = MagicMock()
    mock_creds.expired = False
    mock_creds.valid = True
    mock_default.return_value = (mock_creds, "project")

    gdc = credentials.GoogleDefaultCredentials()
    await gdc.ensure_valid()

    mock_creds.refresh.assert_not_called()


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
    # Force state where initialized is True but credentials is None
    gdc._initialized = True
    gdc._credentials = None
    assert gdc.get_token() is None


def test_get_token_with_valid_creds() -> None:
    creds = MagicMock()
    creds.token = "valid_token"
    gdc = credentials.GoogleDefaultCredentials(credentials=creds)
    assert gdc.get_token() == "valid_token"
