import pytest

from repid import Connection, DummyBucketBroker, DummyMessageBroker, Repid


async def test_connection() -> None:
    app = Repid(
        Connection(
            DummyMessageBroker(),
            DummyBucketBroker(),
            DummyBucketBroker(True),
        )
    )

    assert app.connection.is_open is False

    async with app.magic():
        assert app.connection.is_open is True

    assert app.connection.is_open is True

    async with app.magic(auto_disconnect=True):
        assert app.connection.is_open is True

    assert app.connection.is_open is False


def test_no_magic() -> None:
    with pytest.raises(ValueError, match="Default connection isn't set."):
        Repid.get_magic_connection()


def test_update_config() -> None:
    Repid(Connection(DummyMessageBroker()), update_config=True)
