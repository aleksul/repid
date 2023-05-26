import pytest

from repid import Connection, DummyBucketBroker, DummyMessageBroker, Repid


async def test_connection() -> None:
    app = Repid(
        Connection(
            DummyMessageBroker(),
            DummyBucketBroker(),
            DummyBucketBroker(use_result_bucket=True),
        ),
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


@pytest.mark.parametrize(
    "connection",
    [
        Connection(DummyMessageBroker()),
        Connection(DummyMessageBroker(), args_bucket_broker=DummyBucketBroker()),
        Connection(
            DummyMessageBroker(),
            results_bucket_broker=DummyBucketBroker(use_result_bucket=True),
        ),
        Connection(
            DummyMessageBroker(),
            DummyBucketBroker(),
            DummyBucketBroker(use_result_bucket=True),
        ),
    ],
)
def test_update_config(connection: Connection) -> None:
    Repid(connection, update_config=True)
