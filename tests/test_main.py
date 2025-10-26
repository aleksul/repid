import pytest

from repid import Connection, InMemoryBucketBroker, InMemoryMessageBroker, Repid


async def test_connection() -> None:
    app = Repid(
        Connection(
            InMemoryMessageBroker(),
            InMemoryBucketBroker(),
            InMemoryBucketBroker(use_result_bucket=True),
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
        Connection(InMemoryMessageBroker()),
        Connection(InMemoryMessageBroker(), args_bucket_broker=InMemoryBucketBroker()),
        Connection(
            InMemoryMessageBroker(),
            results_bucket_broker=InMemoryBucketBroker(use_result_bucket=True),
        ),
        Connection(
            InMemoryMessageBroker(),
            InMemoryBucketBroker(),
            InMemoryBucketBroker(use_result_bucket=True),
        ),
    ],
)
def test_update_config(connection: Connection) -> None:
    Repid(connection, update_config=True)
