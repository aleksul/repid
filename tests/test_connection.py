import pytest

from repid import Connection, DummyBucketBroker, DummyMessageBroker


def test_result_bucket_check() -> None:
    Connection(
        DummyMessageBroker(),
        results_bucket_broker=DummyBucketBroker(use_result_bucket=True),
    )

    with pytest.raises(ValueError, match="Results bucket"):
        Connection(
            DummyMessageBroker(),
            results_bucket_broker=DummyBucketBroker(use_result_bucket=False),
        )


def test_args_bucket_broker_shortcut() -> None:
    with_args_broker = Connection(
        DummyMessageBroker(),
        args_bucket_broker=DummyBucketBroker(),
    )
    with_args_broker._ab

    without_args_broker = Connection(DummyMessageBroker())
    with pytest.raises(ValueError, match="Args bucket broker"):
        without_args_broker._ab


def test_result_bucket_broker_shortcut() -> None:
    with_result_broker = Connection(
        DummyMessageBroker(),
        results_bucket_broker=DummyBucketBroker(use_result_bucket=True),
    )
    with_result_broker._rb

    without_result_broker = Connection(DummyMessageBroker())
    with pytest.raises(ValueError, match="Results bucket broker"):
        without_result_broker._rb


async def test_is_open() -> None:
    conn = Connection(DummyMessageBroker())

    assert not conn.is_open

    await conn.connect()
    assert conn.is_open

    await conn.disconnect()
    assert not conn.is_open


def test_sets_middleware() -> None:
    msg_broker, args_broker, result_broker = (
        DummyMessageBroker(),
        DummyBucketBroker(),
        DummyBucketBroker(True),
    )

    assert msg_broker._signal_emitter is None
    assert args_broker._signal_emitter is None
    assert result_broker._signal_emitter is None

    Connection(msg_broker, args_broker, result_broker)

    assert msg_broker._signal_emitter is not None
    assert args_broker._signal_emitter is not None
    assert result_broker._signal_emitter is not None
