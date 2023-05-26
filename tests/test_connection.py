import pytest

from repid import Connection, InMemoryBucketBroker, InMemoryMessageBroker


def test_result_bucket_check() -> None:
    Connection(
        InMemoryMessageBroker(),
        results_bucket_broker=InMemoryBucketBroker(use_result_bucket=True),
    )

    with pytest.raises(ValueError, match="Results bucket"):
        Connection(
            InMemoryMessageBroker(),
            results_bucket_broker=InMemoryBucketBroker(use_result_bucket=False),
        )


def test_args_bucket_broker_shortcut() -> None:
    with_args_broker = Connection(
        InMemoryMessageBroker(),
        args_bucket_broker=InMemoryBucketBroker(),
    )
    with_args_broker._ab  # noqa: B018

    without_args_broker = Connection(InMemoryMessageBroker())
    with pytest.raises(ValueError, match="Args bucket broker"):
        without_args_broker._ab  # noqa: B018


def test_result_bucket_broker_shortcut() -> None:
    with_result_broker = Connection(
        InMemoryMessageBroker(),
        results_bucket_broker=InMemoryBucketBroker(use_result_bucket=True),
    )
    with_result_broker._rb  # noqa: B018

    without_result_broker = Connection(InMemoryMessageBroker())
    with pytest.raises(ValueError, match="Results bucket broker"):
        without_result_broker._rb  # noqa: B018


async def test_is_open() -> None:
    conn = Connection(InMemoryMessageBroker())

    assert not conn.is_open

    await conn.connect()
    assert conn.is_open

    await conn.disconnect()
    assert not conn.is_open


def test_sets_middleware() -> None:
    msg_broker, args_broker, result_broker = (
        InMemoryMessageBroker(),
        InMemoryBucketBroker(),
        InMemoryBucketBroker(use_result_bucket=True),
    )

    assert msg_broker._signal_emitter is None
    assert args_broker._signal_emitter is None
    assert result_broker._signal_emitter is None

    Connection(msg_broker, args_broker, result_broker)

    assert msg_broker._signal_emitter is not None
    assert args_broker._signal_emitter is not None
    assert result_broker._signal_emitter is not None


def test_in_memory_renaming_deprecation() -> None:
    from repid import DummyBucketBroker, DummyMessageBroker

    with pytest.warns(
        DeprecationWarning,
        match="DummyBucketBroker was renamed to InMemoryBucketBroker.",
    ):
        DummyBucketBroker()

    with pytest.warns(
        DeprecationWarning,
        match="DummyBucketBroker was renamed to InMemoryBucketBroker.",
    ):
        DummyBucketBroker(use_result_bucket=True)

    with pytest.warns(
        DeprecationWarning,
        match="DummyMessageBroker was renamed to InMemoryMessageBroker.",
    ):
        DummyMessageBroker()


def test_in_memory_module_renaming_deprecation() -> None:
    with pytest.warns(
        DeprecationWarning,
        match="Module repid.connections.dummy was renamed to repid.connections.in_memory",
    ):
        from repid.connections.dummy import DummyBucketBroker, DummyMessageBroker  # noqa: F401
