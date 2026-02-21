from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from repid.connections.pubsub.protocol import resilience


def test_resilience_config_defaults() -> None:
    config = resilience.ResilienceConfig()
    assert config.max_attempts == 5
    assert config.base_delay == 1.0


def test_resilience_state_init() -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    assert state.config == config
    assert state.attempt_count == 0


def test_calculate_delay() -> None:
    config = resilience.ResilienceConfig(
        base_delay=1.0,
        max_delay=10.0,
        jitter_factor=0.25,
    )
    state = resilience.ResilienceState(config)

    # Exponential backoff: base_delay * 2^(attempt-1)
    # attempt=1: 1.0 * 2^0 = 1.0
    # attempt=2: 1.0 * 2^1 = 2.0
    # attempt=3: 1.0 * 2^2 = 4.0

    # We can't easily check exact value due to jitter, but we can check range

    state._attempt_count = 1
    delay1 = state.calculate_delay()
    # 1.0 +/- 25% -> [0.75, 1.25]
    assert 0.75 <= delay1 <= 1.25

    state._attempt_count = 3
    delay3 = state.calculate_delay()
    # 4.0 +/- 25% -> [3.0, 5.0]
    assert 3.0 <= delay3 <= 5.0

    # Test explicit attempt
    delay_explicit = state.calculate_delay(attempt=1)
    assert 0.75 <= delay_explicit <= 1.25


@patch("asyncio.sleep")
async def test_with_retry_generator_immediate_success(mock_sleep: AsyncMock) -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    async def gen() -> AsyncIterator[int]:
        yield 1
        yield 2

    results = []
    async for val in resilience.with_retry_generator(state, gen):
        results.append(val)

    assert results == [1, 2]
    mock_sleep.assert_not_called()


async def test_resilience_reset() -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)
    await state.record_failure()
    assert state.attempt_count == 1

    await state.reset()
    assert state.attempt_count == 0
    assert state._last_success_time is None


async def test_with_retry_generator_non_retryable() -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    err = grpc.aio.AioRpcError(
        grpc.StatusCode.PERMISSION_DENIED,
        MagicMock(spec=grpc.aio.Metadata),
        MagicMock(spec=grpc.aio.Metadata),
        "denied",
    )

    async def fail_gen() -> AsyncIterator[int]:
        raise err
        yield 1

    with pytest.raises(grpc.aio.AioRpcError):
        async for _ in resilience.with_retry_generator(state, fail_gen):
            pass


def test_is_retryable() -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    # Retryable
    err_unavailable = MagicMock(spec=grpc.aio.AioRpcError)
    err_unavailable.code.return_value = grpc.StatusCode.UNAVAILABLE
    assert state.is_retryable(err_unavailable)

    # Terminating
    err_denied = MagicMock(spec=grpc.aio.AioRpcError)
    err_denied.code.return_value = grpc.StatusCode.PERMISSION_DENIED
    assert not state.is_retryable(err_denied)

    # Unknown (not in terminating list)
    err_out_range = MagicMock(spec=grpc.aio.AioRpcError)
    err_out_range.code.return_value = grpc.StatusCode.OUT_OF_RANGE
    assert state.is_retryable(err_out_range)  # True because it's not terminating


@patch("asyncio.sleep")
async def test_with_retry_success(mock_sleep: AsyncMock) -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    op = AsyncMock(return_value="success")
    result = await resilience.with_retry(state, op)

    assert result == "success"
    op.assert_called_once()
    mock_sleep.assert_not_called()
    assert state.attempt_count == 0


@patch("asyncio.sleep")
async def test_with_retry_failure_then_success(mock_sleep: AsyncMock) -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    err = grpc.aio.AioRpcError(
        grpc.StatusCode.UNAVAILABLE,
        MagicMock(spec=grpc.aio.Metadata),
        MagicMock(spec=grpc.aio.Metadata),
        "unavailable",
    )

    op = AsyncMock(side_effect=[err, "success"])

    result = await resilience.with_retry(state, op)

    assert result == "success"
    assert op.call_count == 2
    mock_sleep.assert_called_once()


@patch("asyncio.sleep")
async def test_with_retry_exhausted(_mock_sleep: AsyncMock) -> None:  # noqa: PT019
    config = resilience.ResilienceConfig(max_attempts=2)
    state = resilience.ResilienceState(config)

    err = grpc.aio.AioRpcError(
        grpc.StatusCode.UNAVAILABLE,
        MagicMock(spec=grpc.aio.Metadata),
        MagicMock(spec=grpc.aio.Metadata),
        "unavailable",
    )

    op = AsyncMock(side_effect=err)

    with pytest.raises(resilience.ReconnectionExhaustedError):
        await resilience.with_retry(state, op)

    assert op.call_count == 2


@patch("asyncio.sleep")
async def test_with_retry_non_retryable_error(mock_sleep: AsyncMock) -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    err = grpc.aio.AioRpcError(
        grpc.StatusCode.PERMISSION_DENIED,
        MagicMock(spec=grpc.aio.Metadata),
        MagicMock(spec=grpc.aio.Metadata),
        "denied",
    )

    op = AsyncMock(side_effect=err)

    with pytest.raises(grpc.aio.AioRpcError):
        await resilience.with_retry(state, op)

    assert op.call_count == 1
    mock_sleep.assert_not_called()


async def test_resilient_operation_context() -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    # Success
    async with resilience.resilient_operation(state):
        pass
    assert state._last_success_time is not None

    # Failure
    err = grpc.aio.AioRpcError(
        grpc.StatusCode.UNKNOWN,
        MagicMock(spec=grpc.aio.Metadata),
        MagicMock(spec=grpc.aio.Metadata),
        "fail",
    )
    with pytest.raises(grpc.aio.AioRpcError):
        async with resilience.resilient_operation(state):
            raise err

    assert state.attempt_count == 1


def test_reconnection_exhausted_error() -> None:
    err = resilience.ReconnectionExhaustedError(3, ValueError("foo"))
    assert err.attempts == 3
    assert isinstance(err.last_error, ValueError)
    assert str(err) == "Failed to reconnect after 3 attempts. Last error: foo"


@patch("asyncio.sleep")
async def test_with_retry_generator(mock_sleep: AsyncMock) -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    # Mock iterator that yields 1, raises UNAVAILABLE, then yields 2
    # But generator needs to be recreated on retry.

    call_count = 0

    async def fast_gen() -> AsyncIterator[int]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            yield 1
            err = grpc.aio.AioRpcError(
                grpc.StatusCode.UNAVAILABLE,
                MagicMock(spec=grpc.aio.Metadata),
                MagicMock(spec=grpc.aio.Metadata),
                "fail",
            )
            raise err
        else:
            yield 2

    gen_factory = MagicMock(side_effect=fast_gen)

    items = []
    async for item in resilience.with_retry_generator(state, gen_factory):
        items.append(item)

    assert items == [1, 2]
    assert gen_factory.call_count == 2
    mock_sleep.assert_called_once()


async def test_calculate_delay_no_args() -> None:
    config = resilience.ResilienceConfig(base_delay=1.0)
    state = resilience.ResilienceState(config)
    state._attempt_count = 2
    delay = state.calculate_delay()
    assert delay > 0


async def test_record_success_resets_if_stable() -> None:
    config = resilience.ResilienceConfig(stability_threshold=1.0)
    state = resilience.ResilienceState(config)
    state._attempt_count = 5
    state._last_success_time = 100.0

    with patch("time.monotonic", return_value=102.0):
        await state.record_success()

    assert state._attempt_count == 0


async def test_with_retry_generator_retries_fail() -> None:
    config = resilience.ResilienceConfig(max_attempts=2, base_delay=0.01)
    state = resilience.ResilienceState(config)

    rpc_error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="Try again",
    )

    call_count = 0

    async def gen() -> AsyncIterator[int]:
        nonlocal call_count
        call_count += 1
        yield 1
        raise rpc_error

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(resilience.ReconnectionExhaustedError):
            async for _ in resilience.with_retry_generator(state, gen):
                pass

        assert mock_sleep.call_count == 2
        assert call_count == 2


async def test_resilience_generator_exhausted() -> None:
    config = resilience.ResilienceConfig(max_attempts=1)
    rs = resilience.ResilienceState(config)

    await rs.record_failure()  # Attempt 1 used

    async def failing_gen() -> AsyncIterator[int]:
        yield 1

    with pytest.raises(resilience.ReconnectionExhaustedError):
        # Already exhausted, should raise immediately
        async for _ in resilience.with_retry_generator(rs, failing_gen):
            pass
