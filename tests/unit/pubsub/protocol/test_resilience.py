import asyncio
import time
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from repid.connections.pubsub.protocol import resilience


def test_calculate_delay() -> None:
    config = resilience.ResilienceConfig(
        base_delay=1.0,
        max_delay=10.0,
        jitter_factor=0.25,
    )
    state = resilience.ResilienceState(config)

    state._attempt_count = 1
    delay1 = state.calculate_delay()
    assert 0.75 <= delay1 <= 1.25

    state._attempt_count = 3
    delay3 = state.calculate_delay()
    assert 3.0 <= delay3 <= 5.0

    delay_explicit = state.calculate_delay(attempt=1)
    assert 0.75 <= delay_explicit <= 1.25


@patch.object(asyncio, "sleep")
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
    assert state.should_retry()


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


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        pytest.param(grpc.StatusCode.UNAVAILABLE, True, id="transient_error"),
        pytest.param(grpc.StatusCode.PERMISSION_DENIED, False, id="terminating_error"),
        pytest.param(grpc.StatusCode.OUT_OF_RANGE, True, id="nonterminating_error"),
    ],
)
def test_is_retryable(code: grpc.StatusCode, expected: bool) -> None:
    state = resilience.ResilienceState(resilience.ResilienceConfig())
    error = grpc.aio.AioRpcError(code, grpc.aio.Metadata(), grpc.aio.Metadata())

    assert state.is_retryable(error) is expected


@patch.object(asyncio, "sleep")
async def test_with_retry_success(mock_sleep: AsyncMock) -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)

    op = AsyncMock(return_value="success")
    result = await resilience.with_retry(state, op)

    assert result == "success"
    op.assert_called_once()
    mock_sleep.assert_not_called()
    assert state.attempt_count == 0


@patch.object(asyncio, "sleep")
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


@patch.object(asyncio, "sleep")
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


@patch.object(asyncio, "sleep")
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


async def test_resilient_operation_context_records_success() -> None:
    config = resilience.ResilienceConfig()
    state = resilience.ResilienceState(config)
    await state.record_failure()

    async with resilience.resilient_operation(state):
        pass
    assert state.attempt_count == 0


async def test_resilient_operation_context_records_failure() -> None:
    state = resilience.ResilienceState(resilience.ResilienceConfig())
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


@patch.object(asyncio, "sleep")
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


@pytest.mark.parametrize(
    ("connected_for", "expected_count"),
    [
        pytest.param(0.5, 5, id="just_reconnected"),
        pytest.param(1.0, 0, id="stable"),
    ],
)
async def test_stream_response_counts_once_the_stream_is_stable(
    connected_for: float,
    expected_count: int,
) -> None:
    config = resilience.ResilienceConfig(stability_threshold=1.0)
    state = resilience.ResilienceState(config)
    state._attempt_count = 5

    with patch.object(time, "monotonic", return_value=100.0 + connected_for):
        await state.record_stream_success(connected_at=100.0)

    assert state.attempt_count == expected_count


async def test_with_retry_generator_retries_fail() -> None:
    config = resilience.ResilienceConfig(failure_threshold=2, base_delay=0.01)
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

    with patch.object(asyncio, "sleep", new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(resilience.ReconnectionExhaustedError):
            async for _ in resilience.with_retry_generator(state, gen):
                pass

        # The second failure opens the breaker, so there is no third connect.
        assert mock_sleep.call_count == 1
        assert call_count == 2


async def test_resilience_generator_exhausted() -> None:
    config = resilience.ResilienceConfig(failure_threshold=1)
    rs = resilience.ResilienceState(config)

    await rs.record_failure()  # Opens the breaker

    async def failing_gen() -> AsyncIterator[int]:
        yield 1

    with pytest.raises(resilience.ReconnectionExhaustedError):
        # Already exhausted, should raise immediately
        async for _ in resilience.with_retry_generator(rs, failing_gen):
            pass


def _unavailable() -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(
        grpc.StatusCode.UNAVAILABLE,
        grpc.aio.Metadata(),
        grpc.aio.Metadata(),
        "unavailable",
    )


def _breaker(*, max_attempts: int = 5, open_duration: float = 30.0) -> resilience.ResilienceState:
    config = resilience.ResilienceConfig(
        max_attempts=max_attempts,
        base_delay=0,
        max_delay=0,
        jitter_factor=0,
        failure_threshold=3,
        open_duration=open_duration,
    )
    return resilience.ResilienceState(config)


async def test_successful_calls_reset_the_shared_failure_count() -> None:
    # Steady traffic with one transient failure per call must never trip the breaker.
    state = _breaker()
    for _ in range(10):
        op = AsyncMock(side_effect=[_unavailable(), "ok"])
        assert await resilience.with_retry(state, op) == "ok"
        assert state.attempt_count == 0
    assert state.should_retry()


async def test_breaker_opens_after_consecutive_failures_and_fails_fast() -> None:
    state = _breaker(max_attempts=5)
    failing = AsyncMock(side_effect=_unavailable())

    with pytest.raises(resilience.CircuitOpenError) as opened:
        await resilience.with_retry(state, failing)
    assert failing.call_count == 3
    assert isinstance(opened.value.last_error, grpc.aio.AioRpcError)
    assert not state.should_retry()

    untouched = AsyncMock(return_value="ok")
    with pytest.raises(resilience.CircuitOpenError):
        await resilience.with_retry(state, untouched)
    untouched.assert_not_called()


async def test_failures_from_different_calls_add_up() -> None:
    state = _breaker(max_attempts=2)
    with pytest.raises(resilience.ReconnectionExhaustedError) as exhausted:
        await resilience.with_retry(state, AsyncMock(side_effect=_unavailable()))
    assert not isinstance(exhausted.value, resilience.CircuitOpenError)

    with pytest.raises(resilience.CircuitOpenError):
        await resilience.with_retry(state, AsyncMock(side_effect=_unavailable()))
    assert state.attempt_count == 3


async def test_successful_probe_after_cooldown_closes_the_breaker() -> None:
    state = _breaker(open_duration=30)
    with patch.object(time, "monotonic", return_value=100.0):
        for _ in range(3):
            await state.record_failure()

    with (
        patch.object(time, "monotonic", return_value=129.0),
        pytest.raises(resilience.CircuitOpenError),
    ):
        await resilience.with_retry(state, AsyncMock(return_value="ok"))

    with patch.object(time, "monotonic", return_value=130.0):
        assert await resilience.with_retry(state, AsyncMock(return_value="ok")) == "ok"
    assert state.should_retry()
    assert state.attempt_count == 0


async def test_failed_probe_keeps_the_breaker_open_for_another_period() -> None:
    state = _breaker(open_duration=30)
    with patch.object(time, "monotonic", return_value=100.0):
        for _ in range(3):
            await state.record_failure()

    probe = AsyncMock(side_effect=_unavailable())
    with (
        patch.object(time, "monotonic", return_value=130.0),
        pytest.raises(resilience.CircuitOpenError),
    ):
        await resilience.with_retry(state, probe)
    # The probe failed, and its retry was refused by the reopened breaker.
    probe.assert_called_once()

    with (
        patch.object(time, "monotonic", return_value=159.0),
        pytest.raises(resilience.CircuitOpenError),
    ):
        await resilience.with_retry(state, AsyncMock(return_value="ok"))


async def test_only_one_probe_runs_at_a_time() -> None:
    state = _breaker(open_duration=0)
    for _ in range(3):
        await state.record_failure()

    release = asyncio.Event()

    async def slow_probe() -> str:
        await release.wait()
        return "ok"

    probe = asyncio.create_task(resilience.with_retry(state, slow_probe))
    await asyncio.sleep(0)
    with pytest.raises(resilience.CircuitOpenError):
        await resilience.with_retry(state, AsyncMock(return_value="ok"))

    release.set()
    assert await probe == "ok"
    assert state.should_retry()


async def test_cancelled_probe_lets_the_next_call_probe() -> None:
    state = _breaker(open_duration=0)
    for _ in range(3):
        await state.record_failure()

    probe = asyncio.create_task(resilience.with_retry(state, asyncio.Event().wait))
    await asyncio.sleep(0)
    probe.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe

    assert await resilience.with_retry(state, AsyncMock(return_value="ok")) == "ok"


async def test_non_retryable_error_shows_pub_sub_is_reachable() -> None:
    state = _breaker()
    await state.record_failure()
    denied = grpc.aio.AioRpcError(
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.aio.Metadata(),
        grpc.aio.Metadata(),
        "denied",
    )

    with pytest.raises(grpc.aio.AioRpcError):
        await resilience.with_retry(state, AsyncMock(side_effect=denied))
    assert state.attempt_count == 0


def test_circuit_open_error_message() -> None:
    err = resilience.CircuitOpenError(10, ValueError("down"))
    assert isinstance(err, resilience.ReconnectionExhaustedError)
    assert str(err) == "Circuit breaker is open after 10 consecutive failures. Last error: down"
