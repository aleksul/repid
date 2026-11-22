"""Resilience mechanisms for Pub/Sub gRPC operations.

Each unary call retries with exponential backoff and jitter. A circuit breaker
shared by every call and stream of a server opens after too many consecutive
failures, then lets one probe call through after a cooldown.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TypeVar

import grpc
import grpc.aio

from repid._utils._typing import override

T = TypeVar("T")


class ReconnectionExhaustedError(Exception):
    """Raised when all reconnection attempts have been exhausted."""

    def __init__(self, attempts: int, last_error: Exception | None = None) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Failed to reconnect after {attempts} attempts. Last error: {last_error}",
        )


class CircuitOpenError(ReconnectionExhaustedError):
    """Raised without contacting Pub/Sub while the circuit breaker is open."""

    @override
    def __str__(self) -> str:
        return (
            f"Circuit breaker is open after {self.attempts} consecutive failures. "
            f"Last error: {self.last_error}"
        )


@dataclass(slots=True)
class ResilienceConfig:
    """Configuration for resilience mechanisms.

    Attributes:
        max_attempts: Maximum number of attempts per unary call.
        base_delay: Initial delay between retries in seconds.
        max_delay: Maximum delay between retries in seconds.
        jitter_factor: Factor for randomizing delay (0.0 to 1.0).
            A value of 0.25 means ±25% jitter.
        stability_threshold: Seconds a streaming pull must stay connected before
            its responses count as recovery.
        failure_threshold: Consecutive failed attempts, across every call and
            stream, that open the circuit breaker.
        open_duration: Seconds the circuit breaker stays open before one probe
            call may try again.
        retryable_status_codes: gRPC status codes that should trigger retry.
    """

    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 32.0
    jitter_factor: float = 0.25
    stability_threshold: float = 60.0
    failure_threshold: int = 10
    open_duration: float = 30.0
    retryable_status_codes: frozenset[grpc.StatusCode] = field(
        default_factory=lambda: frozenset(
            {
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.ABORTED,
                grpc.StatusCode.INTERNAL,
                grpc.StatusCode.UNKNOWN,
            },
        ),
    )
    terminating_status_codes: frozenset[grpc.StatusCode] = field(
        default_factory=lambda: frozenset(
            {
                grpc.StatusCode.CANCELLED,
                grpc.StatusCode.INVALID_ARGUMENT,
                grpc.StatusCode.NOT_FOUND,
                grpc.StatusCode.PERMISSION_DENIED,
                grpc.StatusCode.UNAUTHENTICATED,
            },
        ),
    )


class ResilienceState:
    """Circuit breaker shared by every call and stream of one server.

    It counts consecutive failed attempts. Any successful call closes it, as
    does a response on a stream that has stayed connected long enough. Once the
    count reaches `failure_threshold`, calls fail without contacting Pub/Sub and
    streams stop reconnecting. After `open_duration`, one probe call may try:
    success closes the breaker, failure keeps it open for another period.
    """

    def __init__(self, config: ResilienceConfig) -> None:
        self._config = config
        self._attempt_count = 0
        self._opened_at: float | None = None
        self._probing = False

    @property
    def config(self) -> ResilienceConfig:
        """Get the resilience configuration."""
        return self._config

    @property
    def attempt_count(self) -> int:
        """Current number of consecutive failed attempts."""
        return self._attempt_count

    def begin_attempt(self, last_error: Exception | None = None) -> bool:
        """Admit one attempt of a call and return whether it is the probe.

        Raises:
            CircuitOpenError: The breaker is open and its probe is not due or
                already running.
        """
        if self._opened_at is None:
            return False
        cooling_down = time.monotonic() - self._opened_at < self._config.open_duration
        if self._probing or cooling_down:
            raise CircuitOpenError(self._attempt_count, last_error)
        self._probing = True
        return True

    def end_probe(self) -> None:
        """Release the probe slot after an attempt that reached no verdict."""
        self._probing = False

    async def record_success(self) -> None:
        """Record that Pub/Sub answered, closing the breaker."""
        self._attempt_count = 0
        self._opened_at = None
        self._probing = False

    async def record_stream_success(self, connected_at: float) -> None:
        """Record a stream response, which counts once the stream proved stable.

        A stream that connects and fails again right away must not hide the
        failures around it.
        """
        if time.monotonic() - connected_at >= self._config.stability_threshold:
            await self.record_success()

    async def record_failure(self) -> int:
        """Record a failed attempt and return the new consecutive count."""
        self._attempt_count += 1
        self._probing = False
        if self._attempt_count >= self._config.failure_threshold:
            # A failed probe starts a new open period.
            self._opened_at = time.monotonic()
        return self._attempt_count

    async def reset(self) -> None:
        """Reset state completely."""
        self._attempt_count = 0
        self._opened_at = None
        self._probing = False

    def calculate_delay(self, attempt: int | None = None) -> float:
        """Calculate the delay for the next retry attempt with jitter.

        Args:
            attempt: The attempt number (1-based). If None, uses current count.

        Returns:
            The delay in seconds, including jitter.
        """
        if attempt is None:
            attempt = self._attempt_count

        # Exponential backoff: base_delay * 2^(attempt-1)
        delay: float = self._config.base_delay * (2 ** (attempt - 1))
        delay = min(delay, self._config.max_delay)

        # Apply jitter: delay ± (delay * jitter_factor)
        jitter_range = delay * self._config.jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)  # noqa: S311

        return max(0.0, delay + jitter)

    def is_retryable(self, error: grpc.aio.AioRpcError) -> bool:
        """Check if an error should trigger a retry.

        Returns True if the error is explicitly retryable or NOT explicitly terminating.
        """
        code = error.code()
        if code in self._config.retryable_status_codes:
            return True
        return code not in self._config.terminating_status_codes

    def should_retry(self) -> bool:
        """Whether the breaker is closed, so a stream may reconnect."""
        return self._opened_at is None


@asynccontextmanager
async def resilient_operation(
    state: ResilienceState,
    _operation_name: str = "operation",
) -> AsyncIterator[None]:
    """Context manager for resilient operations.

    Tracks success/failure and updates the resilience state.
    Does NOT handle retries - that's the caller's responsibility.

    Usage:
        async with resilient_operation(state, "publish"):
            await do_something()

    Args:
        state: The shared resilience state.
        operation_name: Name of the operation for logging.

    Yields:
        None

    Raises:
        grpc.aio.AioRpcError: Re-raised if the operation fails.
    """
    try:
        yield
        await state.record_success()
    except grpc.aio.AioRpcError:
        await state.record_failure()
        raise


async def with_retry(
    state: ResilienceState,
    operation: Callable[[], Awaitable[T]],
    operation_name: str = "operation",
) -> T:
    """Execute an operation with automatic retry on transient failures.

    Each call makes up to `max_attempts` attempts with its own backoff. Every
    attempt also reports to the shared circuit breaker, which stops the call
    early once it opens.

    Args:
        state: The server's resilience state.
        operation: The async operation to execute.
        operation_name: Name for logging purposes.

    Returns:
        The result of the operation.

    Raises:
        ReconnectionExhaustedError: If all retry attempts fail.
        CircuitOpenError: If the circuit breaker is open.
        grpc.aio.AioRpcError: If the error is not retryable.
    """
    last_error: Exception | None = None
    _ = operation_name  # For future logging use

    max_attempts = state.config.max_attempts
    for attempt in range(1, max_attempts + 1):
        probe = state.begin_attempt(last_error)
        try:
            result = await operation()
        except grpc.aio.AioRpcError as e:
            last_error = e
            if not state.is_retryable(e):
                # Pub/Sub answered, so it is reachable.
                await state.record_success()
                raise
            await state.record_failure()
            if attempt < max_attempts:
                await asyncio.sleep(state.calculate_delay(attempt))
            continue
        except BaseException:
            if probe:
                state.end_probe()
            raise
        await state.record_success()
        return result

    raise ReconnectionExhaustedError(max_attempts, last_error)


async def with_retry_generator(
    state: ResilienceState,
    create_iterator: Callable[[], AsyncIterator[T]],
    operation_name: str = "stream",
) -> AsyncIterator[T]:
    """Execute a streaming operation with automatic reconnection on failure.

    Args:
        state: The shared resilience state.
        create_iterator: Factory to create the async iterator.
        operation_name: Name for logging purposes.

    Yields:
        Items from the stream.

    Raises:
        ReconnectionExhaustedError: If all reconnection attempts fail.
        grpc.aio.AioRpcError: If the error is not retryable.
    """
    last_error: Exception | None = None
    _ = operation_name  # For future logging use

    while state.should_retry():
        try:
            iterator = create_iterator()
            connected_at = time.monotonic()
            async for item in iterator:
                await state.record_stream_success(connected_at)
                yield item
            # Stream completed normally
            return
        except grpc.aio.AioRpcError as e:
            last_error = e
            await state.record_failure()

            if not state.is_retryable(e):
                raise
            if not state.should_retry():
                break

            delay = state.calculate_delay()
            await asyncio.sleep(delay)

    raise ReconnectionExhaustedError(state.attempt_count, last_error)
