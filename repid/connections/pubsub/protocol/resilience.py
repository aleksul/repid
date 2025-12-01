"""Resilience mechanisms for Pub/Sub gRPC operations.

This module provides shared retry logic with exponential backoff,
jitter, and stability-based reset for both publishing and subscribing.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TypeVar

import grpc.aio

T = TypeVar("T")


class ReconnectionExhaustedError(Exception):
    """Raised when all reconnection attempts have been exhausted."""

    def __init__(self, attempts: int, last_error: Exception | None = None) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Failed to reconnect after {attempts} attempts. Last error: {last_error}",
        )


@dataclass(slots=True)
class ResilienceConfig:
    """Configuration for resilience mechanisms.

    Attributes:
        max_attempts: Maximum number of retry/reconnection attempts.
        base_delay: Initial delay between retries in seconds.
        max_delay: Maximum delay between retries in seconds.
        jitter_factor: Factor for randomizing delay (0.0 to 1.0).
            A value of 0.25 means ±25% jitter.
        stability_threshold: Time in seconds of successful operation
            after which the attempt counter resets to 0.
        retryable_status_codes: gRPC status codes that should trigger retry.
    """

    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 32.0
    jitter_factor: float = 0.25
    stability_threshold: float = 60.0
    retryable_status_codes: frozenset[grpc.StatusCode] = field(
        default_factory=lambda: frozenset(
            {
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.ABORTED,
                grpc.StatusCode.INTERNAL,
            },
        ),
    )


class ResilienceState:
    """Tracks state for resilience mechanisms.

    This class is thread-safe and can be shared between operations
    to provide unified retry tracking across publish and subscribe.
    """

    def __init__(self, config: ResilienceConfig) -> None:
        self._config = config
        self._attempt_count = 0
        self._last_success_time: float | None = None
        self._lock = asyncio.Lock()

    @property
    def config(self) -> ResilienceConfig:
        """Get the resilience configuration."""
        return self._config

    @property
    def attempt_count(self) -> int:
        """Current number of consecutive failed attempts."""
        return self._attempt_count

    async def record_success(self) -> None:
        """Record a successful operation, potentially resetting attempt count."""
        async with self._lock:
            now = time.monotonic()

            # If we've been stable for long enough, reset attempt count
            if (
                self._last_success_time is not None
                and now - self._last_success_time >= self._config.stability_threshold
            ):
                self._attempt_count = 0

            self._last_success_time = now

    async def record_failure(self) -> int:
        """Record a failed operation and return the new attempt count."""
        async with self._lock:
            self._attempt_count += 1
            return self._attempt_count

    async def reset(self) -> None:
        """Reset state completely."""
        async with self._lock:
            self._attempt_count = 0
            self._last_success_time = None

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
        """Check if an error should trigger a retry."""
        return error.code() in self._config.retryable_status_codes

    def should_retry(self) -> bool:
        """Check if we should attempt another retry."""
        return self._attempt_count < self._config.max_attempts


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

    Args:
        state: The shared resilience state.
        operation: The async operation to execute.
        operation_name: Name for logging purposes.

    Returns:
        The result of the operation.

    Raises:
        ReconnectionExhaustedError: If all retry attempts fail.
        grpc.aio.AioRpcError: If the error is not retryable.
    """
    last_error: Exception | None = None
    _ = operation_name  # For future logging use

    while state.should_retry():
        try:
            async with resilient_operation(state):
                return await operation()
        except grpc.aio.AioRpcError as e:
            last_error = e

            if not state.is_retryable(e):
                raise

            delay = state.calculate_delay()
            await asyncio.sleep(delay)

    raise ReconnectionExhaustedError(state.attempt_count, last_error)


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
            async for item in iterator:
                await state.record_success()
                yield item
            # Stream completed normally
            return
        except grpc.aio.AioRpcError as e:
            last_error = e
            await state.record_failure()

            if not state.is_retryable(e):
                raise

            delay = state.calculate_delay()
            await asyncio.sleep(delay)

    raise ReconnectionExhaustedError(state.attempt_count, last_error)
