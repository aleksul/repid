"""
AMQP Reconnection Strategy - Configurable backoff for reconnection.

This module provides reconnection logic with:
- Exponential backoff
- Jitter to prevent thundering herd
- Configurable retry limits
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ReconnectConfig:
    """Configuration for reconnection behavior."""

    enabled: bool = True
    max_attempts: int = 10
    initial_delay: float = 0.1
    max_delay: float = 30.0
    multiplier: float = 2.0
    jitter: float = 0.1  # Random jitter factor (0.0 - 1.0)


class ReconnectStrategy:
    """
    Implements exponential backoff with jitter for reconnection.

    Pattern: Strategy
    - Configurable backoff parameters
    - Jitter prevents thundering herd
    - Tracks attempt count for circuit breaking

    Example:
        strategy = ReconnectStrategy(ReconnectConfig())

        while strategy.should_retry():
            try:
                await connect()
                strategy.reset()  # Success
                break
            except ConnectionError:
                await strategy.wait()
    """

    def __init__(self, config: ReconnectConfig | None = None) -> None:
        self._config = config or ReconnectConfig()
        self._attempts = 0
        self._next_delay = self._config.initial_delay

    @property
    def config(self) -> ReconnectConfig:
        """Get the configuration."""
        return self._config

    @property
    def attempts(self) -> int:
        """Get the current attempt count."""
        return self._attempts

    @property
    def remaining_attempts(self) -> int:
        """Get remaining retry attempts."""
        return max(0, self._config.max_attempts - self._attempts)

    def reset(self) -> None:
        """Reset after successful connection."""
        self._attempts = 0
        self._next_delay = self._config.initial_delay

    def should_retry(self) -> bool:
        """Check if we should attempt reconnection."""
        return self._config.enabled and self._attempts < self._config.max_attempts

    def get_delay(self) -> float:
        """
        Get the next delay with jitter.

        Does not update internal state - call `record_attempt()` after waiting.
        """
        jitter = self._next_delay * self._config.jitter * random.random()  # noqa: S311
        return self._next_delay + jitter

    def record_attempt(self) -> None:
        """Record an attempt and update delay for next attempt."""
        self._attempts += 1
        self._next_delay = min(
            self._next_delay * self._config.multiplier,
            self._config.max_delay,
        )

    async def wait(self) -> None:
        """
        Wait before next reconnection attempt.

        Updates internal state after waiting.
        """
        delay = self.get_delay()

        logger.info(
            "Reconnect attempt %d/%d in %.2fs",
            self._attempts + 1,
            self._config.max_attempts,
            delay,
        )

        await asyncio.sleep(delay)
        self.record_attempt()

    async def wait_if_needed(self) -> bool:
        """
        Wait if retry is possible, return False if retries exhausted.

        Returns:
            True if waited and should retry, False if no more retries.
        """
        if not self.should_retry():
            return False

        await self.wait()
        return True
