from __future__ import annotations

from repid.connections.amqp.protocol.reconnect import ReconnectConfig, ReconnectStrategy


async def test_reconnect_strategy_remaining_attempts() -> None:
    config = ReconnectConfig(max_attempts=5, initial_delay=0.01, max_delay=1.0)
    strategy = ReconnectStrategy(config)

    assert strategy.remaining_attempts == 5
    strategy._attempts = 2
    assert strategy.remaining_attempts == 3
    strategy._attempts = 5
    assert strategy.remaining_attempts == 0
    strategy._attempts = 10
    assert strategy.remaining_attempts == 0


async def test_reconnect_strategy_wait_if_possible() -> None:
    config = ReconnectConfig(max_attempts=2, initial_delay=0.001, max_delay=1.0)
    strategy = ReconnectStrategy(config)

    # First attempt should wait
    result = await strategy.wait_if_needed()
    assert result is True
    assert strategy.attempts == 1

    # Second attempt should wait
    result = await strategy.wait_if_needed()
    assert result is True
    assert strategy.attempts == 2

    # Third attempt should not wait (exhausted)
    result = await strategy.wait_if_needed()
    assert result is False


async def test_reconnect_strategy_config_property() -> None:
    config = ReconnectConfig(enabled=True, max_attempts=5, initial_delay=1.0)
    strategy = ReconnectStrategy(config)

    assert strategy.config == config
    assert strategy.config.max_attempts == 5


async def test_reconnect_strategy_attempts_property() -> None:
    config = ReconnectConfig(enabled=True, max_attempts=5)
    strategy = ReconnectStrategy(config)

    assert strategy.attempts == 0
    strategy.record_attempt()
    assert strategy.attempts == 1


async def test_reconnect_strategy_reset() -> None:
    config = ReconnectConfig(enabled=True, max_attempts=5, initial_delay=1.0)
    strategy = ReconnectStrategy(config)

    # Make some attempts
    strategy.record_attempt()
    strategy.record_attempt()
    assert strategy.attempts == 2

    # Reset should clear attempts
    strategy.reset()
    assert strategy.attempts == 0
    assert strategy._next_delay == config.initial_delay


async def test_reconnect_strategy_wait_updates_attempts() -> None:
    strategy = ReconnectStrategy(
        ReconnectConfig(enabled=True, max_attempts=2, initial_delay=0.0, jitter=0.0),
    )

    assert strategy.should_retry() is True
    await strategy.wait()
    assert strategy.attempts == 1
    assert strategy.should_retry() is True

    await strategy.wait()
    assert strategy.attempts == 2
    assert strategy.should_retry() is False
