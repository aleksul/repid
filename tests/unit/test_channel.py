from typing import Any, cast

import pytest

from repid import Channel
from repid.limits import BackpressurePolicy, MessageLimits


def test_channel_equality() -> None:
    channel1 = Channel(address="test_channel", title="Channel 1")
    channel2 = Channel(address="test_channel", title="Channel 2")
    channel3 = Channel(address="other_channel", title="Channel 3")

    assert channel1 == channel2
    assert channel1 != channel3


def test_message_limits_reject_invalid_backpressure() -> None:
    with pytest.raises(TypeError, match="BackpressurePolicy"):
        MessageLimits(backpressure="invalid")  # type: ignore[arg-type]


def test_channel_equality_with_non_channel_raises() -> None:
    channel = Channel(address="test_channel")

    with pytest.raises(ValueError, match="Cannot compare Channel with"):
        channel == "not a channel"  # noqa: B015


def test_channel_limits_and_backpressure_defaults() -> None:
    limits = MessageLimits()

    channel = Channel(address="test_channel", limits=limits)

    assert channel.limits is limits
    assert channel.limits.backpressure is None


def test_channel_accepts_explicit_backpressure() -> None:
    policy = BackpressurePolicy(strategies=("native",), on_unavailable="error")
    limits = MessageLimits(backpressure=policy)
    channel = Channel(address="test_channel", limits=limits)

    assert channel.limits is limits
    assert channel.limits.backpressure is policy


def test_channel_deduplicates_limit_policies_by_identity() -> None:
    policy = cast(Any, object())

    channel = Channel(address="test_channel", limit_policies=(policy, policy))

    assert channel.limit_policies == (policy,)
