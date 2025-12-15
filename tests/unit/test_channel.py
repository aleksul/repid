import pytest

from repid import Channel


def test_channel_equality() -> None:
    channel1 = Channel(address="test_channel", title="Channel 1")
    channel2 = Channel(address="test_channel", title="Channel 2")
    channel3 = Channel(address="other_channel", title="Channel 3")

    assert channel1 == channel2
    assert channel1 != channel3


def test_channel_equality_with_non_channel_raises() -> None:
    channel = Channel(address="test_channel")

    with pytest.raises(ValueError, match="Cannot compare Channel with"):
        channel == "not a channel"  # noqa: B015
