import pytest

from repid import Channel, Concurrency


def test_channel_equality() -> None:
    channel1 = Channel(
        address="test_channel",
        title="Channel 1",
        concurrency=Concurrency(messages=1),
    )
    channel2 = Channel(
        address="test_channel",
        title="Channel 2",
        concurrency=Concurrency(messages=2),
    )
    channel3 = Channel(address="other_channel", title="Channel 3")

    assert channel1 == channel2
    assert channel1 != channel3
    assert hash(channel1) == hash(channel2)


def test_channel_equality_with_non_channel_raises() -> None:
    channel = Channel(address="test_channel")

    with pytest.raises(ValueError, match="Cannot compare Channel with"):
        channel == "not a channel"  # noqa: B015
