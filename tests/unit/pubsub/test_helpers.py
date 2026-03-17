import pytest

from repid.connections.pubsub.helpers import ChannelOverride


def test_channel_override_init_empty() -> None:
    override = ChannelOverride()
    assert override._data == {}


def test_channel_override_init_all() -> None:
    override = ChannelOverride(
        project="p",
        topic="t",
        subscription="s",
    )
    assert override._data == {
        "project": "p",
        "topic": "t",
        "subscription": "s",
    }


def test_channel_override_getitem() -> None:
    override = ChannelOverride(project="p")
    assert override["project"] == "p"
    with pytest.raises(KeyError):
        _ = override["topic"]


def test_channel_override_contains() -> None:
    override = ChannelOverride(project="p")
    assert "project" in override
    assert "topic" not in override


def test_channel_override_get() -> None:
    override = ChannelOverride(project="p")
    assert override.get("project") == "p"
    assert override.get("topic") is None
    assert override.get("topic", "default") == "default"
