from typing import Any

from repid.connections.pubsub.protocol import _helpers


def test_channel_config() -> None:
    async def cb(msg: Any) -> None:
        pass

    config = _helpers.ChannelConfig(
        channel="c",
        subscription_path="s",
        callback=cb,
    )
    assert config.channel == "c"
    assert config.subscription_path == "s"
    assert config.callback == cb


def test_queued_delivery() -> None:
    async def cb(msg: Any) -> None:
        pass

    msg: Any = "msg"

    delivery = _helpers.QueuedDelivery(
        callback=cb,
        message=msg,
    )
    assert delivery.callback == cb
    assert delivery.message == msg
