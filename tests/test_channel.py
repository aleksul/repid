from repid import Channel, ExternalDocs


def test_channel_creation() -> None:
    Channel(
        address="test_channel",
        title="Channel 1",
        summary="A test channel summary",
        description="A test channel description",
        parameters={"param1": {"description": "string"}},
        bindings={"amqp1": {"someBindingKey": "someBindingValue"}},
        external_docs=ExternalDocs(url="https://example.com/docs", description="Channel docs"),
    )


def test_channel_equality() -> None:
    channel1 = Channel(address="test_channel", title="Channel 1")
    channel2 = Channel(address="test_channel", title="Channel 2")
    channel3 = Channel(address="other_channel", title="Channel 3")

    assert channel1 == channel2
    assert channel1 != channel3
