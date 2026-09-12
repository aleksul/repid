from repid.connections.in_memory.utils import DummyQueue


async def test_dummy_queue_message_uses_stable_delivery_identity() -> None:
    msg1 = DummyQueue.Message(payload=b"1", headers={"a": "b"}, content_type="json", message_id="1")
    msg2 = DummyQueue.Message(payload=b"1", headers={"a": "b"}, content_type="json", message_id="1")

    deliveries = {msg1, msg2}
    msg1.headers["a"] = "changed"  # type: ignore[index]

    assert msg1 is not msg2
    assert msg1 != msg2
    assert len(deliveries) == 2
    assert msg1 in deliveries
