from repid.connections.in_memory.utils import DummyQueue


async def test_dummy_queue_message_eq_and_hash() -> None:
    msg1 = DummyQueue.Message(payload=b"1", headers={"a": "b"}, content_type="json", message_id="1")
    msg2 = DummyQueue.Message(payload=b"1", headers={"a": "b"}, content_type="json", message_id="1")
    msg3 = DummyQueue.Message(payload=b"2", headers={"a": "b"}, content_type="json", message_id="1")

    assert msg1 == msg2
    assert msg1 != msg3
    assert msg1 != "some info"

    assert hash(msg1) == hash(msg2)
    assert hash(msg1) != hash(msg3)

    # Test with no headers for hash
    msg_no_headers = DummyQueue.Message(payload=b"1")
    msg_no_headers2 = DummyQueue.Message(payload=b"1")
    assert hash(msg_no_headers) == hash(msg_no_headers2)
