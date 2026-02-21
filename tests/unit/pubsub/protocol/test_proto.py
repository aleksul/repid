import pytest

from repid.connections.pubsub.protocol import proto


def test_varint_encoding() -> None:
    assert proto._encode_varint(0) == b"\x00"
    assert proto._encode_varint(1) == b"\x01"
    assert proto._encode_varint(127) == b"\x7f"
    assert proto._encode_varint(128) == b"\x80\x01"
    assert proto._encode_varint(300) == b"\xac\x02"

    # Negative numbers (handled as unsigned 64-bit)
    assert proto._encode_varint(-1) == b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01"


def test_varint_decoding() -> None:
    val, pos = proto._decode_varint(b"\x00", 0)
    assert val == 0
    assert pos == 1

    val, pos = proto._decode_varint(b"\x80\x01", 0)
    assert val == 128
    assert pos == 2

    # Truncated
    with pytest.raises(ValueError, match="Truncated varint"):
        proto._decode_varint(b"\x80", 0)


def test_tag_encoding_decoding() -> None:
    encoded = proto._encode_tag(1, proto.WIRE_TYPE_VARINT)
    assert encoded == b"\x08"  # (1 << 3) | 0 = 8

    field_num, wire_type, pos = proto._decode_tag(encoded, 0)
    assert field_num == 1
    assert wire_type == proto.WIRE_TYPE_VARINT
    assert pos == 1


def test_string_bytes_encoding() -> None:
    assert proto._encode_string("hello") == b"\x05hello"
    assert proto._encode_bytes(b"world") == b"\x05world"

    data, pos = proto._decode_length_delimited(b"\x05hello", 0)
    assert data == b"hello"
    assert pos == 6

    # Truncated
    with pytest.raises(ValueError, match="Truncated length-delimited field"):
        proto._decode_length_delimited(b"\x05hel", 0)


def test_skip_field() -> None:
    # Varint
    pos = proto._skip_field(b"\x08", 0, proto.WIRE_TYPE_VARINT)
    assert pos == 1  # 0000 1000 -> 0x08, value 8, 1 byte

    # Fixed64
    pos = proto._skip_field(b"12345678", 0, proto.WIRE_TYPE_FIXED64)
    assert pos == 8

    # Length delimited
    pos = proto._skip_field(b"\x04abcd", 0, proto.WIRE_TYPE_LENGTH_DELIMITED)
    assert pos == 5

    # Fixed32
    pos = proto._skip_field(b"1234", 0, proto.WIRE_TYPE_FIXED32)
    assert pos == 4

    # Unknown
    with pytest.raises(ValueError, match="Unknown wire type"):
        proto._skip_field(b"", 0, 7)


def test_pubsub_message_serialization() -> None:
    msg = proto.PubsubMessage(
        data=b"hello",
        attributes={"key": "value"},
        message_id="123",
        publish_time_seconds=100,
        publish_time_nanos=500,
        ordering_key="order",
    )
    serialized = msg.serialize()
    deserialized = proto.PubsubMessage.deserialize(serialized)

    assert deserialized.data == b"hello"
    assert deserialized.attributes == {"key": "value"}
    assert deserialized.message_id == "123"
    assert deserialized.publish_time_seconds == 100
    assert deserialized.publish_time_nanos == 500
    assert deserialized.ordering_key == "order"


def test_pubsub_message_empty() -> None:
    msg = proto.PubsubMessage()
    serialized = msg.serialize()
    assert serialized == b""
    deserialized = proto.PubsubMessage.deserialize(serialized)
    assert deserialized.data == b""


def test_publish_request_serialization() -> None:
    msg = proto.PubsubMessage(data=b"test")
    req = proto.PublishRequest(topic="topic", messages=[msg])
    serialized = req.serialize()
    deserialized = proto.PublishRequest.deserialize(serialized)

    assert deserialized.topic == "topic"
    assert len(deserialized.messages) == 1
    assert deserialized.messages[0].data == b"test"


def test_publish_response_serialization() -> None:
    resp = proto.PublishResponse(message_ids=["1", "2"])
    serialized = resp.serialize()
    deserialized = proto.PublishResponse.deserialize(serialized)

    assert deserialized.message_ids == ["1", "2"]


def test_received_message_serialization() -> None:
    msg = proto.PubsubMessage(data=b"content")
    recv_msg = proto.ReceivedMessage(
        ack_id="ack1",
        message=msg,
        delivery_attempt=1,
    )
    serialized = recv_msg.serialize()
    deserialized = proto.ReceivedMessage.deserialize(serialized)

    assert deserialized.ack_id == "ack1"
    assert deserialized.message is not None
    assert deserialized.message.data == b"content"
    assert deserialized.delivery_attempt == 1


def test_acknowledge_request_serialization() -> None:
    req = proto.AcknowledgeRequest(
        subscription="sub",
        ack_ids=["a1", "a2"],
    )
    serialized = req.serialize()
    deserialized = proto.AcknowledgeRequest.deserialize(serialized)

    assert deserialized.subscription == "sub"
    assert deserialized.ack_ids == ["a1", "a2"]


def test_modify_ack_deadline_request_serialization() -> None:
    req = proto.ModifyAckDeadlineRequest(
        subscription="sub",
        ack_ids=["a1"],
        ack_deadline_seconds=60,
    )
    serialized = req.serialize()
    deserialized = proto.ModifyAckDeadlineRequest.deserialize(serialized)

    assert deserialized.subscription == "sub"
    assert deserialized.ack_ids == ["a1"]
    assert deserialized.ack_deadline_seconds == 60


def test_streaming_pull_request_serialization() -> None:
    req = proto.StreamingPullRequest(
        subscription="sub",
        ack_ids=["a1"],
        modify_deadline_seconds=[10],
        modify_deadline_ack_ids=["a2"],
        stream_ack_deadline_seconds=30,
        client_id="client1",
        max_outstanding_messages=100,
        max_outstanding_bytes=1000,
    )
    serialized = req.serialize()
    deserialized = proto.StreamingPullRequest.deserialize(serialized)

    assert deserialized.subscription == "sub"
    assert deserialized.ack_ids == ["a1"]
    assert deserialized.modify_deadline_seconds == [10]
    assert deserialized.modify_deadline_ack_ids == ["a2"]
    assert deserialized.stream_ack_deadline_seconds == 30
    assert deserialized.client_id == "client1"
    assert deserialized.max_outstanding_messages == 100
    assert deserialized.max_outstanding_bytes == 1000


def test_streaming_pull_response_serialization() -> None:
    msg = proto.PubsubMessage(data=b"data")
    recv_msg = proto.ReceivedMessage(ack_id="ack", message=msg)
    resp = proto.StreamingPullResponse(received_messages=[recv_msg])

    serialized = resp.serialize()
    deserialized = proto.StreamingPullResponse.deserialize(serialized)

    assert len(deserialized.received_messages) == 1
    assert deserialized.received_messages[0].ack_id == "ack"


def test_skip_unknown_fields_in_deserialization() -> None:
    # Construct a valid message but append an unknown field at the end
    # Tag 99, wire type VARINT (0), value 1
    unknown_field = proto._encode_tag(99, proto.WIRE_TYPE_VARINT) + proto._encode_varint(1)

    # Test with PubsubMessage
    msg = proto.PubsubMessage(data=b"test")
    serialized = msg.serialize() + unknown_field

    deserialized = proto.PubsubMessage.deserialize(serialized)
    assert deserialized.data == b"test"


def test_timestamp_encoding_decoding_variants() -> None:
    # Only seconds
    data = proto._encode_timestamp(10, 0)
    assert proto._decode_timestamp(data) == (10, 0)

    # Only nanos
    data = proto._encode_timestamp(0, 500)
    assert proto._decode_timestamp(data) == (0, 500)

    # Both
    data = proto._encode_timestamp(10, 500)
    assert proto._decode_timestamp(data) == (10, 500)

    # Unknown fields in timestamp
    unknown = proto._encode_tag(99, proto.WIRE_TYPE_VARINT) + proto._encode_varint(1)
    # Append unknown to valid data
    assert proto._decode_timestamp(data + unknown) == (10, 500)


def test_deserialize_unknown_fields_in_messages() -> None:
    unknown_field = proto._encode_tag(99, proto.WIRE_TYPE_VARINT) + proto._encode_varint(1)

    # PubsubMessage
    base_pubsub = proto.PubsubMessage(data=b"abc").serialize()
    pubsub_msg = proto.PubsubMessage.deserialize(base_pubsub + unknown_field)
    assert pubsub_msg.data == b"abc"

    # PublishRequest
    base_pub_req = proto.PublishRequest(topic="t").serialize()
    pub_req = proto.PublishRequest.deserialize(base_pub_req + unknown_field)
    assert pub_req.topic == "t"

    # PublishResponse
    base_pub_resp = proto.PublishResponse(message_ids=["1"]).serialize()
    pub_resp = proto.PublishResponse.deserialize(base_pub_resp + unknown_field)
    assert pub_resp.message_ids == ["1"]

    # ReceivedMessage
    base_recv = proto.ReceivedMessage(ack_id="a").serialize()
    recv_msg = proto.ReceivedMessage.deserialize(base_recv + unknown_field)
    assert recv_msg.ack_id == "a"

    # AcknowledgeRequest
    base_ack = proto.AcknowledgeRequest(subscription="s").serialize()
    ack_req = proto.AcknowledgeRequest.deserialize(base_ack + unknown_field)
    assert ack_req.subscription == "s"


def test_map_entry_unknown_fields() -> None:
    # Testing _decode_map_entry with unknown field
    # Map entry is: key(1), value(2)
    entry_bytes = (
        proto._encode_tag(1, proto.WIRE_TYPE_LENGTH_DELIMITED)
        + proto._encode_string("k")
        + proto._encode_tag(2, proto.WIRE_TYPE_LENGTH_DELIMITED)
        + proto._encode_string("v")
    )
    unknown = proto._encode_tag(99, proto.WIRE_TYPE_VARINT) + proto._encode_varint(1)

    k, v = proto._decode_map_entry(entry_bytes + unknown)
    assert k == "k"
    assert v == "v"


def test_deserialize_extra_types_unknown_fields() -> None:
    unknown_field = proto._encode_tag(99, proto.WIRE_TYPE_VARINT) + proto._encode_varint(1)

    # ModifyAckDeadlineRequest
    base_mod = proto.ModifyAckDeadlineRequest(
        subscription="s",
        ack_ids=["a"],
        ack_deadline_seconds=10,
    ).serialize()
    mod_req = proto.ModifyAckDeadlineRequest.deserialize(base_mod + unknown_field)
    assert mod_req.subscription == "s"
    assert mod_req.ack_ids == ["a"]
    assert mod_req.ack_deadline_seconds == 10

    # StreamingPullRequest
    base_str = proto.StreamingPullRequest(
        subscription="s",
        ack_ids=["a"],
        stream_ack_deadline_seconds=10,
    ).serialize()
    str_req = proto.StreamingPullRequest.deserialize(base_str + unknown_field)
    assert str_req.subscription == "s"
    assert str_req.stream_ack_deadline_seconds == 10

    # StreamingPullResponse
    base_str_resp = proto.StreamingPullResponse(
        received_messages=[proto.ReceivedMessage(ack_id="a")],
    ).serialize()
    str_resp = proto.StreamingPullResponse.deserialize(base_str_resp + unknown_field)
    assert len(str_resp.received_messages) == 1
    assert str_resp.received_messages[0].ack_id == "a"
