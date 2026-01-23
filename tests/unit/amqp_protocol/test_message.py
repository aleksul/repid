from __future__ import annotations

from repid.connections.amqp._uamqp._decode import transfer_frames_to_message
from repid.connections.amqp._uamqp._encode import message_to_transfer_frames
from repid.connections.amqp._uamqp.message import Message


def test_message_encoding_with_various_properties() -> None:
    # Test message with application properties
    msg = Message(
        data=b"test data",
        application_properties={"key": "value", "number": 42},
    )

    # Encode to transfer frames
    frames = list(
        message_to_transfer_frames(
            message=msg,
            handle=1,
            delivery_id=10,
            delivery_tag=b"tag",
            settled=False,
            max_frame_size=4096,
        ),
    )

    assert len(frames) >= 1
    assert frames[0].handle == 1
    assert frames[0].delivery_id == 10
    assert frames[0].delivery_tag == b"tag"

    # Decode back
    decoded_msg = transfer_frames_to_message(frames)
    assert decoded_msg.data == b"test data"
    assert decoded_msg.application_properties == {"key": "value", "number": 42}


def test_message_encoding_multi_frame() -> None:
    # Test with large data that will span multiple frames
    large_data = b"x" * 10000
    msg = Message(data=large_data)

    frames = list(
        message_to_transfer_frames(
            message=msg,
            handle=2,
            delivery_id=20,
            delivery_tag=b"large_tag",
            settled=True,
            max_frame_size=512,
        ),
    )

    # Should create multiple frames for large data
    assert len(frames) > 1
    decoded_msg = transfer_frames_to_message(frames)
    assert decoded_msg.data == large_data


def test_message_with_footer() -> None:
    # Test with footer
    msg = Message(data=b"data", footer={"footer_key": "footer_value"})

    frames = list(
        message_to_transfer_frames(
            message=msg,
            handle=3,
            delivery_id=30,
            delivery_tag=b"footer_tag",
            settled=True,
            max_frame_size=1024,
        ),
    )

    assert len(frames) >= 1
    decoded_msg = transfer_frames_to_message(frames)
    assert decoded_msg.data == b"data"
    assert decoded_msg.footer == {"footer_key": "footer_value"}
