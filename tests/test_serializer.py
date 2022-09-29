"""
from repid.job import Job
from repid.serializer import MessageSerializer


def test_simple_message(fake_connection):
    j = Job("my-awesome-job")
    msg = j._to_message()
    encoded = MessageSerializer.encode(msg)
    decoded = MessageSerializer.decode(encoded)
    assert msg == decoded
"""
