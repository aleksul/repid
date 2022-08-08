import pytest

from repid import Queue


def test_inappropriate_queue_name(fake_connection):
    with pytest.raises(ValueError, match="Queue name must"):
        Queue("some!@#$%^inappropriate_name")


def test_different_types_not_equal(fake_connection):
    q = Queue()
    assert not (q == 123)


def test_str(fake_connection):
    q = Queue()
    assert str(q) == "Queue(default)"
