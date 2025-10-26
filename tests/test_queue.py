import pytest

from repid import Queue

pytestmark = pytest.mark.usefixtures("fake_connection")


def test_inappropriate_queue_name() -> None:
    with pytest.raises(ValueError, match="Queue name must"):
        Queue("some!@#$%^inappropriate_name")


def test_different_types_not_equal() -> None:
    q = Queue()
    assert q != 123


def test_str() -> None:
    q = Queue()
    assert str(q) == "Queue(default)"
