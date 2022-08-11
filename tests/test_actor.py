import pytest

from repid import Worker
from repid.actor import Actor


async def test_inappropriate_actor_inputs(fake_connection):
    w = Worker()

    with pytest.raises(ValueError, match="Actor name must"):

        @w.actor(name="some!@#$%^inappropriate_name")
        def a():
            pass

    with pytest.raises(ValueError, match="Queue name must"):

        @w.actor(queue="some!@#$%^inappropriate_name")
        def b():
            pass


def test_actor_str():
    def a():
        pass

    assert str(Actor(a)) == "Actor(a, name='a', queue='default')"
