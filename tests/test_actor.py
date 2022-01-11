import pytest
from aioredis import Redis

from repid import Worker
from repid.actor import Actor


def test_inappropriate_actor_inputs(redis: Redis):
    w = Worker(redis)

    with pytest.raises(ValueError, match="Actor name must"):

        @w.actor(name="some!@#$%^inappropriate_name")
        def a():
            pass

    with pytest.raises(ValueError, match="Queue name must"):

        @w.actor(queue="some!@#$%^inappropriate_name")
        def b():
            pass

    with pytest.raises(ValueError, match="Number of retries"):

        @w.actor(retries=0)
        def c():
            pass


def test_actor_repr(redis: Redis):
    def a():
        pass

    assert Actor(a).__repr__() == f"Actor({a}, name=a, queue=default, retries=1)"


def test_actor_str(redis: Redis):
    def a():
        pass

    assert str(Actor(a)) == "Actor(a)"
