import pytest

from repid import Router


async def test_inappropriate_actor_inputs() -> None:
    r = Router()

    with pytest.raises(ValueError, match="Actor name must"):

        @r.actor(name="some!@#$%^inappropriate_name")
        def a() -> None:
            pass

    with pytest.raises(ValueError, match="Queue name must"):

        @r.actor(queue="some!@#$%^inappropriate_name")
        def b() -> None:
            pass
