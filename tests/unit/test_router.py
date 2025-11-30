from __future__ import annotations

from repid import Router
from repid.data.channel import Channel


def test_empty_router() -> None:
    router = Router()
    assert len(router.actors) == 0


def test_router_includes_other_router() -> None:
    router = Router()
    other_router = Router()

    @other_router.actor
    async def actor1() -> None:
        pass

    router.include_router(other_router)

    # Check that the actor was included
    assert len(router.actors) == 1
    assert router.actors[0].name == "actor1"


def test_router_decorator_registers_actor() -> None:
    router = Router()

    @router.actor
    async def actor1() -> None:
        pass

    # Check that the actor was registered
    assert len(router.actors) == 1
    assert router.actors[0].name == "actor1"


def test_router_decorator_overrides_actor_name() -> None:
    router = Router()

    @router.actor(name="actor1_renamed")
    async def actor1() -> None:
        pass

    # Check that the renamed actor exists
    assert len(router.actors) == 1
    assert router.actors[0].name == "actor1_renamed"


def test_router_with_defaults() -> None:
    router = Router(channel=Channel(address="custom_channel"))

    @router.actor
    async def actor1() -> None:
        pass

    # Check that the actor is registered on the custom channel
    assert len(router.actors) == 1
    assert router.actors[0].channel_address == "custom_channel"


def test_nested_router_include() -> None:
    router1 = Router()
    router2 = Router()
    router3 = Router()

    @router3.actor
    async def deep_actor() -> None:
        pass

    router2.include_router(router3)
    router1.include_router(router2)

    # Check that the deeply nested actor was included
    assert len(router1.actors) == 1
    assert router1.actors[0].name == "deep_actor"


def test_router_defaults_propagation() -> None:
    router1 = Router(channel=Channel(address="channel1"))
    router2 = Router()

    @router2.actor
    async def actor_in_router2() -> None:
        pass

    router1.include_router(router2)

    # Check that the actor in router2 has the default channel from router1
    assert len(router1.actors) == 1
    assert router1.actors[0].channel_address == "channel1"
    assert router1.actors[0].name == "actor_in_router2"


def test_router_defaults_propagation_override() -> None:
    router1 = Router(channel=Channel(address="channel1"))
    router2 = Router(channel=Channel(address="channel2"))

    @router2.actor
    async def actor_in_router2() -> None:
        pass

    router1.include_router(router2)

    # Check that the actor in router2 has the default channel from router2
    assert len(router2.actors) == 1
    assert router2.actors[0].channel_address == "channel2"
    assert router2.actors[0].name == "actor_in_router2"
