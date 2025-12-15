from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from repid import Router
from repid.converter import BasicConverter
from repid.data import Channel, MessageData
from repid.router import catch_all_routing_strategy, topic_based_routing_strategy


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


def test_topic_based_routing_strategy_no_headers() -> None:
    strategy = topic_based_routing_strategy(actor_name="test_actor")
    result = strategy(MessageData(payload=b"", headers=None))

    assert result is False


def test_topic_based_routing_strategy_matching() -> None:
    strategy = topic_based_routing_strategy(actor_name="test_actor")
    result = strategy(MessageData(payload=b"", headers={"topic": "test_actor"}))

    assert result is True


def test_topic_based_routing_strategy_not_matching() -> None:
    strategy = topic_based_routing_strategy(actor_name="test_actor")
    result = strategy(MessageData(payload=b"", headers={"topic": "other_actor"}))

    assert result is False


def test_catch_all_routing_strategy() -> None:
    strategy = catch_all_routing_strategy(actor_name="any")
    result = strategy(MessageData(payload=b"", headers=None))

    assert result is True


def test_include_router_propagates_timeout() -> None:
    router1 = Router(timeout=30.0)
    router2 = Router()

    router1.include_router(router2)

    assert router2.timeout == 30.0


def test_include_router_propagates_run_in_process() -> None:
    router1 = Router(run_in_process=True)
    router2 = Router()

    router1.include_router(router2)

    assert router2.run_in_process is True


def test_include_router_propagates_pool_executor() -> None:
    executor = ThreadPoolExecutor(max_workers=2)
    router1 = Router(pool_executor=executor)
    router2 = Router()

    router1.include_router(router2)

    assert router2.pool_executor is executor


def test_include_router_propagates_converter() -> None:
    router1 = Router(converter=BasicConverter)
    router2 = Router()

    router1.include_router(router2)

    assert router2.converter is BasicConverter
