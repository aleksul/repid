from __future__ import annotations

from collections.abc import Callable, Coroutine
from concurrent.futures import Future, ThreadPoolExecutor
from typing import cast

import pytest

from repid import Router
from repid._utils import NotSet
from repid.connections.abc import ReceivedMessageT
from repid.converter import BasicConverter
from repid.data import ActorData, Channel, MessageData
from repid.middlewares import ActorMiddlewareT
from repid.router import catch_all_routing_strategy, topic_based_routing_strategy


class TrackingThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self) -> None:
        super().__init__(max_workers=1)
        self.submit_calls = 0

    def submit(
        self,
        fn: Callable[..., object],
        /,
        *args: object,
        **kwargs: object,
    ) -> Future:
        self.submit_calls += 1
        return super().submit(fn, *args, **kwargs)


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


def test_router_channels_uses_materialized_channels() -> None:
    router = Router(channel=Channel(address="custom_channel"))

    @router.actor
    async def actor1() -> None:
        pass

    assert len(router.channels) == 1
    assert router.channels[0].address == "custom_channel"


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

    @router2.actor
    async def my_actor() -> None:
        pass

    router1.include_router(router2)

    assert router1.actors[0].timeout == 30.0
    assert router2.timeout is NotSet


def test_include_router_propagates_keep_alive() -> None:
    router1 = Router(keep_alive=15.0)
    router2 = Router()

    @router2.actor
    async def my_actor() -> None:
        pass

    router1.include_router(router2)

    assert router1.actors[0].keep_alive == 15.0
    assert router2.keep_alive is NotSet


def test_include_router_does_not_mutate_run_in_process() -> None:
    router1 = Router(run_in_process=True)
    router2 = Router()

    @router2.actor
    async def my_actor() -> None:
        pass

    router1.include_router(router2)

    assert router2.run_in_process is NotSet


async def test_include_router_propagates_pool_executor() -> None:
    with TrackingThreadPoolExecutor() as executor:
        router1 = Router(pool_executor=executor)
        router2 = Router()

        @router2.actor
        def my_actor() -> str:
            return "ok"

        router1.include_router(router2)

        assert await router1.actors[0].fn() == "ok"
        assert executor.submit_calls == 1
        assert router2.pool_executor is NotSet


def test_include_router_propagates_converter() -> None:
    router1 = Router(converter=BasicConverter)
    router2 = Router()

    @router2.actor
    async def my_actor() -> None:
        pass

    router1.include_router(router2)

    assert type(router1.actors[0].converter) is BasicConverter
    assert router2.converter is NotSet


def test_include_router_then_add_actor_works() -> None:
    router1 = Router()
    router2 = Router()

    router1.include_router(router2)

    @router2.actor
    async def my_actor() -> None:
        pass

    assert len(router1.actors) == 1
    assert len(router2.actors) == 1


def test_include_router_then_add_actor_then_include() -> None:
    router1 = Router()
    router2 = Router()

    router1.include_router(router2)

    @router2.actor
    async def my_actor() -> None:
        pass

    router1.include_router(router2)

    assert len(router1.actors) == 1
    assert len(router2.actors) == 1


def test_include_router_multiple_times() -> None:
    router1 = Router()
    router2 = Router()

    @router2.actor
    async def my_actor() -> None:
        pass

    for _ in range(5):
        router1.include_router(router2)

    assert len(router1.actors) == 1
    assert len(router2.actors) == 1


def test_include_router_multiple_times_keeps_original_position() -> None:
    router1 = Router()
    router2 = Router()

    @router1.actor
    async def first_actor() -> None:
        pass

    router1.include_router(router2)

    @router1.actor
    async def last_actor() -> None:
        pass

    @router2.actor
    async def child_actor() -> None:
        pass

    router1.include_router(router2)

    assert [actor.name for actor in router1.actors] == [
        "first_actor",
        "child_actor",
        "last_actor",
    ]


def test_include_router_late_nested_actor_inherits_defaults() -> None:
    router1 = Router(channel="grandparent", timeout=5.0)
    router2 = Router()
    router3 = Router()

    router1.include_router(router2)
    router2.include_router(router3)

    @router3.actor
    async def deep_actor() -> None:
        pass

    assert len(router1.actors) == 1
    assert router1.actors[0].name == "deep_actor"
    assert router1.actors[0].channel_address == "grandparent"
    assert router1.actors[0].timeout == 5.0
    assert router2.channel is NotSet
    assert router3.channel is NotSet
    assert router2.timeout is NotSet
    assert router3.timeout is NotSet


async def test_include_router_combines_middlewares_without_mutating_child() -> None:
    calls: list[str] = []

    async def parent_middleware(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[object, object, object]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> object:
        calls.append("parent_enter")
        result = await call_next(message, actor)
        calls.append("parent_exit")
        return result

    async def child_middleware(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[object, object, object]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> object:
        calls.append("child_enter")
        result = await call_next(message, actor)
        calls.append("child_exit")
        return result

    router1 = Router(middlewares=[cast(ActorMiddlewareT, parent_middleware)])
    router2 = Router(middlewares=[cast(ActorMiddlewareT, child_middleware)])

    @router2.actor
    async def my_actor() -> None:
        calls.append("actor")

    router1.include_router(router2)

    pipeline = router1.actors[0].middleware_pipeline

    async def mock_leaf(_message: ReceivedMessageT, actor: ActorData) -> None:
        await actor.fn()

    await pipeline(mock_leaf, None, router1.actors[0])  # type: ignore[arg-type]

    assert calls == [
        "parent_enter",
        "child_enter",
        "actor",
        "child_exit",
        "parent_exit",
    ]
    assert router2.middlewares == [child_middleware]


def test_include_router_raises_on_self_include() -> None:
    router = Router()

    with pytest.raises(ValueError, match="cycle"):
        router.include_router(router)


def test_include_router_raises_on_ancestor_include() -> None:
    router1 = Router()
    router2 = Router()
    router3 = Router()

    router1.include_router(router2)
    router2.include_router(router3)

    with pytest.raises(ValueError, match="cycle"):
        router3.include_router(router1)


def test_contains_router_returns_false_for_seen_router() -> None:
    router1 = Router()
    router2 = Router()

    assert router1._contains_router(router2, {id(router1)}) is False


def test_actor_raises_on_both_run_in_process_and_pool_executor() -> None:
    router = Router()
    executor = ThreadPoolExecutor(max_workers=1)

    with pytest.raises(
        ValueError,
        match=r"Specify either 'run_in_process' or 'pool_executor', not both\.",
    ):

        @router.actor(run_in_process=True, pool_executor=executor)
        def my_actor() -> None:
            pass


@pytest.mark.parametrize("confirmation_mode", ["ack_first", "always_ack"])
def test_actor_raises_on_invalid_confirmation_mode_and_on_error(confirmation_mode: str) -> None:
    router = Router()

    with pytest.raises(
        ValueError,
        match=r"The 'on_error' parameter is not compatible with 'ack_first' or 'always_ack' "
        r"confirmation modes, as the message will always be acknowledged\.",
    ):

        @router.actor(confirmation_mode=confirmation_mode, on_error="reject")  # type: ignore[call-overload]
        def my_actor() -> None:
            pass
