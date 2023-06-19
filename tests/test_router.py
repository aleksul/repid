from __future__ import annotations

from collections import defaultdict

import pytest

from repid.actor import ActorData
from repid.converter import BasicConverter
from repid.retry_policy import RetryPolicyT, default_retry_policy_factory
from repid.router import Router, RouterDefaults


def test_empty_router() -> None:
    router = Router()
    assert router.actors == {}
    assert router.topics == frozenset()
    assert router.queues == frozenset()


def test_router_with_default_queue() -> None:
    defaults = RouterDefaults(queue="default_queue")
    router = Router(defaults=defaults)
    assert router.defaults.queue == "default_queue"


def test_router_includes_other_router() -> None:
    router = Router()
    other_router = Router()

    fn = lambda x: x  # noqa: E731
    actor1 = ActorData(
        fn=fn,
        name="actor1",
        queue="queue1",
        retry_policy=default_retry_policy_factory(),
        converter=BasicConverter(fn),
    )
    other_router.actors = {"actor1": actor1}
    temp = defaultdict(set)
    temp["queue1"].add("actor1")
    other_router.topics_by_queue = temp

    router.include_router(other_router)

    assert router.actors == {"actor1": actor1}
    assert router.topics == frozenset({"actor1"})
    assert router.queues == frozenset({"queue1"})


def test_router_decorator_registers_actor() -> None:
    router = Router()

    @router.actor
    def actor1() -> None:
        pass

    assert router.actors["actor1"]
    assert router.topics == frozenset({"actor1"})


def test_router_decorator_overrides_actor_name() -> None:
    router = Router()

    @router.actor(name="actor1_renamed")
    def actor1() -> None:
        pass

    assert router.actors["actor1_renamed"]
    assert router.topics == frozenset({"actor1_renamed"})


@pytest.mark.parametrize(
    ("queue", "retry_policy", "converter"),
    [
        (None, None, None),
        ("actor1_queue", default_retry_policy_factory(), BasicConverter),
        (None, default_retry_policy_factory(), BasicConverter),
        (None, None, BasicConverter),
    ],
)
def test_router_decorator_registers_actor_with_defaults(
    queue: str | None,
    retry_policy: RetryPolicyT | None,
    converter: type[BasicConverter] | None,
) -> None:
    router = Router()
    defaults = RouterDefaults(queue="default_queue")
    router.defaults = defaults

    @router.actor(queue=queue, retry_policy=retry_policy, converter=converter)
    def actor1() -> None:
        pass

    if queue is None:
        assert router.actors["actor1"].queue == "default_queue"
    else:
        assert router.actors["actor1"].queue == queue

    if retry_policy is not None:
        assert router.actors["actor1"].retry_policy is retry_policy

    assert isinstance(router.actors["actor1"].converter, BasicConverter)


def test_router_decorator_uses_default_retry_policy() -> None:
    expected_retry_policy = default_retry_policy_factory(min_backoff=1)
    defaults = RouterDefaults(retry_policy=expected_retry_policy)

    router = Router(defaults=defaults)

    @router.actor
    def actor1() -> None:
        pass

    assert router.actors["actor1"].retry_policy is expected_retry_policy
