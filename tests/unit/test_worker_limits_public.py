from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest

from repid import ActorLimits, Channel, MessageLimits, Repid, ReservationLeaseT, Router
from repid.connections import SubscriberDispatcher
from repid.connections.abc import CapabilitiesT
from repid.connections.in_memory import InMemoryServer
from repid.data import MessageData


async def _run_one(
    app: Repid,
    server: InMemoryServer,
    *,
    channel: str = "default",
    topic: str,
    **kwargs: Any,
) -> None:
    app.servers.register_server("memory", server, is_default=True)
    async with server.connection():
        worker = asyncio.create_task(
            app.run_worker(messages_limit=1, register_signals=[], **kwargs),
        )
        await server.publish(
            channel=channel,
            message=MessageData(payload=b"{}", headers={"topic": topic}),
        )
        assert (await worker).processed == 1


async def test_run_worker_passes_prepared_dispatcher() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def default_actor() -> None:
        pass

    app.include_router(router)
    server = InMemoryServer()
    captured: list[object] = []
    subscribe = server.subscribe

    async def capture_subscribe(**kwargs: Any) -> Any:
        captured.append(kwargs["dispatcher"])
        return await subscribe(**kwargs)

    server.subscribe = capture_subscribe  # type: ignore[method-assign]
    await _run_one(app, server, topic="default_actor")

    default_dispatcher = cast(SubscriberDispatcher, captured[0])
    assert default_dispatcher.native_message_limit("default") == 1000

    explicit = MessageLimits()
    app = Repid()
    router = Router()

    @router.actor
    async def unlimited_actor() -> None:
        pass

    app.include_router(router)
    server = InMemoryServer()
    captured = []
    subscribe = server.subscribe

    async def capture_explicit_subscribe(**kwargs: Any) -> Any:
        captured.append(kwargs["dispatcher"])
        return await subscribe(**kwargs)

    server.subscribe = capture_explicit_subscribe  # type: ignore[method-assign]
    await _run_one(app, server, topic="unlimited_actor", limits=explicit)

    explicit_dispatcher = cast(SubscriberDispatcher, captured[0])
    assert explicit_dispatcher.native_message_limit("default") is None


async def test_run_worker_prepares_channel_limits_in_dispatcher() -> None:
    app = Repid()
    channel_limits = MessageLimits(max_messages=2)
    router = Router()

    @router.actor(channel=Channel(address="orders", limits=channel_limits))
    async def orders() -> None:
        pass

    app.include_router(router)
    server = InMemoryServer()
    captured: list[object] = []
    subscribe = server.subscribe

    async def capture_subscribe(**kwargs: Any) -> Any:
        captured.append(kwargs["dispatcher"])
        return await subscribe(**kwargs)

    server.subscribe = capture_subscribe  # type: ignore[method-assign]
    await _run_one(app, server, channel="orders", topic="orders")

    dispatcher = cast(SubscriberDispatcher, captured[0])
    assert dispatcher.native_message_limit("orders") == 2


async def test_run_worker_reserves_and_releases_custom_policy() -> None:
    events: list[str] = []

    class Lease:
        async def release(self) -> None:
            events.append("release")

    class LimitPolicy:
        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Callable[[], Awaitable[None]],  # noqa: ARG002
        ) -> ReservationLeaseT:
            await asyncio.sleep(0)
            events.extend(("estimate", "reserve:1"))
            return Lease()

    app = Repid()
    router = Router()

    @router.actor
    async def limited_actor() -> None:
        events.append("actor")

    app.include_router(router)
    await _run_one(
        app,
        InMemoryServer(),
        topic="limited_actor",
        limit_policies=(LimitPolicy(),),
    )

    assert events == ["estimate", "reserve:1", "actor", "release"]


async def test_run_worker_deduplicates_shared_policy_across_all_scopes() -> None:
    events: list[str] = []

    class Lease:
        async def release(self) -> None:
            events.append("release")

    class LimitPolicy:
        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Callable[[], Awaitable[None]],  # noqa: ARG002
        ) -> ReservationLeaseT:
            await asyncio.sleep(0)
            events.extend(("estimate", "reserve:1"))
            return Lease()

    shared_policy = LimitPolicy()
    router_limits = ActorLimits(max_messages=1)
    app = Repid()
    router = Router(limits=router_limits, limit_policies=(shared_policy,))

    @router.actor(
        channel=Channel(address="default", limit_policies=(shared_policy,)),
        limits=router_limits,
        limit_policies=(shared_policy,),
    )
    async def shared_actor() -> None:
        events.append("actor")

    app.include_router(router)
    await _run_one(
        app,
        InMemoryServer(),
        topic="shared_actor",
        limits=MessageLimits(max_messages=1),
        limit_policies=(shared_policy,),
    )

    assert events == ["estimate", "reserve:1", "actor", "release"]


async def test_run_worker_composes_nested_router_limit_policies() -> None:
    events: list[str] = []

    class Lease:
        def __init__(self, name: str) -> None:
            self.name = name

        async def release(self) -> None:
            events.append(f"release:{self.name}")

    class LimitPolicy:
        def __init__(self, name: str) -> None:
            self.name = name

        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Callable[[], Awaitable[None]],  # noqa: ARG002
        ) -> ReservationLeaseT:
            events.append(f"reserve:{self.name}")
            return Lease(self.name)

    parent = Router(limit_policies=(LimitPolicy("parent"),))
    child = Router(limit_policies=(LimitPolicy("child"),))

    @child.actor
    async def nested_actor() -> None:
        events.append("actor")

    parent.include_router(child)
    app = Repid()
    app.include_router(parent)
    await _run_one(app, InMemoryServer(), topic="nested_actor")

    assert set(events[:2]) == {"reserve:parent", "reserve:child"}
    assert events[2] == "actor"
    assert events[3:] == [event.replace("reserve", "release") for event in reversed(events[:2])]


async def test_run_worker_policy_owns_async_pricing_and_fallback() -> None:
    events: list[str] = []

    class Lease:
        async def release(self) -> None:
            events.append("release")

    class LimitPolicy:
        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Callable[[], Awaitable[None]],  # noqa: ARG002
        ) -> ReservationLeaseT:
            events.append("estimate")
            try:
                await asyncio.sleep(0)
                raise ValueError("estimate unavailable")
            except ValueError:
                cost = 2
            events.append(f"reserve:{cost}")
            return Lease()

    app = Repid()
    router = Router()

    @router.actor(limit_policies=(LimitPolicy(),))
    async def fallback_actor() -> None:
        events.append("actor")

    app.include_router(router)
    await _run_one(app, InMemoryServer(), topic="fallback_actor")

    assert events == ["estimate", "reserve:2", "actor", "release"]


async def test_run_worker_applies_router_count_and_actor_byte_execution_limits() -> None:
    app = Repid()
    router = Router(limits=ActorLimits(max_messages=1))
    started = asyncio.Event()
    release = asyncio.Event()
    running = 0
    peak = 0

    @router.actor(limits=ActorLimits(max_payload_bytes=2))
    async def constrained_actor() -> None:
        nonlocal peak, running
        running += 1
        peak = max(peak, running)
        started.set()
        await release.wait()
        running -= 1

    app.include_router(router)
    server = InMemoryServer()
    app.servers.register_server("memory", server, is_default=True)
    async with server.connection():
        worker = asyncio.create_task(app.run_worker(messages_limit=2, register_signals=[]))
        for _ in range(2):
            await server.publish(
                channel="default",
                message=MessageData(payload=b"{}", headers={"topic": "constrained_actor"}),
            )
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert peak == 1
        release.set()
        assert (await worker).processed == 2

    assert peak == 1


@pytest.mark.parametrize(
    ("action", "expected_queued"),
    [
        pytest.param("nack", False, id="nack"),
        pytest.param("reject", True, id="reject"),
        pytest.param(lambda message: "reject" if message.payload else "nack", True, id="dynamic"),
    ],
)
async def test_run_worker_applies_actor_oversized_payload_action(
    action: Any,
    expected_queued: bool,
) -> None:
    app = Repid()
    router = Router()
    ran = False

    @router.actor(limits=ActorLimits(max_payload_bytes=1, oversized_payload_action=action))
    async def oversized() -> None:
        nonlocal ran
        ran = True

    app.include_router(router)
    server = InMemoryServer()
    app.servers.register_server("memory", server, is_default=True)
    async with server.connection():
        worker = asyncio.create_task(
            app.run_worker(
                messages_limit=1,
                register_signals=[],
                actor_limits_propagation="off",
            ),
        )
        await server.publish(
            channel="default",
            message=MessageData(payload=b"{}", headers={"topic": "oversized"}),
        )
        assert (await worker).processed == 1
        assert server.queues["default"].queue.empty() is not expected_queued

    assert not ran


async def test_messages_limit_stops_before_extra_actors_start() -> None:
    app = Repid()
    server = InMemoryServer()
    started = 0
    router = Router()

    @router.actor
    async def limited_run() -> None:
        nonlocal started
        started += 1
        await asyncio.sleep(0)

    app.include_router(router)
    app.servers.register_server("memory", server, is_default=True)
    async with server.connection():
        for _ in range(3):
            await server.publish(
                channel="default",
                message=MessageData(payload=b"{}", headers={"topic": "limited_run"}),
            )
        result = await app.run_worker(messages_limit=1, register_signals=[])

    assert result.processed == 1
    assert started == 1


async def test_worker_shutdown_cancels_waiter_and_releases_partial_reservation() -> None:
    events: list[str] = []
    waiting = asyncio.Event()

    class Lease:
        def __init__(self, name: str) -> None:
            self.name = name

        async def release(self) -> None:
            events.append(f"release:{self.name}")

    class LimitPolicy:
        def __init__(self, name: str) -> None:
            self.name = name
            self.blocks = False

        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Callable[[], Awaitable[None]],
        ) -> ReservationLeaseT:
            events.append(f"reserve:{self.name}")
            if self.blocks:
                await on_wait()
                waiting.set()
                await asyncio.Future()
            return Lease(self.name)

    policies = [LimitPolicy("one"), LimitPolicy("two")]
    blocker = max(policies, key=id)
    blocker.blocks = True
    acquired = min(policies, key=id)
    app = Repid()
    router = Router(limit_policies=tuple(reversed(policies)))
    ran = 0

    @router.actor
    async def delayed_actor() -> None:
        nonlocal ran
        ran += 1

    app.include_router(router)
    server = InMemoryServer()
    app.servers.register_server("memory", server, is_default=True)
    async with server.connection():
        worker = asyncio.create_task(
            app.run_worker(
                graceful_shutdown_time=0,
                register_signals=[signal.SIGUSR1],
            ),
        )
        await server.publish(
            channel="default",
            message=MessageData(payload=b"{}", headers={"topic": "delayed_actor"}),
        )
        await asyncio.wait_for(waiting.wait(), timeout=1)
        signal.raise_signal(signal.SIGUSR1)
        await asyncio.wait_for(worker, timeout=1)

        assert events == [
            f"reserve:{acquired.name}",
            f"reserve:{blocker.name}",
            f"release:{acquired.name}",
        ]
        assert ran == 0

        blocker.blocks = False
        events.clear()
        result = await app.run_worker(messages_limit=1, register_signals=[])

    assert result.processed == 1
    assert ran == 1
    assert events == [
        f"reserve:{acquired.name}",
        f"reserve:{blocker.name}",
        f"release:{blocker.name}",
        f"release:{acquired.name}",
    ]


async def test_worker_shutdown_never_runs_delivery_waiting_at_intake() -> None:
    app = Repid()
    server = InMemoryServer()
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_started = False
    router = Router()

    @router.actor(channel=Channel(address="first"))
    async def first() -> None:
        first_started.set()
        await release_first.wait()

    @router.actor(channel=Channel(address="second"))
    async def second() -> None:
        nonlocal second_started
        second_started = True

    app.include_router(router)
    app.servers.register_server("memory", server, is_default=True)
    async with server.connection():
        await server.publish(
            channel="first",
            message=MessageData(payload=b"{}", headers={"topic": "first"}),
        )
        await server.publish(
            channel="second",
            message=MessageData(payload=b"{}", headers={"topic": "second"}),
        )
        worker = asyncio.create_task(
            app.run_worker(
                messages_limit=1,
                limits=MessageLimits(max_messages=1),
                register_signals=[],
            ),
        )
        await asyncio.wait_for(first_started.wait(), timeout=1)
        release_first.set()
        assert (await asyncio.wait_for(worker, timeout=1)).processed == 1

    assert not second_started


async def test_run_worker_tasks_limit_is_explicit_deprecated_alias() -> None:
    app = Repid()
    app.servers.register_server("memory", InMemoryServer(), is_default=True)

    with pytest.warns(DeprecationWarning, match="tasks_limit"):
        assert (await app.run_worker(tasks_limit=1, register_signals=[])).processed == 0

    with (
        pytest.warns(DeprecationWarning, match="tasks_limit"),
        pytest.raises(ValueError, match="either"),
    ):
        await app.run_worker(limits=MessageLimits(), tasks_limit=1, register_signals=[])


async def test_run_worker_reports_unavailable_execution_backpressure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class NoPauseServer(InMemoryServer):
        @property
        def capabilities(self) -> CapabilitiesT:
            return {
                "supports_native_reply": True,
                "supports_lightweight_pause": False,
                "supports_channel_pause": False,
                "supports_keep_alive": False,
            }

    app = Repid()
    router = Router(limits=ActorLimits(max_messages=1))
    entered = asyncio.Event()
    release = asyncio.Event()

    @router.actor
    async def blocked() -> None:
        entered.set()
        await release.wait()

    app.include_router(router)
    server = NoPauseServer()
    app.servers.register_server("memory", server, is_default=True)
    async with server.connection():
        worker = asyncio.create_task(
            app.run_worker(
                messages_limit=2,
                register_signals=[],
                actor_limits_propagation="off",
            ),
        )
        for _ in range(2):
            await server.publish(
                channel="default",
                message=MessageData(payload=b"{}", headers={"topic": "blocked"}),
            )
        await asyncio.wait_for(entered.wait(), timeout=1)
        await asyncio.sleep(0.01)
        release.set()
        await worker

    assert [record.message for record in caplog.records].count(
        "runner.execution_backpressure.unavailable",
    ) == 1


async def test_run_worker_actor_policy_sees_actor_data() -> None:
    events: list[str] = []

    class Lease:
        async def release(self) -> None:
            events.append("release")

    class LimitPolicy:
        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: Any,
            on_wait: Callable[[], Awaitable[None]],  # noqa: ARG002
        ) -> ReservationLeaseT:
            events.extend((f"estimate:{actor.name}", "reserve"))
            return Lease()

    app = Repid()
    router = Router()

    @router.actor(limit_policies=(LimitPolicy(),))
    async def seen_actor() -> None:
        events.append("actor")

    app.include_router(router)
    await _run_one(app, InMemoryServer(), topic="seen_actor")

    assert events == ["estimate:seen_actor", "reserve", "actor", "release"]


async def test_run_worker_nacks_at_fetch_when_shrunken_cap_meets_strict_action() -> None:
    app = Repid()
    ran = 0
    router = Router()

    @router.actor(
        channel=Channel(
            address="jobs",
            limits=MessageLimits(max_payload_bytes=100, oversized_payload_action="nack"),
        ),
        limits=ActorLimits(max_payload_bytes=10),
    )
    async def shrunk() -> None:
        nonlocal ran
        ran += 1

    app.include_router(router)
    server = InMemoryServer()
    app.servers.register_server("memory", server, is_default=True)
    async with server.connection():
        await server.publish(
            channel="jobs",
            message=MessageData(payload=b"x" * 50, headers={"topic": "shrunk"}),
        )
        await server.publish(
            channel="jobs",
            message=MessageData(payload=b"ok", headers={"topic": "shrunk"}),
        )
        result = await asyncio.wait_for(
            app.run_worker(messages_limit=1, register_signals=[]),
            timeout=1,
        )

        assert result.processed == 1
        assert server.queues["jobs"].queue.empty()

    assert ran == 1
