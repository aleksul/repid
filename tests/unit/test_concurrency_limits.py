from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any, Literal

import pytest

from repid import (
    ActorLimits,
    Channel,
    IntakeControl,
    MessageCountIntake,
    MessageLimits,
    PayloadByteIntake,
    Repid,
    Router,
)
from repid._runner import _Runner
from repid.connections.abc import CapabilitiesT, ReceivedMessageT, SubscriberT
from repid.connections.in_memory import InMemoryServer
from repid.data import ActorData, MessageData
from repid.limits import UNLIMITED_NATIVE_FLOW, NativeFlow


class RecordingServer(InMemoryServer):
    def __init__(self, *, native: bool = True, pause: bool = True) -> None:
        super().__init__()
        self.native = native
        self.can_pause = pause
        self.messages: list[ReceivedMessageT] = []
        self.flows: list[NativeFlow] = []
        self.subscriptions: list[SubscriberT] = []
        self.pauses: list[str | None] = []
        self.resumes: list[str | None] = []
        self.paused = asyncio.Event()
        self.replaced = asyncio.Event()
        self.two_delivered = asyncio.Event()
        self.three_delivered = asyncio.Event()
        self.subscribed = asyncio.Event()

    @property
    def capabilities(self) -> CapabilitiesT:
        result = super().capabilities
        for key in result:
            if "native" in key or "oversized" in key:
                result[key] = self.native  # type: ignore[literal-required]
        result["supports_channel_pause"] = self.can_pause
        result["supports_worker_pause"] = self.can_pause
        return result

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
    ) -> SubscriberT:
        self.flows.append(native_flow)
        callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]] = {}
        for channel, callback in channels_to_callbacks.items():

            async def submit(
                message: ReceivedMessageT,
                callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]] = callback,
            ) -> None:
                self.messages.append(message)
                if len(self.messages) >= 2:
                    self.two_delivered.set()
                if len(self.messages) >= 3:
                    self.three_delivered.set()
                await callback(message)

            callbacks[channel] = submit
        subscriber = await super().subscribe(
            channels_to_callbacks=callbacks,
            native_flow=native_flow,
        )
        pause, resume = subscriber.pause, subscriber.resume

        async def recorded_pause(channel: str | None = None) -> None:
            self.pauses.append(channel)
            self.paused.set()
            await pause(channel)

        async def recorded_resume(channel: str | None = None) -> None:
            self.resumes.append(channel)
            await resume(channel)

        subscriber.pause = recorded_pause  # type: ignore[method-assign]
        subscriber.resume = recorded_resume  # type: ignore[method-assign]
        self.subscriptions.append(subscriber)
        self.subscribed.set()
        if len(self.subscriptions) > 1:
            self.replaced.set()
        return subscriber


def application(server: InMemoryServer, router: Router) -> Repid:
    app = Repid()
    app.servers.register_server("default", server)
    app.include_router(router)
    return app


async def publish(
    server: InMemoryServer,
    actor: str = "job",
    *,
    channel: str = "default",
    size: int = 4,
) -> None:
    await server.publish(
        channel=channel,
        message=MessageData(
            payload=b"null" + b" " * (size - 4),
            headers={"topic": actor},
            content_type="application/json",
        ),
    )


@pytest.mark.parametrize("scope", ["worker", "channel", "router", "actor"])
async def test_each_scope_bounds_execution(scope: str) -> None:
    server = RecordingServer(native=False)
    ready, release = asyncio.Event(), asyncio.Event()
    active = maximum = 0
    router = Router(limits=ActorLimits(max_messages=2) if scope == "router" else None)
    channel = Channel(
        address="default",
        limits=MessageLimits(max_messages=2) if scope == "channel" else None,
    )

    @router.actor(channel=channel, limits=ActorLimits(max_messages=2) if scope == "actor" else None)
    async def job() -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        if active == 2:
            ready.set()
        await release.wait()
        active -= 1

    app = application(server, router)
    async with server.connection():
        for _ in range(6):
            await publish(server)
        work = asyncio.create_task(
            app.run_worker(
                messages_limit=6,
                limits=MessageLimits(max_messages=2) if scope == "worker" else MessageLimits(),
                register_signals=[],
            ),
        )
        await asyncio.wait_for(ready.wait(), 2)
        release.set()
        result = await asyncio.wait_for(work, 2)
    assert result.processed == 6
    assert maximum == 2
    assert all(message.is_acted_on for message in server.messages)


async def test_default_empty_and_alias_conflict() -> None:
    server = RecordingServer()
    router = Router()

    @router.actor
    async def job() -> None:
        pass

    app = application(server, router)
    async with server.connection():
        await publish(server)
        await app.run_worker(messages_limit=1, register_signals=[])
        assert server.flows[-1].worker.max_messages == 1000
        await publish(server)
        await app.run_worker(messages_limit=1, limits=MessageLimits(), register_signals=[])
        assert server.flows[-1].worker.max_messages is None
        await publish(server)
        with pytest.warns(DeprecationWarning, match="tasks_limit is deprecated"):
            await app.run_worker(messages_limit=1, tasks_limit=2, register_signals=[])
        assert server.flows[-1].worker.max_messages == 2
        with pytest.raises(ValueError, match=r"either tasks_limit or limits"):
            await app.run_worker(tasks_limit=2, limits=MessageLimits(max_messages=2))


@pytest.mark.parametrize("field", ["max_messages", "max_payload_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_invalid_numeric_limits(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=r"must|less"):
        MessageLimits(**{field: value})


@pytest.mark.parametrize(
    "kwargs",
    [
        {"pause_at": 0},
        {"pause_at": 1, "resume_at": 1},
        {"resume_at": -1},
        {"native": False},
        {"native": "batch"},
    ],
)
def test_invalid_intake(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r"must|less"):
        MessageCountIntake(**kwargs)


def test_byte_settings_cannot_be_used_for_counts() -> None:
    with pytest.raises(TypeError):
        IntakeControl(messages=PayloadByteIntake())


async def test_nested_shared_and_independent_execution_pools() -> None:
    server = RecordingServer(native=False)
    shared = ActorLimits(max_messages=1)
    root = Router(limits=shared)
    nested = Router(limits=shared)

    @nested.actor(limits=shared)
    async def job() -> None:
        pass

    root.include_router(nested)
    app = application(server, root)
    async with server.connection():
        await publish(server)
        result = await asyncio.wait_for(app.run_worker(messages_limit=1, register_signals=[]), 2)
    assert result.processed == 1
    actor = root.actors[0]
    assert all(pool is shared for pool in actor.execution_limits)


async def test_oversized_intake_does_not_count_itself() -> None:
    server = RecordingServer(native=False)
    router = Router()
    called = []

    @router.actor
    async def job() -> None:
        called.append(1)

    app = application(server, router)
    async with server.connection():
        await publish(server, size=150)
        result = await asyncio.wait_for(
            app.run_worker(
                messages_limit=1,
                limits=MessageLimits(max_payload_bytes=100),
                register_signals=[],
            ),
            2,
        )
    assert result.processed == 1
    assert len(called) == 1


@pytest.mark.parametrize("action", ["reject", "nack", "invalid", "raises", "coroutine"])
async def test_oversized_terminal_actions_and_bad_callbacks(
    action: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = RecordingServer(native=False)
    router = Router()

    @router.actor(limits=ActorLimits(max_payload_bytes=10, on_oversized_payload="reject"))
    async def job() -> None:
        pytest.fail("Oversized message must not execute")

    def callback(message: ReceivedMessageT) -> Any:
        del message
        if action == "raises":
            raise RuntimeError("sensitive payload content")
        if action == "coroutine":

            async def invalid() -> str:
                return "run_alone"

            return invalid()
        return action

    app = application(server, router)
    async with server.connection():
        await publish(server, size=20)
        # One submission: avoid intentionally requeued reject messages looping forever.
        captured = asyncio.Event()
        original = server.subscribe

        async def subscribe(**kwargs: Any) -> SubscriberT:
            original_callback = kwargs["channels_to_callbacks"]["default"]

            async def once(message: ReceivedMessageT) -> None:
                await original_callback(message)
                captured.set()
                await asyncio.Event().wait()

            kwargs["channels_to_callbacks"]["default"] = once
            return await original(**kwargs)

        server.subscribe = subscribe  # type: ignore[method-assign]
        work = asyncio.create_task(
            app.run_worker(
                limits=MessageLimits(max_payload_bytes=10, on_oversized_payload=callback),
                register_signals=[],
                graceful_shutdown_time=0,
            ),
        )
        await asyncio.wait_for(captured.wait(), 2)
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await work
    expected = "rejected" if action == "reject" else "nacked"
    assert server.messages[0].action == expected
    assert "sensitive payload content" not in caplog.text


async def test_nominal_byte_propagation_can_reduce_oversized_concurrency() -> None:
    server = RecordingServer(native=False)
    router = Router()
    active = maximum = 0
    release, started = asyncio.Event(), asyncio.Event()

    async def execute() -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(active, maximum)
        started.set()
        await release.wait()
        active -= 1

    router.actor(execute, name="first", limits=ActorLimits(max_payload_bytes=100))
    router.actor(execute, name="second", limits=ActorLimits(max_payload_bytes=100))
    app = application(server, router)
    async with server.connection():
        await publish(server, "first", size=150)
        await publish(server, "second", size=150)
        work = asyncio.create_task(
            app.run_worker(messages_limit=2, limits=MessageLimits(), register_signals=[]),
        )
        await asyncio.wait_for(started.wait(), 2)
        release.set()
        await asyncio.wait_for(work, 2)
    assert maximum == 1
    assert server.flows[0].channels["default"].max_payload_bytes is None


async def test_explicit_native_unsupported_fails_before_subscription() -> None:
    server = RecordingServer(native=False)
    router = Router()

    @router.actor
    async def job() -> None:
        pass

    async with server.connection():
        with pytest.raises(ValueError, match="Explicit native"):
            await application(server, router).run_worker(
                intake_control=IntakeControl(messages=MessageCountIntake(native=2)),
                register_signals=[],
            )
    assert not server.subscriptions


async def test_worker_window_does_not_become_channel_absolute_window() -> None:
    server = RecordingServer()
    router = Router()

    @router.actor(channel=Channel(address="one", limits=MessageLimits(max_messages=2)))
    async def job() -> None:
        pass

    @router.actor(channel=Channel(address="two", limits=MessageLimits(max_messages=3)))
    async def other() -> None:
        pass

    async with server.connection():
        await publish(server, channel="one")
        await application(server, router).run_worker(
            messages_limit=1,
            intake_control=IntakeControl(messages=MessageCountIntake(native=7)),
            register_signals=[],
        )
    assert server.flows[0].worker.max_messages == 7
    assert server.flows[0].channels["one"].max_messages == 2
    assert server.flows[0].channels["two"].max_messages == 3


async def test_channel_collision_is_checked_after_inheritance() -> None:
    server = RecordingServer()
    router = Router()
    router.actor(
        lambda: None,
        name="one",
        channel=Channel(address="jobs", limits=MessageLimits(max_messages=2)),
    )
    router.actor(
        lambda: None,
        name="two",
        channel=Channel(address="jobs", limits=MessageLimits(max_messages=3)),
    )
    async with server.connection():
        with pytest.raises(ValueError, match="Conflicting"):
            await application(server, router).run_worker(register_signals=[])
    assert not server.subscriptions


class BlockingPolicy:
    def __init__(self) -> None:
        self.waiting = asyncio.Event()
        self.release = asyncio.Event()
        self.entries = self.exits = self.notifications = 0

    @asynccontextmanager
    async def reserve(
        self,
        message: ReceivedMessageT,
        actor: ActorData,
        on_capacity_exhausted: Callable[[], Awaitable[None]],
    ) -> AsyncIterator[None]:
        del message, actor
        self.entries += 1
        await on_capacity_exhausted()
        await on_capacity_exhausted()
        self.waiting.set()
        await self.release.wait()
        try:
            yield
        finally:
            self.exits += 1


async def test_shutdown_rejects_admitted_policy_wait_without_starting_actor() -> None:
    server = RecordingServer(native=False)
    policy = BlockingPolicy()
    router = Router(limit_policies=(policy,))
    calls = []

    @router.actor(limit_policies=(policy,))
    async def job() -> None:
        calls.append(1)

    app = application(server, router)
    async with server.connection():
        await publish(server)
        work = asyncio.create_task(
            app.run_worker(limit_policies=(policy,), register_signals=[], graceful_shutdown_time=0),
        )
        await asyncio.wait_for(policy.waiting.wait(), 2)
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(work, 2)
    assert not calls
    assert policy.entries == 1
    assert server.messages[0].action == "rejected"


async def test_resubscribe_waits_for_policy_and_processing_drain() -> None:
    server = RecordingServer(native=False, pause=False)
    policy = BlockingPolicy()
    router = Router(limit_policies=(policy,))
    done = asyncio.Event()

    @router.actor
    async def job() -> None:
        done.set()

    app = application(server, router)
    async with server.connection():
        await publish(server)
        work = asyncio.create_task(
            app.run_worker(
                limits=MessageLimits(),
                intake_control=IntakeControl(pause_strategies=("resubscribe",)),
                register_signals=[],
                graceful_shutdown_time=0,
            ),
        )
        await asyncio.wait_for(policy.waiting.wait(), 2)
        assert len(server.subscriptions) == 1
        policy.release.set()
        await asyncio.wait_for(done.wait(), 2)
        await asyncio.wait_for(server.replaced.wait(), 2)
        assert server.messages[0].action == "acked"
        assert policy.exits == 1
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(work, 2)


async def test_shutdown_interrupts_resubscribe_drain() -> None:
    server = RecordingServer(native=False, pause=False)
    policy = BlockingPolicy()
    router = Router(limit_policies=(policy,))

    @router.actor
    async def job() -> None:
        pytest.fail("Actor must not start")

    async with server.connection():
        await publish(server)
        work = asyncio.create_task(
            application(server, router).run_worker(
                limits=MessageLimits(),
                intake_control=IntakeControl(pause_strategies=("resubscribe",)),
                register_signals=[],
                graceful_shutdown_time=0,
            ),
        )
        await asyncio.wait_for(policy.waiting.wait(), 2)
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(work, 2)
    assert len(server.subscriptions) == 1
    assert server.messages[0].action == "rejected"


@pytest.mark.parametrize("propagation", ["auto", "off"])
async def test_nominal_byte_budget_and_opt_out(propagation: Literal["auto", "off"]) -> None:
    server = RecordingServer()
    router = Router()
    router.actor(lambda: None, name="job", limits=ActorLimits(max_payload_bytes=100))
    router.actor(lambda: None, name="other", limits=ActorLimits(max_payload_bytes=100))
    async with server.connection():
        await publish(server, size=150)
        result = await asyncio.wait_for(
            application(server, router).run_worker(
                messages_limit=1,
                limits=MessageLimits(),
                actor_limits_propagation=propagation,
                register_signals=[],
            ),
            2,
        )
    assert result.processed == 1
    assert server.flows[0].channels["default"].max_payload_bytes == (
        200 if propagation == "auto" else None
    )


async def test_exact_quota_with_backlog() -> None:
    server = RecordingServer(native=False)
    router = Router()
    calls = []

    @router.actor(limits=ActorLimits(max_messages=2))
    async def job() -> None:
        calls.append(1)

    async with server.connection():
        for _ in range(25):
            await publish(server)
        result = await asyncio.wait_for(
            application(server, router).run_worker(
                messages_limit=7,
                limits=MessageLimits(max_messages=20),
                register_signals=[],
            ),
            2,
        )
    assert result.processed == 7
    assert len(calls) == 7
    assert sum(message.action == "acked" for message in server.messages) == 7


async def test_delivered_overshoot_does_not_block_reserved_admission() -> None:
    server = RecordingServer(native=False, pause=False)
    router = Router()
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def job() -> None:
        calls.append(1)
        started.set()
        await release.wait()

    for channel in ("one", "two", "three"):
        router.actor(job, name="job", channel=Channel(address=channel))
    async with server.connection():
        for channel in ("one", "two", "three"):
            await publish(server, channel=channel)
        work = asyncio.create_task(
            application(server, router).run_worker(
                messages_limit=3,
                limits=MessageLimits(max_messages=1),
                register_signals=[],
            ),
        )
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.wait_for(server.two_delivered.wait(), 2)
        assert len(server.messages) >= 2
        release.set()
        result = await asyncio.wait_for(work, 2)
    assert len(calls) == 3
    assert result.processed == 3


async def test_early_ack_holds_execution_until_middleware_unwinds() -> None:
    server = RecordingServer(native=False)
    router = Router()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    @router.actor(limits=ActorLimits(max_messages=1), confirmation_mode="ack_first")
    async def job() -> None:
        calls.append(1)
        entered.set()
        await release.wait()

    async with server.connection():
        await publish(server)
        await publish(server)
        work = asyncio.create_task(
            application(server, router).run_worker(
                messages_limit=2,
                actor_limits_propagation="off",
                limits=MessageLimits(),
                register_signals=[],
            ),
        )
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.wait_for(server.paused.wait(), 2)
        assert server.messages[0].action == "acked"
        assert len(calls) == 1
        release.set()
        await asyncio.wait_for(work, 2)
    assert len(calls) == 2


@pytest.mark.parametrize("failure_stage", ["enter", "exit"])
async def test_policy_failure_unwinds_prior_contexts_and_fails_worker(failure_stage: str) -> None:
    server = RecordingServer(native=False)
    events = []

    class Policy:
        def __init__(self, name: str) -> None:
            self.name = name

        @asynccontextmanager
        async def reserve(
            self,
            message: ReceivedMessageT,
            actor: ActorData,
            on_capacity_exhausted: Callable[[], Awaitable[None]],
        ) -> AsyncIterator[None]:
            del message, actor, on_capacity_exhausted
            events.append(f"enter:{self.name}")
            if self.name == "second" and failure_stage == "enter":
                raise RuntimeError("reservation failed")
            try:
                yield
            finally:
                events.append(f"exit:{self.name}")
                if self.name == "second" and failure_stage == "exit":
                    raise RuntimeError("reservation failed")

    first, second = Policy("first"), Policy("second")
    router = Router(limit_policies=(first, second))
    router.actor(lambda: None, name="job")
    async with server.connection():
        await publish(server)
        with pytest.raises(RuntimeError, match="reservation failed"):
            await asyncio.wait_for(
                application(server, router).run_worker(messages_limit=1, register_signals=[]),
                2,
            )
    assert events[-1] == "exit:first"
    assert events[:2] == ["enter:first", "enter:second"]
    assert server.messages[0].is_acted_on


async def test_shared_pool_identity_across_channels_and_equal_distinct_pools() -> None:
    server = RecordingServer(native=False)
    shared = ActorLimits(max_messages=1)
    router = Router()
    active = maximum = 0
    both, release = asyncio.Event(), asyncio.Event()

    async def execute() -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        if active == 2:
            both.set()
        await release.wait()
        active -= 1

    router.actor(execute, name="job", channel=Channel(address="one"), limits=shared)
    router.actor(execute, name="job", channel=Channel(address="two"), limits=shared)
    router.actor(
        execute,
        name="job",
        channel=Channel(address="three"),
        limits=ActorLimits(max_messages=1),
    )
    async with server.connection():
        for channel in ("one", "two", "three"):
            await publish(server, channel=channel)
        work = asyncio.create_task(
            application(server, router).run_worker(
                messages_limit=3,
                limits=MessageLimits(),
                register_signals=[],
            ),
        )
        await asyncio.wait_for(both.wait(), 2)
        assert maximum == 2
        release.set()
        await asyncio.wait_for(work, 2)
    assert maximum == 2


@pytest.mark.parametrize("mode", ["deliver", "allow_blocked"])
async def test_native_bytes_smaller_than_local_budget(
    mode: Literal["deliver", "allow_blocked"],
) -> None:
    server = RecordingServer()
    router = Router()
    called = asyncio.Event()

    @router.actor
    async def job() -> None:
        called.set()

    async with server.connection():
        await publish(server, size=150)
        work = asyncio.create_task(
            application(server, router).run_worker(
                messages_limit=1,
                limits=MessageLimits(max_payload_bytes=200),
                intake_control=IntakeControl(
                    payload_bytes=PayloadByteIntake(native=100, oversized_delivery=mode),
                ),
                register_signals=[],
            ),
        )
        await asyncio.wait_for(server.subscribed.wait(), 2)
        if mode == "deliver":
            await asyncio.wait_for(work, 2)
            assert called.is_set()
        else:
            # Let the bounded delivery loop reach its native gate.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert not server.messages
            assert not called.is_set()
            work.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(work, 2)
    assert server.flows[0].worker.max_payload_bytes == 100
    assert server.flows[0].worker.oversized_delivery == mode


async def test_oversized_priority_does_not_starve_on_later_small_deliveries() -> None:
    server = RecordingServer(native=False, pause=False)
    router = Router()
    first_started, big_started = asyncio.Event(), asyncio.Event()
    release_first, release_big = asyncio.Event(), asyncio.Event()
    order = []

    @router.actor(channel=Channel(address="first"))
    async def first() -> None:
        order.append("first")
        first_started.set()
        await release_first.wait()

    @router.actor(channel=Channel(address="big"))
    async def big() -> None:
        order.append("big")
        big_started.set()
        await release_big.wait()

    @router.actor(channel=Channel(address="small"))
    async def small() -> None:
        order.append("small")

    async with server.connection():
        await publish(server, "first", channel="first", size=10)
        work = asyncio.create_task(
            application(server, router).run_worker(
                messages_limit=3,
                limits=MessageLimits(max_payload_bytes=30),
                register_signals=[],
            ),
        )
        await asyncio.wait_for(first_started.wait(), 2)
        await publish(server, "big", channel="big", size=40)
        await asyncio.wait_for(server.two_delivered.wait(), 2)
        await publish(server, "small", channel="small", size=4)
        await asyncio.wait_for(server.three_delivered.wait(), 2)
        release_first.set()
        await asyncio.wait_for(big_started.wait(), 2)
        assert order == ["first", "big"]
        release_big.set()
        await asyncio.wait_for(work, 2)
    assert order == ["first", "big", "small"]


async def test_opposing_policy_paths_use_one_global_acquisition_order() -> None:
    server = RecordingServer(native=False, pause=False)
    events = []

    class Policy:
        def __init__(self, name: str) -> None:
            self.lock = asyncio.Lock()
            self.name = name

        @asynccontextmanager
        async def reserve(
            self,
            message: ReceivedMessageT,
            actor: ActorData,
            on_capacity_exhausted: Callable[[], Awaitable[None]],
        ) -> AsyncIterator[None]:
            del message
            if self.lock.locked():
                await on_capacity_exhausted()
            async with self.lock:
                events.append((actor.name, self.name, "enter"))
                try:
                    await asyncio.sleep(0)
                    yield
                finally:
                    events.append((actor.name, self.name, "exit"))

    first, second = Policy("first"), Policy("second")
    router = Router()
    router.actor(
        lambda: None,
        name="one",
        channel=Channel(address="one"),
        limit_policies=(first, second),
    )
    router.actor(
        lambda: None,
        name="two",
        channel=Channel(address="two"),
        limit_policies=(second, first),
    )
    async with server.connection():
        await publish(server, "one", channel="one")
        await publish(server, "two", channel="two")
        result = await asyncio.wait_for(
            application(server, router).run_worker(messages_limit=2, register_signals=[]),
            2,
        )
    assert result.processed == 2
    for actor in ("one", "two"):
        assert [(policy, state) for name, policy, state in events if name == actor] == [
            ("first", "enter"),
            ("second", "enter"),
            ("second", "exit"),
            ("first", "exit"),
        ]


async def test_shutdown_rejects_waiting_intake_without_new_processing() -> None:
    server = RecordingServer(native=False, pause=False)
    router = Router()
    started = asyncio.Event()
    calls = []

    async def job() -> None:
        calls.append(1)
        started.set()
        await asyncio.Event().wait()

    for channel in ("one", "two"):
        router.actor(job, name="job", channel=Channel(address=channel))
    async with server.connection():
        await publish(server, channel="one")
        await publish(server, channel="two")
        work = asyncio.create_task(
            application(server, router).run_worker(
                limits=MessageLimits(max_messages=1),
                register_signals=[],
                graceful_shutdown_time=0,
            ),
        )
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.wait_for(server.two_delivered.wait(), 2)
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(work, 2)
    assert len(calls) == 1
    assert all(message.action == "rejected" for message in server.messages)


@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_runtime_intake_control_failures_stop_worker(operation: str) -> None:
    server = RecordingServer(native=False)
    router = Router()
    original = server.subscribe

    async def subscribe(**kwargs: Any) -> SubscriberT:
        subscriber = await original(**kwargs)

        async def fail(_channel: str | None = None) -> None:
            raise RuntimeError(f"{operation} failed")

        setattr(subscriber, operation, fail)
        return subscriber

    server.subscribe = subscribe  # type: ignore[method-assign]
    calls = []

    @router.actor
    async def job() -> None:
        calls.append(1)

    async with server.connection():
        await publish(server)
        await publish(server)
        with pytest.raises(RuntimeError, match=f"{operation} failed"):
            await asyncio.wait_for(
                application(server, router).run_worker(
                    limits=MessageLimits(max_messages=1),
                    messages_limit=2,
                    register_signals=[],
                ),
                2,
            )
    assert len(calls) == (0 if operation == "pause" else 1)
    assert all(message.is_acted_on for message in server.messages)


async def test_resubscription_failure_propagates_after_draining_old_work() -> None:
    server = RecordingServer(native=False, pause=False)
    original = server.subscribe

    async def subscribe(**kwargs: Any) -> SubscriberT:
        if server.subscriptions:
            raise RuntimeError("replacement failed")
        return await original(**kwargs)

    server.subscribe = subscribe  # type: ignore[method-assign]
    policy = BlockingPolicy()
    router = Router(limit_policies=(policy,))
    router.actor(lambda: None, name="job")
    async with server.connection():
        await publish(server)
        work = asyncio.create_task(
            application(server, router).run_worker(
                limits=MessageLimits(),
                register_signals=[],
                intake_control=IntakeControl(pause_strategies=("resubscribe",)),
            ),
        )
        await asyncio.wait_for(policy.waiting.wait(), 2)
        policy.release.set()
        with pytest.raises(RuntimeError, match="replacement failed"):
            await asyncio.wait_for(work, 2)
    assert server.messages[0].action == "acked"
    assert policy.exits == 1


async def test_strict_unavailable_intake_control_fails_before_subscription() -> None:
    server = RecordingServer(native=False, pause=False)
    router = Router()
    router.actor(lambda: None, name="job")
    async with server.connection():
        with pytest.raises(ValueError, match="No available pause strategy"):
            await application(server, router).run_worker(
                register_signals=[],
                intake_control=IntakeControl(on_unavailable="error"),
            )
    assert not server.subscriptions


async def test_oversized_disposal_does_not_consume_quota() -> None:
    server = RecordingServer(native=False)
    router = Router()
    calls = []
    router.actor(lambda: calls.append(1), name="job")
    async with server.connection():
        await publish(server, size=20)
        await publish(server, size=4)
        result = await asyncio.wait_for(
            application(server, router).run_worker(
                messages_limit=1,
                limits=MessageLimits(max_payload_bytes=10, on_oversized_payload="nack"),
                register_signals=[],
            ),
            2,
        )
    assert result.processed == 1
    assert len(calls) == 1
    assert [message.action for message in server.messages] == ["nacked", "acked"]


async def test_channel_policy_is_reserved_and_unwound() -> None:
    server = RecordingServer()
    policy = BlockingPolicy()
    policy.release.set()
    router = Router()
    router.actor(
        lambda: None,
        name="job",
        channel=Channel(address="default", limit_policies=(policy,)),
    )
    async with server.connection():
        await publish(server)
        result = await asyncio.wait_for(
            application(server, router).run_worker(messages_limit=1, register_signals=[]),
            2,
        )
    assert result.processed == 1
    assert policy.entries == 1
    assert policy.exits == 1


async def test_resume_failure_after_handoff_preserves_running_actor_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = RecordingServer(native=False)
    router = Router()
    started, release, completed, resume_failed = (asyncio.Event() for _ in range(4))
    failed = False
    reconcile = _Runner._reconcile

    async def fail_resume(runner: _Runner) -> None:
        nonlocal failed
        if runner._tasks and not failed:
            failed = True
            await started.wait()
            resume_failed.set()
            raise RuntimeError("resume failed")
        await reconcile(runner)

    monkeypatch.setattr(_Runner, "_reconcile", fail_resume)

    @router.actor()
    async def job() -> None:
        started.set()
        await release.wait()
        completed.set()

    async with server.connection():
        await publish(server)
        work = asyncio.create_task(
            application(server, router).run_worker(
                register_signals=[],
                graceful_shutdown_time=1,
            ),
        )
        try:
            await asyncio.wait_for(resume_failed.wait(), 2)
            assert started.is_set()
            assert not server.messages[0].is_acted_on
            release.set()
            with pytest.raises(RuntimeError, match="resume failed"):
                await asyncio.wait_for(work, 2)
        finally:
            release.set()
            if not work.done():
                work.cancel()
            await asyncio.gather(work, return_exceptions=True)
    assert completed.is_set()
    assert server.messages[0].action == "acked"
