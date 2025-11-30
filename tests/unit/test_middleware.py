from __future__ import annotations

from collections.abc import Callable, Coroutine
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Any, TypeVar

import pytest

from repid import ActorData, MessageData, Repid, Router
from repid.connections.abc import ReceivedMessageT
from repid.middlewares import (
    _compile_actor_middleware_pipeline,
    _compile_producer_middleware_pipeline,
)

T = TypeVar("T")


async def test_actor_middleware_pipeline_order() -> None:
    calls: list[str] = []

    async def middleware_1(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        calls.append("enter1")
        r = await call_next(message, actor)
        calls.append("exit1")
        return r

    async def middleware_2(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        calls.append("enter2")
        r = await call_next(message, actor)
        calls.append("exit2")
        return r

    async def actor_handler_mock(message: ReceivedMessageT, actor: ActorData) -> str:  # noqa: ARG001
        calls.append("leaf")
        return "ok"

    pipeline_factory = _compile_actor_middleware_pipeline([middleware_1, middleware_2])
    pipeline = pipeline_factory(actor_handler_mock)

    # fake pipeline inputs as we don't use them
    res = await pipeline(None, None)  # type: ignore[arg-type]
    assert res == "ok"
    assert calls == ["enter1", "enter2", "leaf", "exit2", "exit1"]


async def test_producer_middleware_pipeline_order() -> None:
    calls: list[str] = []

    async def producer_middleware_1(
        call_next: Callable[[str, MessageData, dict[str, Any] | None], Coroutine[None, None, T]],
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None,
    ) -> T:
        calls.append("enter1")
        r = await call_next(channel, message, server_specific_parameters)
        calls.append("exit1")
        return r

    async def producer_middleware_2(
        call_next: Callable[[str, MessageData, dict[str, Any] | None], Coroutine[None, None, T]],
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None,
    ) -> T:
        calls.append("enter2")
        r = await call_next(channel, message, server_specific_parameters)
        calls.append("exit2")
        return r

    async def producer_handler_mock(
        channel: str,  # noqa: ARG001
        message: MessageData,  # noqa: ARG001
        server_specific_parameters: dict[str, Any] | None,  # noqa: ARG001
    ) -> str:
        calls.append("leaf")
        return "ok"

    pipeline_factory = _compile_producer_middleware_pipeline(
        [producer_middleware_1, producer_middleware_2],
    )
    pipeline = pipeline_factory(producer_handler_mock)

    # fake pipeline inputs as we don't use them
    res = await pipeline("default", MessageData(payload=b""), None)
    assert res == "ok"
    assert calls == ["enter1", "enter2", "leaf", "exit2", "exit1"]


async def test_actor_middleware_on_actor_level() -> None:
    calls: list[str] = []

    async def middleware(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        calls.append("enter")
        r = await call_next(message, actor)
        calls.append("exit")
        return r

    router = Router()

    @router.actor(middlewares=[middleware])
    async def actor(message: Any, actor: Any) -> str:  # noqa: ARG001
        calls.append("actor")
        return "ok"

    # get the compiled pipeline
    pipeline = router.actors[0].middleware_pipeline

    async def mock_leaf(message: ReceivedMessageT, actor: ActorData) -> str:
        return await actor.fn(message, actor)

    res = await pipeline(mock_leaf, None, router.actors[0])  # type: ignore[arg-type]
    assert res == "ok"
    assert calls == ["enter", "actor", "exit"]


async def test_actor_middleware_on_router_level() -> None:
    calls: list[str] = []

    async def middleware(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        calls.append("enter")
        r = await call_next(message, actor)
        calls.append("exit")
        return r

    router = Router(middlewares=[middleware])

    @router.actor
    async def actor(message: Any, actor: Any) -> str:  # noqa: ARG001
        calls.append("actor")
        return "ok"

    pipeline = router.actors[0].middleware_pipeline

    async def mock_leaf(message: ReceivedMessageT, actor: ActorData) -> str:
        return await actor.fn(message, actor)

    res = await pipeline(mock_leaf, None, router.actors[0])  # type: ignore[arg-type]
    assert res == "ok"
    assert calls == ["enter", "actor", "exit"]


async def test_actor_middleware_on_repid_level() -> None:
    calls: list[str] = []

    async def middleware(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        calls.append("enter")
        r = await call_next(message, actor)
        calls.append("exit")
        return r

    repid = Repid(actor_middlewares=[middleware])
    router = Router()

    @router.actor
    async def actor(message: Any, actor: Any) -> str:  # noqa: ARG001
        calls.append("actor")
        return "ok"

    repid.include_router(router)

    # Repid delegates to _centralized_router
    pipeline = repid._centralized_router.actors[0].middleware_pipeline

    async def mock_leaf(message: ReceivedMessageT, actor: ActorData) -> str:
        return await actor.fn(message, actor)

    res = await pipeline(mock_leaf, None, repid._centralized_router.actors[0])  # type: ignore[arg-type]
    assert res == "ok"
    assert calls == ["enter", "actor", "exit"]


async def test_producer_middleware_on_repid_level() -> None:
    calls: list[str] = []

    async def middleware(
        call_next: Callable[[str, MessageData, dict[str, Any] | None], Coroutine[None, None, T]],
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None,
    ) -> T:
        calls.append("enter")
        r = await call_next(channel, message, server_specific_parameters)
        calls.append("exit")
        return r

    repid = Repid(producer_middlewares=[middleware])

    pipeline = repid._producer_middleware_pipeline

    async def mock_leaf(
        channel: str,  # noqa: ARG001
        message: MessageData,  # noqa: ARG001
        server_specific_parameters: dict[str, Any] | None,  # noqa: ARG001
    ) -> str:
        calls.append("leaf")
        return "ok"

    pipeline_fn = pipeline(mock_leaf)
    res = await pipeline_fn("default", MessageData(payload=b""), None)
    assert res == "ok"
    assert calls == ["enter", "leaf", "exit"]


async def test_actor_middleware_combination() -> None:
    calls: list[str] = []

    def create_middleware(name: str) -> Callable:
        async def middleware(
            call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
            message: ReceivedMessageT,
            actor: ActorData,
        ) -> T:
            calls.append(f"enter_{name}")
            r = await call_next(message, actor)
            calls.append(f"exit_{name}")
            return r

        return middleware

    mw1 = create_middleware("actor")
    mw2 = create_middleware("router")
    mw3 = create_middleware("repid")

    repid = Repid(actor_middlewares=[mw3])
    router = Router(middlewares=[mw2])

    @router.actor(middlewares=[mw1])
    async def actor(message: Any, actor: Any) -> str:  # noqa: ARG001
        calls.append("leaf")
        return "ok"

    repid.include_router(router)

    pipeline = repid._centralized_router.actors[0].middleware_pipeline

    async def mock_leaf(message: ReceivedMessageT, actor: ActorData) -> str:
        return await actor.fn(message, actor)

    res = await pipeline(mock_leaf, None, repid._centralized_router.actors[0])  # type: ignore[arg-type]
    assert res == "ok"
    assert calls == [
        "enter_repid",
        "enter_router",
        "enter_actor",
        "leaf",
        "exit_actor",
        "exit_router",
        "exit_repid",
    ]


async def test_actor_middleware_contextvar() -> None:
    ctx_var: ContextVar[str] = ContextVar("ctx_var", default="empty")
    calls: list[str] = []

    async def middleware_outer(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        token = ctx_var.set("outer")
        calls.append(f"outer_enter:{ctx_var.get()}")
        try:
            return await call_next(message, actor)
        finally:
            calls.append(f"outer_exit:{ctx_var.get()}")
            ctx_var.reset(token)

    async def middleware_inner(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        calls.append(f"inner_enter:{ctx_var.get()}")
        r = await call_next(message, actor)
        calls.append(f"inner_exit:{ctx_var.get()}")
        return r

    router = Router(middlewares=[middleware_outer])

    @router.actor(middlewares=[middleware_inner])
    async def actor(message: Any, actor: Any) -> str:  # noqa: ARG001
        calls.append(f"leaf:{ctx_var.get()}")
        return "ok"

    pipeline = router.actors[0].middleware_pipeline

    async def mock_leaf(message: ReceivedMessageT, actor: ActorData) -> str:
        return await actor.fn(message, actor)

    res = await pipeline(mock_leaf, None, router.actors[0])  # type: ignore[arg-type]
    assert res == "ok"
    assert calls == [
        "outer_enter:outer",
        "inner_enter:outer",
        "leaf:outer",
        "inner_exit:outer",
        "outer_exit:outer",
    ]
    assert ctx_var.get() == "empty"


async def test_actor_middleware_error_handling() -> None:
    calls: list[str] = []

    async def middleware(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        calls.append("enter")
        try:
            return await call_next(message, actor)
        except ValueError:
            calls.append("caught")
            raise
        finally:
            calls.append("exit")

    router = Router()

    @router.actor(middlewares=[middleware])
    async def actor(message: Any, actor: Any) -> str:  # noqa: ARG001
        calls.append("actor")
        raise ValueError("oops")

    pipeline = router.actors[0].middleware_pipeline

    async def mock_leaf(message: ReceivedMessageT, actor: ActorData) -> str:
        return await actor.fn(message, actor)

    with pytest.raises(ValueError, match="oops"):
        await pipeline(mock_leaf, None, router.actors[0])  # type: ignore[arg-type]

    assert calls == ["enter", "actor", "caught", "exit"]


async def test_producer_middleware_error_handling() -> None:
    calls: list[str] = []

    async def middleware(
        call_next: Callable[[str, MessageData, dict[str, Any] | None], Coroutine[None, None, T]],
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None,
    ) -> T:
        calls.append("enter")
        try:
            return await call_next(channel, message, server_specific_parameters)
        except ValueError:
            calls.append("caught")
            raise
        finally:
            calls.append("exit")

    repid = Repid(producer_middlewares=[middleware])

    pipeline = repid._producer_middleware_pipeline

    async def mock_leaf(
        channel: str,  # noqa: ARG001
        message: MessageData,  # noqa: ARG001
        server_specific_parameters: dict[str, Any] | None,  # noqa: ARG001
    ) -> str:
        calls.append("leaf")
        raise ValueError("oops")

    pipeline_fn = pipeline(mock_leaf)
    with pytest.raises(ValueError, match="oops"):
        await pipeline_fn("default", MessageData(payload=b""), None)

    assert calls == ["enter", "leaf", "caught", "exit"]


async def test_actor_middleware_mutation() -> None:
    @dataclass
    class MockMessage:
        payload: bytes

    async def middleware(
        call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[None, None, T]],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> T:
        message.payload = b"mutated"  # type: ignore[misc]
        return await call_next(message, actor)

    async def actor_handler_mock(message: ReceivedMessageT, actor: ActorData) -> str:  # noqa: ARG001
        return message.payload.decode()  # test that leaf sees mutated payload

    pipeline_factory = _compile_actor_middleware_pipeline([middleware])
    pipeline = pipeline_factory(actor_handler_mock)

    res = await pipeline(MockMessage(payload=b"original"), None)  # type: ignore[arg-type]
    assert res == "mutated"


async def test_producer_middleware_mutation() -> None:
    async def middleware(
        call_next: Callable[[str, MessageData, dict[str, Any] | None], Coroutine[None, None, T]],
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None,
    ) -> T:
        new_message = replace(message, payload=b"mutated")
        return await call_next(channel, new_message, server_specific_parameters)

    repid = Repid(producer_middlewares=[middleware])

    pipeline = repid._producer_middleware_pipeline

    async def mock_leaf(
        channel: str,  # noqa: ARG001
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None,  # noqa: ARG001
    ) -> str:
        return message.payload.decode()  # test that leaf sees mutated payload

    pipeline_fn = pipeline(mock_leaf)
    res = await pipeline_fn("default", MessageData(payload=b"original"), None)
    assert res == "mutated"
