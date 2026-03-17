from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data import ActorData, MessageData

ActorMiddlewareReturnsT = TypeVar("ActorMiddlewareReturnsT")
ProducerMiddlewareReturnsT = TypeVar("ProducerMiddlewareReturnsT")


class ActorMiddlewareT(Protocol):
    async def __call__(
        self,
        call_next: Callable[
            [ReceivedMessageT, ActorData],
            Coroutine[Any, Any, ActorMiddlewareReturnsT],
        ],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> ActorMiddlewareReturnsT: ...


class ProducerMiddlewareT(Protocol):
    async def __call__(
        self,
        call_next: Callable[
            [str, MessageData, dict[str, Any] | None],
            Coroutine[Any, Any, ProducerMiddlewareReturnsT],
        ],
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None,
    ) -> ProducerMiddlewareReturnsT: ...


YourFunc = TypeVar("YourFunc", bound=Callable)


class _ProducerMiddlewareLastLeaf(Protocol):
    async def __call__(
        self,
        *,
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None,
    ) -> Any: ...


def _compile_actor_middleware_pipeline(
    middlewares: Sequence[ActorMiddlewareT] | None,
) -> Callable[
    [Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]]],
    Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]],
]:
    """Compile middlewares into a call-next chain factory.

    Returns a function that, given a leaf (call_next), produces a coroutine function
    with signature (message, actor) -> Any that runs all middlewares around the leaf.
    """
    mws: list[ActorMiddlewareT] = list(middlewares or [])

    def assemble(
        leaf: Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]],
    ) -> Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]]:
        async def last(msg: ReceivedMessageT, act: ActorData) -> Any:
            return await leaf(msg, act)

        next_fn: Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]] = last

        for mw in reversed(mws):
            prev = next_fn

            async def layer(
                msg: ReceivedMessageT,
                act: ActorData,
                _mw: ActorMiddlewareT = mw,
                _nxt: Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]] = prev,
            ) -> Any:
                result = _mw(_nxt, msg, act)
                return await result if isawaitable(result) else result

            next_fn = layer

        return next_fn

    return assemble


def _compile_producer_middleware_pipeline(
    middlewares: Sequence[ProducerMiddlewareT] | None,
) -> Callable[
    [_ProducerMiddlewareLastLeaf],
    Callable[[str, MessageData, dict[str, Any] | None], Coroutine],
]:
    """Compile middlewares into a call-next chain factory.

    Returns a function that, given a leaf (call_next), produces a coroutine function
    with signature (channel, message, server_specific_parameters) -> Any that runs all middlewares around the leaf.
    """
    mws: list[ProducerMiddlewareT] = list(middlewares or [])

    def assemble(
        leaf: _ProducerMiddlewareLastLeaf,
    ) -> Callable[[str, MessageData, dict[str, Any] | None], Coroutine]:
        async def last(
            channel: str,
            message: MessageData,
            server_specific_parameters: dict[str, Any] | None,
        ) -> Any:
            return await leaf(
                channel=channel,
                message=message,
                server_specific_parameters=server_specific_parameters,
            )

        next_fn: Callable[[str, MessageData, dict[str, Any] | None], Coroutine[Any, Any, Any]] = (
            last
        )

        for mw in reversed(mws):
            prev = next_fn

            async def layer(
                channel: str,
                message: MessageData,
                server_specific_parameters: dict[str, Any] | None,
                _mw: ProducerMiddlewareT = mw,
                _nxt: Callable[
                    [str, MessageData, dict[str, Any] | None],
                    Coroutine[Any, Any, Any],
                ] = prev,
            ) -> Any:
                result = _mw(_nxt, channel, message, server_specific_parameters)
                return await result if isawaitable(result) else result

            next_fn = layer

        return next_fn

    return assemble
