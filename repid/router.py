from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from functools import partial
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeVar, overload

from repid._utils.asyncify import asyncify
from repid.converter import DefaultConverter
from repid.data import ActorData, Channel

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from repid.asyncapi.models import OperationBindingsObject
    from repid.connections.abc import BaseMessageT, ReceivedMessageT
    from repid.converter import ConverterT
    from repid.data import Channel, ExternalDocs, Tag


class RoutingStrategyT(Protocol):
    def __call__(self, *, actor_name: str, **kwargs: Any) -> Callable[[BaseMessageT], bool]: ...


def topic_based_routing_strategy(*, actor_name: str, **_: Any) -> Callable[[BaseMessageT], bool]:
    def strategy(message: BaseMessageT) -> bool:
        if message.headers is None:
            return False
        return message.headers.get("topic") == actor_name

    return strategy


def catch_all_routing_strategy(*, actor_name: str, **_: Any) -> Callable[[BaseMessageT], bool]:  # noqa: ARG001
    def strategy(_: BaseMessageT) -> bool:
        return True

    return strategy


class MiddlewareT(Protocol):
    async def __call__(
        self,
        call_next: Callable[..., Any],
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> Any: ...


YourFunc = TypeVar("YourFunc", bound=Callable)


def _compile_middleware_pipeline(
    middlewares: Sequence[MiddlewareT] | None,
) -> Callable[
    [Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]]],
    Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]],
]:
    """Compile middlewares into a call-next chain factory.

    Returns a function that, given a leaf (call_next), produces a coroutine function
    with signature (message, actor) -> Any that runs all middlewares around the leaf.
    """
    mws: list[MiddlewareT] = list(middlewares or [])

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
                _mw: MiddlewareT = mw,
                _nxt: Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]] = prev,
            ) -> Any:
                result = _mw(_nxt, msg, act)
                return await result if isawaitable(result) else result

            next_fn = layer

        return next_fn

    return assemble


@dataclass(frozen=True)
class RouterDefaults:
    channel: str | Channel = "default"
    middlewares: list[MiddlewareT] | None = None
    timeout: float = 300.0
    run_in_process: bool = False
    pool_executor: Executor | None = None
    converter: type[ConverterT] = DefaultConverter


class Router:
    __slots__ = ("_actors_per_channel_address", "_channels", "defaults")

    def __init__(self, *, defaults: RouterDefaults | None = None) -> None:
        self._actors_per_channel_address: dict[str, list[ActorData]] = defaultdict(list)
        self._channels: dict[str, Channel] = {}
        self.defaults = defaults or RouterDefaults()

    def include_router(self, router: Router) -> None:
        for channel_address, actors in router._actors_per_channel_address.items():
            self._actors_per_channel_address[channel_address].extend(actors)
        for channel_address, channel in router._channels.items():
            if channel_address not in self._channels:
                self._channels[channel_address] = channel

    @property
    def channels(self) -> list[Channel]:
        return list(self._channels.values())

    @property
    def actors(self) -> list[ActorData]:
        actors: list[ActorData] = []
        for actor_list in self._actors_per_channel_address.values():
            actors.extend(actor_list)
        return actors

    @overload
    def actor(
        self,
        fn: None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual"] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        middlewares: list[MiddlewareT] | None = None,
        timeout: float | None = None,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        run_in_process: bool | None = None,
        pool_executor: Executor | None = None,
        converter: type[ConverterT] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: OperationBindingsObject | None = None,
        deprecated: bool = False,
    ) -> Callable[[YourFunc], YourFunc]: ...

    @overload
    def actor(
        self,
        fn: YourFunc,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual"] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        middlewares: list[MiddlewareT] | None = None,
        timeout: float | None = None,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        run_in_process: bool | None = None,
        pool_executor: Executor | None = None,
        converter: type[ConverterT] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: OperationBindingsObject | None = None,
        deprecated: bool = False,
    ) -> YourFunc: ...

    def actor(
        self,
        fn: YourFunc | None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual"] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        middlewares: list[MiddlewareT] | None = None,
        timeout: float | None = None,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        run_in_process: bool | None = None,
        pool_executor: Executor | None = None,
        converter: type[ConverterT] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: OperationBindingsObject | None = None,
        deprecated: bool = False,
    ) -> YourFunc | Callable[[YourFunc], YourFunc]:
        """Actor decorator.

        Args:
            name (str | None, optional):
                actor's name.
                Used for routing a message to this actor, using the name as topic.
                Defaults to the name of your wrapped function.
            confirmation_mode (Literal["auto", "always_ack", "ack_first", "manual"], optional):
                How the message should be acknowledged. Defaults to "auto".
                - "auto": the message will be acknowledged automatically after successful processing,
                    and nacked on failure. If the message was already acted upon, no action will be taken.
                - "always_ack": the message will be acknowledged (if not acted upon) after both
                    successful and failed processing.
                - "ack_first": the message will be acknowledged before processing. May lead to lost
                    messages if the worker crashes or processing fails.
                - "manual": the message will not be acknowledged automatically. You must act on the
                    message inside of the actor. Failure to do so may lead to undefined behavior.
            routing_strategy (RoutingStrategyT, optional):
                A factory, that will create a routing strategy for the actor.
                Can be one of the built-in strategies, or provided by user.
                Messages are always routed to the first matching actor.
                Defaults to a topic-based routing strategy, where the actor's name is matched
                against the "topic" header of the message.
            channel (Channel | str | None, optional):
                AsyncAPI channel for this actor.
                Defaults to Router's default channel.
            middlewares (list[MiddlewareT] | None, optional):
                List of middlewares to apply to this actor.
                If specified, concatenated with Router's default middlewares.
            timeout (float | None, optional):
                Time limit for processing a message, in seconds.
                If the actor does not complete within this time, the message will be rejected.
                If zero or inf+, no time limit is applied.
                Defaults to Router's default timeout.
            title (str | None, optional):
                Human-readable title for the actor.
            summary (str | None, optional):
                Brief summary of what the actor does.
                Defaults to wrapped function's name.
            description (str | None, optional):
                Detailed description of the actor's purpose.
                Defaults to wrapped function's docstring.
            run_in_process (bool | None, optional):
                If True, runs synchronous actors inside of a `ProcessPoolExecutor`
                instead of the default `ThreadPoolExecutor`.
                Defaults to Router's default run_in_process value.
                Has no effect for asynchronous actors.
            pool_executor (Executor | None, optional):
                Custom executor to run synchronous actors.
                If provided, overrides the run_in_process setting.
                Has no effect for asynchronous actors.
            converter (type[ConverterT] | None, optional):
                Class that decides how the arguments and return type
                should be validated & parsed.
                Defaults to Router's default converter.
            security (Sequence[Any] | None, optional):
                Security requirements for the actor to be displayed in AsyncAPI schema.
            tags (Sequence[Tag] | None, optional):
                Tags to categorize the actor in AsyncAPI schema.
            external_docs (ExternalDocs | None, optional):
                External documentation for the actor to be displayed in AsyncAPI schema.
            bindings (OperationBindingsObject | None, optional):
                Operation bindings for the actor, used to specify protocol-specific details.
            deprecated (bool, optional):
                Whether the actor is deprecated. Defaults to False.

        Returns:
            YourFunc: your initial function.
        """

        if fn is None:
            return partial(  # type: ignore[return-value]
                self.actor,
                name=name,
                confirmation_mode=confirmation_mode,
                routing_strategy=routing_strategy,
                channel=channel,
                middlewares=middlewares,
                timeout=timeout,
                title=title,
                summary=summary,
                description=description,
                run_in_process=run_in_process,
                converter=converter,
                security=security,
                tags=tags,
                external_docs=external_docs,
                bindings=bindings,
                deprecated=deprecated,
            )

        if converter is None:
            converter = self.defaults.converter

        if channel is None:
            channel = self.defaults.channel

        channel_address = channel if isinstance(channel, str) else channel.address

        actual_name = name or fn.__name__

        actual_routing_strategy = routing_strategy(actor_name=actual_name)

        defaults_middlewares = self.defaults.middlewares or []
        actor_middlewares = middlewares or []
        all_middlewares: list[MiddlewareT] = [*defaults_middlewares, *actor_middlewares]

        composer = _compile_middleware_pipeline(all_middlewares)

        async def middleware_pipeline(
            call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]],
            message: ReceivedMessageT,
            actor: ActorData,
        ) -> Any:
            if all_middlewares:
                final = composer(call_next)
                return await final(message, actor)
            return await call_next(message, actor)

        actor_data = ActorData(
            fn=asyncify(
                fn,
                run_in_process=run_in_process or self.defaults.run_in_process,
                executor=pool_executor or self.defaults.pool_executor,
            ),
            name=actual_name,
            confirmation_mode=confirmation_mode,
            routing_strategy=actual_routing_strategy,
            middleware_pipeline=middleware_pipeline,
            channel_address=channel_address,
            timeout=timeout if timeout is not None else self.defaults.timeout,
            converter=converter(fn),
            title=title,
            summary=summary or " ".join([part.capitalize() for part in fn.__name__.split("_")]),
            description=description or fn.__doc__,
            security=tuple(security) if security is not None else None,
            tags=tuple(tags) if tags is not None else None,
            external_docs=external_docs,
            bindings=bindings,
            deprecated=deprecated,
        )

        self._actors_per_channel_address[channel_address].append(actor_data)
        if channel_address not in self._channels:
            self._channels[channel_address] = (
                channel if isinstance(channel, Channel) else Channel(address=channel)
            )
        return fn
